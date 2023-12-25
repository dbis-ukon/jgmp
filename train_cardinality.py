import datetime
import math
import random

from torch_geometric.data import Data

from encoder.cardinality_relation_data_generator import CardinalityRelationDataGenerator
from models.bulk_jgmp_cardinality_model import BulkJGMPCardinalityModel
from models.bulk_mscn_cardinality_model import BulkMSCNCardinalityModel
from models.cardinality_model import CardinalityModel
from models.graph_cardinality_model import GraphCardinalityModel
from query.graphlike_query import GraphlikeQuery
from query_data.bulk_cardinality_query_data import BulkCardinalityQueryData
from query_data.bulk_light_cardinality_query_data import BulkLightCardinalityQueryData
from query_data.cardinality_query_data import CardinalityQueryData
from query_data.graphlike_query_data import GraphlikeQueryData
from typing import List, Optional, Tuple, Iterator
from torch_geometric.loader import DataLoader
from torch.optim import Adam, Optimizer
import numpy as np
import torch
from scipy.stats import gmean


def train(model: GraphCardinalityModel,
          training_queries: List[GraphlikeQueryData],
          validation_queries: List[GraphlikeQueryData],
          learning_rate: float = 0.0001,
          batch_size: int = 1024,
          epochs: int = 100,
          card_rel_gen: Optional[Tuple[CardinalityRelationDataGenerator, int]] = None,
          selfsupervised_factor: float = 1):
    device = next(model.parameters()).device
    print("Training on %s" % device)
    print(datetime.datetime.now())

    loader = DataLoader(training_queries, batch_size=batch_size, shuffle=True)
    if card_rel_gen is not None:
        card_rel_data_generator, num_relations = card_rel_gen
        total_relations = num_relations * math.ceil(len(training_queries) / batch_size)
    else:
        card_rel_data_generator = None
    card_rels = []
    selfsupervised_losses_epoch = []

    optimizer = Adam(model.parameters(), lr=learning_rate)
    for epoch in range(epochs):
        total_supervised = 0
        total_selfsupervised = 0
        if card_rel_data_generator is not None:
            if len(selfsupervised_losses_epoch) > 0:
                loss_cutoff = np.percentile(selfsupervised_losses_epoch, 10)
            else:
                loss_cutoff = 0
            keep_card_rels = []
            for card_rel, loss in zip(card_rels, selfsupervised_losses_epoch):
                if loss > loss_cutoff:
                    keep_card_rels.append(card_rel)
            card_rels = keep_card_rels

            missing_card_rels = total_relations - len(card_rels)
            if isinstance(model, BulkJGMPCardinalityModel):
                new_card_rels = card_rel_data_generator.generate_bulk_light_cardinality_relations_parallel(missing_card_rels)
            elif isinstance(model, BulkMSCNCardinalityModel):
                new_card_rels = card_rel_data_generator.generate_bulk_mscn_cardinality_relations_parallel(missing_card_rels)
            else:
                new_card_rels = card_rel_data_generator.generate_cardinality_relations_parallel(missing_card_rels)
            card_rels += new_card_rels
            random.shuffle(card_rels)
            selfsupervised_losses_epoch = []
            card_rel_loader_iter = iter(DataLoader(card_rels, batch_size=num_relations))
        else:
            card_rel_loader_iter = None

        for batch_no, batch in enumerate(loader):
            supervised_batch, selfsupervised_batch, selfsupervised_losses_batch = batch_train(model, optimizer, batch, card_rel_loader_iter, selfsupervised_factor, device)
            total_supervised += supervised_batch
            total_selfsupervised += selfsupervised_batch
            selfsupervised_losses_epoch += selfsupervised_losses_batch

        loss_string = "%d supervised = %f, selfsupervised = %f" % (epoch + 1, total_supervised / (batch_no + 1),
                                                                   total_selfsupervised / (batch_no + 1))

        if len(validation_queries) > 0:
            print(loss_string, end=" ")
            test_result = test(model, validation_queries, device=device)
            if np.isnan(test_result):
                return
        else:
            print(loss_string)

    print(datetime.datetime.now())
    model.encoder().reset()


def batch_train(model: GraphCardinalityModel, optimizer: Optimizer, batch: GraphlikeQueryData, card_rel_loader_iter: Optional[Iterator[DataLoader]], selfsupervised_factor: float, device: torch.device):
    batch.to(device, non_blocking=True)
    optimizer.zero_grad()
    supervised_loss = model.loss(batch)
    total_supervised = supervised_loss.detach().cpu().numpy()
    if card_rel_loader_iter is not None:
        card_rel_query_data = next(card_rel_loader_iter)
        card_rel_query_data.to(device, non_blocking=True)
        selfsupervised_losses = model.loss_relation(card_rel_query_data)
        selfsupervised_loss = torch.mean(selfsupervised_losses)
        loss = supervised_loss + torch.mul(selfsupervised_factor, selfsupervised_loss)
        total_selfsupervised = selfsupervised_loss.detach().cpu().numpy()
        selfsupervised_losses_batch = list(selfsupervised_losses.detach().cpu().numpy())
    else:
        loss = supervised_loss
        total_selfsupervised = 0
        selfsupervised_losses_batch = []
    loss.backward()
    optimizer.step()
    return total_supervised, total_selfsupervised, selfsupervised_losses_batch


def test_base(model: CardinalityModel, encoded_queries: List[Data], device=None) -> Tuple[List[int], List[float]]:
    if len(encoded_queries) == 0:
        return [], []
    batch_size = 16
    loader = DataLoader(encoded_queries, batch_size=batch_size)
    estimations = []
    cardinalities = []
    old_device = next(model.parameters()).device
    if device is not None:
        model.to(device)
    model.eval()
    with torch.no_grad():
        for batch in loader:
            if device is not None:
                batch.to(device, non_blocking=True)
            estimation = model.forward(batch).cpu().numpy()
            cardinality = batch.cardinality.cpu().numpy()
            estimations += [e for e in estimation]
            cardinalities += [c for c in cardinality]
    model.train()
    if device is not None:
        model.to(old_device)
    return cardinalities, estimations


def test(model: CardinalityModel, encoded_queries: List[Data], device=None) -> float:
    cardinalities, estimations = test_base(model, encoded_queries, device=device)
    return q_error_stats(estimations, cardinalities)


def q_error(estimation: float, label: int) -> float:
    trunc_est = max(1, estimation)
    trunc_lab = max(1, label)
    return max(trunc_est / trunc_lab, trunc_lab / trunc_est)


def q_error_stats(estimations: List[float], labels: List[float]) -> float:
    q_errors = []

    for est, lab in zip(estimations, labels):
        q_errors.append(q_error(est, lab))

    mean = np.mean(q_errors)

    stat_string = "mean: %f, median: %f, 90th: %f, 95th: %f, 99th: %f, max: %f, geometric mean: %f" % (mean,
                                                                                                       np.percentile(q_errors, 50),
                                                                                                       np.percentile(q_errors, 90),
                                                                                                       np.percentile(q_errors, 95),
                                                                                                       np.percentile(q_errors, 99),
                                                                                                       np.max(q_errors),
                                                                                                       gmean(q_errors))

    print(stat_string)
    return mean


def encode(queries: List[List[GraphlikeQuery]],
           cardinality_model: CardinalityModel,
           device: torch.device
           ) -> List[CardinalityQueryData]:
    encoded_queries = []
    for query in queries:
        encoded_query = cardinality_model.bulk_encode(query)
        for subquery in encoded_query:
            subquery.to(device, non_blocking=True)
        encoded_queries += encoded_query
    return encoded_queries
