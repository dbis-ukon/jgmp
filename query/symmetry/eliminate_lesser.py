
from query.graphlike_query import GraphlikeQuery
from query.predicatable import Predicatable
from query.predicate import Predicate
from schema.comparison_operator import OPERATORS


def eliminate_lesser_query(query: GraphlikeQuery):
    for node in query.nodes():
        if isinstance(node, Predicatable):
            eliminate_lesser_predicatable(node)
    for _, edge, _ in query.edges():
        if isinstance(edge, Predicatable):
            eliminate_lesser_predicatable(edge)


def eliminate_lesser_predicatable(predicatable: Predicatable):
    predicates = predicatable.predicates()
    new_predicates = []
    for disjunction in predicates:
        new_disjunction = []
        multiply_disjunction = []
        for predicate in disjunction:
            if predicate.operator() == OPERATORS["<"]:
                attribute = predicate.attribute()
                value = predicate.value()
                positive = predicate.positive()
                equal_predicate = Predicate(attribute, OPERATORS["="], value, not positive)
                greater_predicate = Predicate(attribute, OPERATORS[">"], value, not positive)
                if positive:
                    multiply_disjunction.append([equal_predicate, greater_predicate])
                else:
                    new_disjunction.append(equal_predicate)
                    new_disjunction.append(greater_predicate)
            else:
                new_disjunction.append(predicate)
        disjunctions = [new_disjunction]
        for multiply in multiply_disjunction:
            new_disjunctions = []
            for old_disjunction in disjunctions:
                for predicate in multiply:
                    new_disjunctions.append(old_disjunction + [predicate])
            disjunctions = new_disjunctions
        new_predicates += disjunctions
    predicatable.set_predicates(new_predicates)
