from torch.nn import Module
import torch


class QErrorLoss(Module):
    def __init__(self) -> None:
        super().__init__()

    def forward(self, input, target):
        q1 = torch.div(input, target)
        q2 = torch.div(target, input)
        return torch.sum(torch.maximum(q1, q2))
