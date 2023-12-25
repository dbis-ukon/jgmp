
from torch.nn import Module
import torch


class DenseCrossEntropy(Module):
    def __init__(self, out_mse: float = 0):
        super().__init__()
        self._out_mse = out_mse

    def forward(self, out, sums, target, mask=None):
        logarithm = torch.add(out, -torch.log(sums))
        terms = -torch.mul(target, logarithm)
        if mask is not None:
            terms = torch.mul(mask, terms)
        total_loss = torch.sum(terms)
        if self._out_mse != 0:
            out_mse = torch.mul(self._out_mse, torch.mul(out, out))
            if mask is not None:
                out_mse = torch.mul(mask, out_mse)
            total_loss = torch.add(total_loss, torch.sum(out_mse))
        return total_loss
