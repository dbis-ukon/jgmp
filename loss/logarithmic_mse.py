from torch.nn import Module
import torch
from torch.nn import MSELoss


class LogarithmicMSE(Module):
    def __init__(self) -> None:
        super().__init__()
        self._mse = MSELoss()

    def forward(self, input, target):
        log_input = torch.log(input)
        log_target = torch.log(target)
        return self._mse(log_input, log_target)
