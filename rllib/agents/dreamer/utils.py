import random
from ray.rllib.utils.framework import try_import_torch
import numpy as np

torch, nn = try_import_torch()


# Custom initialization for each diff layer
class Linear(nn.Linear):
  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)

  def reset_parameters(self):
      nn.init.xavier_uniform_(self.weight)
      if self.bias is not None:
          nn.init.zeros_(self.bias)


class Conv2d(nn.Conv2d):
  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)

  def reset_parameters(self):
      nn.init.xavier_uniform_(self.weight)
      if self.bias is not None:
          nn.init.zeros_(self.bias)

class ConvTranspose2d(nn.ConvTranspose2d):
  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)

  def reset_parameters(self):
      nn.init.xavier_uniform_(self.weight)
      if self.bias is not None:
          nn.init.zeros_(self.bias)

class GRUCell(nn.GRUCell):
  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)


  def reset_parameters(self):
      nn.init.xavier_uniform_(self.weight_ih)
      nn.init.orthogonal_(self.weight_hh)
      nn.init.zeros_(self.bias_ih) 
      nn.init.zeros_(self.bias_hh)


# Custom Tanh Bijector due to Huge gradients through Actor

class TanhBijector(torch.distributions.Transform):
    def __init__(self):
        super().__init__()

    def atanh(self,x):
      return 0.5 * torch.log((1 + x) / (1 - x))

    def sign(self): return 1.

    def _call(self, x): return torch.tanh(x)

    def _inverse(self, y):
      y = torch.where(
          (torch.abs(y) <= 1.),
          torch.clamp(y, -0.99999997, 0.99999997),
          y)
      y = self.atanh(y)
      return y

    def log_abs_det_jacobian(self, x, y):
        return 2. * (np.log(2) - x - nn.functional.softplus(-2. * x))

class FreezeParameters:
  def __init__(self, parameters):
      """
      Context manager to locally freeze gradients.
      In some cases with can speed up computation because gradients aren't calculated for these listed modules.
      example:
      ```
      with FreezeParameters([module]):
          output_tensor = module(input_tensor)
      ```
      :param modules: iterable of modules. used to call .parameters() to freeze gradients.
      """
      self.parameters = parameters
      self.param_states = [p.requires_grad for p in self.parameters]

  def __enter__(self):
      for param in self.parameters:
          param.requires_grad = False

  def __exit__(self, exc_type, exc_val, exc_tb):
      for i, param in enumerate(self.parameters):
          param.requires_grad = self.param_states[i]






