import logging
logger = logging.getLogger(__name__)

TorchTrainer = None
TorchTrainable = None
TrainingOperator = None

try:
    import torch  # noqa: F401

    from ray.util.sgd.torch.torch_trainer import TorchTrainer

    from ray.util.sgd.torch.training_operator import TrainingOperator

    __all__ = ["TorchTrainer", "TrainingOperator"]
except ImportError:
    logger.warning("PyTorch not found. TorchTrainer will not be available")
