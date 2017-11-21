from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


class Evaluator(object):
    """RLlib optimizers require RL algorithms to implement this interface.

    Any algorithm that implements Evaluator can plug in any RLLib optimizer,
    e.g. async SGD, local multi-GPU SGD, etc.
    """

    def sample(self):
        """Returns experience samples from this Evaluator."""

        raise NotImplementedError

    def gradients(self, samples):
        """Returns a gradient computed w.r.t the specified samples."""

        raise NotImplementedError

    def apply(self, grads):
        """Applies the given gradients to this Evaluator's weights."""

        raise NotImplementedError

    def get_weights(self):
        """Returns the model weights of this Evaluator."""

        raise NotImplementedError

    def set_weights(self, weights):
        """Sets the model weights of this Evaluator."""

        raise NotImplementedError


class TFMultiGpuSupport(Evaluator):
    """The multi-GPU TF optimizer requires this additional interface."""

    def _get_tf_loss_input_shapes(self):
        """Returns a list of the input shapes required for the loss."""

        raise NotImplementedError

    def _build_tf_loss(self, input_placeholders):
        """Returns a loss tensor for the specified inputs."""

        raise NotImplementedError
