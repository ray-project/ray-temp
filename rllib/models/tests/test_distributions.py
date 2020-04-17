import numpy as np
from gym.spaces import Box
from scipy.stats import norm, beta
import unittest

from ray.rllib.models.tf.tf_action_dist import Categorical, MultiCategorical, \
    SquashedGaussian, GumbelSoftmax
from ray.rllib.models.torch.torch_action_dist import TorchMultiCategorical, \
    TorchSquashedGaussian, TorchBeta
from ray.rllib.utils import try_import_tf, try_import_torch
from ray.rllib.utils.numpy import MIN_LOG_NN_OUTPUT, MAX_LOG_NN_OUTPUT, \
    softmax, SMALL_NUMBER
from ray.rllib.utils.test_utils import check, framework_iterator

tf = try_import_tf()
torch, _ = try_import_torch()


class TestDistributions(unittest.TestCase):
    """Tests ActionDistribution classes."""

    def test_categorical(self):
        """Tests the Categorical ActionDistribution (tf only)."""
        num_samples = 100000
        logits = tf.placeholder(tf.float32, shape=(None, 10))
        z = 8 * (np.random.rand(10) - 0.5)
        data = np.tile(z, (num_samples, 1))
        c = Categorical(logits, {})  # dummy config dict
        sample_op = c.sample()
        sess = tf.Session()
        sess.run(tf.global_variables_initializer())
        samples = sess.run(sample_op, feed_dict={logits: data})
        counts = np.zeros(10)
        for sample in samples:
            counts[sample] += 1.0
        probs = np.exp(z) / np.sum(np.exp(z))
        self.assertTrue(np.sum(np.abs(probs - counts / num_samples)) <= 0.01)

    def test_multi_categorical(self):
        batch_size = 100
        num_categories = 3
        num_sub_distributions = 5
        # Create 5 categorical distributions of 3 categories each.
        inputs_space = Box(
            -1.0,
            2.0,
            shape=(batch_size, num_sub_distributions * num_categories))
        values_space = Box(
            0,
            num_categories - 1,
            shape=(num_sub_distributions, batch_size),
            dtype=np.int32)

        inputs = inputs_space.sample()
        input_lengths = [num_categories] * num_sub_distributions
        inputs_split = np.split(inputs, num_sub_distributions, axis=1)

        for fw in framework_iterator():
            # Create the correct distribution object.
            cls = MultiCategorical if fw != "torch" else TorchMultiCategorical
            multi_categorical = cls(inputs, None, input_lengths)

            # Batch of size=3 and deterministic (True).
            expected = np.transpose(np.argmax(inputs_split, axis=-1))
            # Sample, expect always max value
            # (max likelihood for deterministic draw).
            out = multi_categorical.deterministic_sample()
            check(out, expected)

            # Batch of size=3 and non-deterministic -> expect roughly the mean.
            out = multi_categorical.sample()
            check(
                tf.reduce_mean(out)
                if fw != "torch" else torch.mean(out.float()),
                1.0,
                decimals=0)

            # Test log-likelihood outputs.
            probs = softmax(inputs_split)
            values = values_space.sample()

            out = multi_categorical.logp(values if fw != "torch" else [
                torch.Tensor(values[i]) for i in range(num_sub_distributions)
            ])  # v in np.stack(values, 1)])
            expected = []
            for i in range(batch_size):
                expected.append(
                    np.sum(
                        np.log(
                            np.array([
                                probs[j][i][values[j][i]]
                                for j in range(num_sub_distributions)
                            ]))))
            check(out, expected, decimals=4)

            # Test entropy outputs.
            out = multi_categorical.entropy()
            expected_entropy = -np.sum(np.sum(probs * np.log(probs), 0), -1)
            check(out, expected_entropy)

    def test_squashed_gaussian(self):
        """Tests the SquashedGaussian ActionDistribution for all frameworks."""
        input_space = Box(-2.0, 2.0, shape=(200, 10))
        low, high = -2.0, 1.0

        for fw, sess in framework_iterator(
                frameworks=("torch", "tf", "eager"), session=True):
            cls = SquashedGaussian if fw != "torch" else TorchSquashedGaussian

            # Batch of size=n and deterministic.
            inputs = input_space.sample()
            means, _ = np.split(inputs, 2, axis=-1)
            squashed_distribution = cls(inputs, {}, low=low, high=high)
            expected = ((np.tanh(means) + 1.0) / 2.0) * (high - low) + low
            # Sample n times, expect always mean value (deterministic draw).
            out = squashed_distribution.deterministic_sample()
            check(out, expected)

            # Batch of size=n and non-deterministic -> expect roughly the mean.
            inputs = input_space.sample()
            means, log_stds = np.split(inputs, 2, axis=-1)
            squashed_distribution = cls(inputs, {}, low=low, high=high)
            expected = ((np.tanh(means) + 1.0) / 2.0) * (high - low) + low
            values = squashed_distribution.sample()
            if sess:
                values = sess.run(values)
            else:
                values = values.numpy()
            self.assertTrue(np.max(values) <= high)
            self.assertTrue(np.min(values) >= low)

            check(np.mean(values), expected.mean(), decimals=1)

            # Test log-likelihood outputs.
            sampled_action_logp = squashed_distribution.logp(
                values if fw != "torch" else torch.Tensor(values))
            if sess:
                sampled_action_logp = sess.run(sampled_action_logp)
            else:
                sampled_action_logp = sampled_action_logp.numpy()
            # Convert to parameters for distr.
            stds = np.exp(
                np.clip(log_stds, MIN_LOG_NN_OUTPUT, MAX_LOG_NN_OUTPUT))
            # Unsquash values, then get log-llh from regular gaussian.
            # atanh_in = np.clip((values - low) / (high - low) * 2.0 - 1.0,
            #   -1.0 + SMALL_NUMBER, 1.0 - SMALL_NUMBER)
            normed_values = (values - low) / (high - low) * 2.0 - 1.0
            save_normed_values = np.clip(normed_values, -1.0 + SMALL_NUMBER, 1.0 - SMALL_NUMBER)
            unsquashed_values = np.arctanh(save_normed_values)
            log_prob_unsquashed = np.sum(
                np.log(
                    norm.pdf(unsquashed_values, means, stds)),  #  + SMALL_NUMBER),
                -1)
            log_prob = log_prob_unsquashed - \
                np.sum(np.log(1 - np.tanh(unsquashed_values) ** 2),
                       axis=-1)
            check(np.sum(sampled_action_logp), np.sum(log_prob), rtol=0.05)

            # NN output.
            means = np.array([[0.1, 0.2, 0.3, 0.4, 50.0],
                              [-0.1, -0.2, -0.3, -0.4, -1.0]])
            log_stds = np.array([[0.8, -0.2, 0.3, -1.0, 2.0],
                                 [0.7, -0.3, 0.4, -0.9, 2.0]])
            squashed_distribution = cls(
                inputs=np.concatenate([means, log_stds], axis=-1),
                model={},
                low=low,
                high=high)
            # Convert to parameters for distr.
            stds = np.exp(log_stds)
            # Values to get log-likelihoods for.
            values = np.array([[0.9, 0.2, 0.4, -0.1, -1.05],
                               [-0.9, -0.2, 0.4, -0.1, -1.05]])

            # Unsquash values, then get log-llh from regular gaussian.
            unsquashed_values = np.arctanh((values - low) /
                                           (high - low) * 2.0 - 1.0)
            log_prob_unsquashed = \
                np.sum(np.log(norm.pdf(unsquashed_values, means, stds)), -1)
            log_prob = log_prob_unsquashed - \
                np.sum(np.log(1 - np.tanh(unsquashed_values) ** 2),
                       axis=-1)

            outs = squashed_distribution.logp(values if fw != "torch" else
                                              torch.Tensor(values))
            if sess:
                outs = sess.run(outs)
            check(outs, log_prob, decimals=4)

    def test_beta(self):
        input_space = Box(-2.0, 1.0, shape=(200, 10))
        low, high = -1.0, 2.0
        plain_beta_value_space = Box(0.0, 1.0, shape=(200, 5))

        for fw, sess in framework_iterator(frameworks="torch", session=True):
            cls = TorchBeta
            inputs = input_space.sample()
            beta_distribution = cls(inputs, {}, low=low, high=high)

            inputs = beta_distribution.inputs
            alpha, beta_ = np.split(inputs.numpy(), 2, axis=-1)

            # Mean for a Beta distribution: 1 / [1 + (beta/alpha)]
            expected = (1.0 / (1.0 + beta_ / alpha)) * (high - low) + low
            # Sample n times, expect always mean value (deterministic draw).
            out = beta_distribution.deterministic_sample()
            check(out, expected, rtol=0.01)

            # Batch of size=n and non-deterministic -> expect roughly the mean.
            values = beta_distribution.sample()
            if sess:
                values = sess.run(values)
            else:
                values = values.numpy()
            self.assertTrue(np.max(values) <= high)
            self.assertTrue(np.min(values) >= low)

            check(np.mean(values), expected.mean(), decimals=1)

            # Test log-likelihood outputs (against scipy).
            inputs = input_space.sample()
            beta_distribution = cls(inputs, {}, low=low, high=high)
            inputs = beta_distribution.inputs
            alpha, beta_ = np.split(inputs.numpy(), 2, axis=-1)

            values = plain_beta_value_space.sample()
            values_scaled = values * (high - low) + low
            out = beta_distribution.logp(torch.Tensor(values_scaled))
            check(
                out,
                np.sum(np.log(beta.pdf(values, alpha, beta_)), -1),
                rtol=0.001)

            # TODO(sven): Test entropy outputs (against scipy).

    def test_gumbel_softmax(self):
        """Tests the GumbelSoftmax ActionDistribution (tf-eager only)."""
        for fw, sess in framework_iterator(
                frameworks=["tf", "eager"], session=True):
            batch_size = 1000
            num_categories = 5
            input_space = Box(-1.0, 1.0, shape=(batch_size, num_categories))

            # Batch of size=n and deterministic.
            inputs = input_space.sample()
            gumbel_softmax = GumbelSoftmax(inputs, {}, temperature=1.0)

            expected = softmax(inputs)
            # Sample n times, expect always mean value (deterministic draw).
            out = gumbel_softmax.deterministic_sample()
            check(out, expected)

            # Batch of size=n and non-deterministic -> expect roughly that
            # the max-likelihood (argmax) ints are output (most of the time).
            inputs = input_space.sample()
            gumbel_softmax = GumbelSoftmax(inputs, {}, temperature=1.0)
            expected_mean = np.mean(np.argmax(inputs, -1)).astype(np.float32)
            outs = gumbel_softmax.sample()
            if sess:
                outs = sess.run(outs)
            check(np.mean(np.argmax(outs, -1)), expected_mean, rtol=0.08)


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
