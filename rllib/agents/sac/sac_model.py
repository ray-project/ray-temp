import numpy as np

from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.utils import try_import_tf, try_import_tfp

tf = try_import_tf()
tfp = try_import_tfp()

SCALE_DIAG_MIN_MAX = (-20, 2)


def SquashBijector():
    # lazy def since it depends on tfp
    class SquashBijector(tfp.bijectors.Bijector):
        def __init__(self, validate_args=False, name="tanh"):
            super(SquashBijector, self).__init__(
                forward_min_event_ndims=0,
                validate_args=validate_args,
                name=name)

        def _forward(self, x):
            return tf.nn.tanh(x)

        def _inverse(self, y):
            return tf.atanh(y)

        def _forward_log_det_jacobian(self, x):
            return 2. * (np.log(2.) - x - tf.nn.softplus(-2. * x))

    return SquashBijector()


class SACModel(TFModelV2):
    """Extension of standard TFModel for SAC.

    Data flow:
        obs -> forward() -> model_out
        model_out -> get_policy_output() -> pi(s)
        model_out, actions -> get_q_values() -> Q(s, a)
        model_out, actions -> get_twin_q_values() -> Q_twin(s, a)

    Note that this class by itself is not a valid model unless you
    implement forward() in a subclass."""

    def __init__(self,
                 obs_space,
                 action_space,
                 num_outputs,
                 model_config,
                 name,
                 actor_hidden_activation="relu",
                 actor_hiddens=(256, 256),
                 critic_hidden_activation="relu",
                 critic_hiddens=(256, 256),
                 twin_q=False):
        """Initialize variables of this model.

        Extra model kwargs:
            actor_hidden_activation (str): activation for actor network
            actor_hiddens (list): hidden layers sizes for actor network
            critic_hidden_activation (str): activation for critic network
            critic_hiddens (list): hidden layers sizes for critic network
            twin_q (bool): build twin Q networks

        Note that the core layers for forward() are not defined here, this
        only defines the layers for the output heads. Those layers for
        forward() should be defined in subclasses of SACModel.
        """

        if tfp is None:
            raise ImportError("tensorflow-probability package not found")

        super(SACModel, self).__init__(obs_space, action_space, num_outputs,
                                       model_config, name)
        self.action_dim = np.product(action_space.shape)
        self.model_out = tf.keras.layers.Input(
            shape=(num_outputs, ), name="model_out")
        self.actions = tf.keras.layers.Input(
            shape=(self.action_dim, ), name="actions")
        shift_and_log_scale_diag = tf.keras.Sequential([
            tf.keras.layers.Dense(
                units=hidden,
                activation=getattr(tf.nn, actor_hidden_activation),
                name="action_hidden_{}".format(i))
            for i, hidden in enumerate(actor_hiddens)
        ] + [
            tf.keras.layers.Dense(
                units=2 * self.action_dim, activation=None, name="action_out")
        ])(self.model_out)

        shift, log_scale_diag = tf.keras.layers.Lambda(
            lambda shift_and_log_scale_diag: tf.split(
                shift_and_log_scale_diag,
                num_or_size_splits=2,
                axis=-1)
        )(shift_and_log_scale_diag)

        log_scale_diag = tf.keras.layers.Lambda(
            lambda log_sd: tf.clip_by_value(log_sd, *SCALE_DIAG_MIN_MAX))(
                log_scale_diag)

        shift_and_log_scale_diag = tf.keras.layers.Concatenate(axis=-1)(
            [shift, log_scale_diag])

        batch_size = tf.keras.layers.Lambda(lambda x: tf.shape(input=x)[0])(
            self.model_out)

        base_distribution = tfp.distributions.MultivariateNormalDiag(
            loc=tf.zeros(self.action_dim), scale_diag=tf.ones(self.action_dim))

        latents = tf.keras.layers.Lambda(
            lambda batch_size: base_distribution.sample(batch_size))(
                batch_size)

        self.shift_and_log_scale_diag = latents
        self.latents_model = tf.keras.Model(self.model_out, latents)

        def raw_actions_fn(inputs):
            shift, log_scale_diag, latents = inputs
            bijector = tfp.bijectors.Affine(
                shift=shift, scale_diag=tf.exp(log_scale_diag))
            actions = bijector.forward(latents)
            return actions

        raw_actions = tf.keras.layers.Lambda(raw_actions_fn)(
            (shift, log_scale_diag, latents))

        squash_bijector = (SquashBijector())

        actions = tf.keras.layers.Lambda(
            lambda raw_actions: squash_bijector.forward(raw_actions))(
                raw_actions)
        self.actions_model = tf.keras.Model(self.model_out, actions)

        deterministic_actions = tf.keras.layers.Lambda(
            lambda shift: squash_bijector.forward(shift))(shift)

        self.deterministic_actions_model = tf.keras.Model(
            self.model_out, deterministic_actions)

        def log_pis_fn(inputs):
            shift, log_scale_diag, actions = inputs
            base_distribution = tfp.distributions.MultivariateNormalDiag(
                loc=tf.zeros(self.action_dim),
                scale_diag=tf.ones(self.action_dim))
            bijector = tfp.bijectors.Chain((
                squash_bijector,
                tfp.bijectors.Affine(
                    shift=shift, scale_diag=tf.exp(log_scale_diag)),
            ))
            distribution = (tfp.distributions.TransformedDistribution(
                distribution=base_distribution, bijector=bijector))

            log_pis = distribution.log_prob(actions)[:, None]
            return log_pis

        self.actions_input = tf.keras.layers.Input(
            shape=(self.action_dim, ), name="actions")

        log_pis_for_action_input = tf.keras.layers.Lambda(log_pis_fn)(
            [shift, log_scale_diag, self.actions_input])

        self.log_pis_model = tf.keras.Model(
            (self.model_out, self.actions_input), log_pis_for_action_input)

        self.register_variables(self.actions_model.variables)

        def build_q_net(name, observations, actions):
            q_net = tf.keras.Sequential([
                tf.keras.layers.Concatenate(axis=1),
            ] + [
                tf.keras.layers.Dense(
                    units=units,
                    activation=getattr(tf.nn, critic_hidden_activation),
                    name="{}_hidden_{}".format(name, i))
                for i, units in enumerate(critic_hiddens)
            ] + [
                tf.keras.layers.Dense(
                    units=1, activation=None, name="{}_out".format(name))
            ])

            # TODO(hartikainen): Remove the unnecessary Model call here
            q_net = tf.keras.Model([observations, actions],
                                   q_net([observations, actions]))
            return q_net

        self.q_net = build_q_net("q", self.model_out, self.actions)
        self.register_variables(self.q_net.variables)

        if twin_q:
            self.twin_q_net = build_q_net("twin_q", self.model_out,
                                          self.actions)
            self.register_variables(self.twin_q_net.variables)
        else:
            self.twin_q_net = None

        self.log_alpha = tf.Variable(0.0, dtype=tf.float32, name="log_alpha")
        self.alpha = tf.exp(self.log_alpha)

        self.register_variables([self.log_alpha])

    def get_policy_output(self, model_out, deterministic=False):
        """Return the (unscaled) output of the policy network.

        This returns the unscaled outputs of pi(s).

        Arguments:
            model_out (Tensor): obs embeddings from the model layers, of shape
                [BATCH_SIZE, num_outputs].

        Returns:
            tensor of shape [BATCH_SIZE, action_dim] with range [-inf, inf].
        """
        if deterministic:
            actions = self.deterministic_actions_model(model_out)
            log_pis = None
        else:
            actions = self.actions_model(model_out)
            log_pis = self.log_pis_model((model_out, actions))

        return actions, log_pis

    def get_q_values(self, model_out, actions):
        """Return the Q estimates for the most recent forward pass.

        This implements Q(s, a).

        Arguments:
            model_out (Tensor): obs embeddings from the model layers, of shape
                [BATCH_SIZE, num_outputs].
            actions (Tensor): action values that correspond with the most
                recent batch of observations passed through forward(), of shape
                [BATCH_SIZE, action_dim].

        Returns:
            tensor of shape [BATCH_SIZE].
        """
        return self.q_net([model_out, actions])

    def get_twin_q_values(self, model_out, actions):
        """Same as get_q_values but using the twin Q net.

        This implements the twin Q(s, a).

        Arguments:
            model_out (Tensor): obs embeddings from the model layers, of shape
                [BATCH_SIZE, num_outputs].
            actions (Tensor): action values that correspond with the most
                recent batch of observations passed through forward(), of shape
                [BATCH_SIZE, action_dim].

        Returns:
            tensor of shape [BATCH_SIZE].
        """
        return self.twin_q_net([model_out, actions])

    def policy_variables(self):
        """Return the list of variables for the policy net."""

        return list(self.actions_model.variables)

    def q_variables(self):
        """Return the list of variables for Q / twin Q nets."""

        return self.q_net.variables + (self.twin_q_net.variables
                                       if self.twin_q_net else [])
