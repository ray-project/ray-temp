import logging

from ray.rllib.agents.trainer import with_common_config
from ray.rllib.agents.trainer_template import build_trainer
from ray.rllib.agents.dqn.dqn_policy import DQNTFPolicy
from ray.rllib.agents.dqn.simple_q_policy import SimpleQPolicy
from ray.rllib.optimizers import SyncReplayOptimizer
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.deprecation import deprecation_warning
from ray.rllib.utils.explorations.epsilon_greedy import EpsilonGreedy

logger = logging.getLogger(__name__)

# yapf: disable
# __sphinx_doc_begin__
DEFAULT_CONFIG = with_common_config({
    # === Model ===
    # Number of atoms for representing the distribution of return. When
    # this is greater than 1, distributional Q-learning is used.
    # the discrete supports are bounded by v_min and v_max
    "num_atoms": 1,
    "v_min": -10.0,
    "v_max": 10.0,
    # Whether to use noisy network
    "noisy": False,
    # control the initial value of noisy nets
    "sigma0": 0.5,
    # Whether to use dueling dqn
    "dueling": True,
    # Whether to use double dqn
    "double_q": True,
    # Postprocess model outputs with these hidden layers to compute the
    # state and action values. See also the model config in catalog.py.
    "hiddens": [256],
    # N-step Q learning
    "n_step": 1,

    # How to sample non-exploratory actions.
    # TODO(sven): This has no effect so far as we don't use a distribution.
    "deterministic": True,

    # === Exploration Settings ===
    "exploration": {
        "type": EpsilonGreedy,  # Exploration class.
        # Config for the Exploration class' c'tor:
        "initial_epsilon": 1.0,  # Initial epsilon value.
        "final_epsilon": 0.02,  # Final epsilon value.
        "epsilon_timesteps": 10000,  # Timesteps over which to anneal eps.
        # Whether to use a distribution of per-worker fixed epsilons for
        # exploration.
        "fixed_per_worker_epsilon": False
    },

    # TODO(sven): Make Exploration class for parameter noise.
    # If True parameter space noise will be used for exploration
    # See https://blog.openai.com/better-exploration-with-parameter-noise/
    "parameter_noise": False,

    # Minimum env steps to optimize for per train call. This value does
    # not affect learning, only the length of iterations.
    "timesteps_per_iteration": 1000,
    # Update the target network every `target_network_update_freq` steps.
    "target_network_update_freq": 500,

    # TODO(sven): Make Exploration class for softmax Q action selection.
    # Use softmax for sampling actions. Required for off policy estimation.
    "soft_q": False,
    # Softmax temperature. Q values are divided by this value prior to softmax.
    # Softmax approaches argmax as the temperature drops to zero.
    "softmax_temp": 1.0,
    # === Replay buffer ===
    # Size of the replay buffer. Note that if async_updates is set, then
    # each worker will have a replay buffer of this size.
    "buffer_size": 50000,
    # If True prioritized replay buffer will be used.
    "prioritized_replay": True,
    # Alpha parameter for prioritized replay buffer.
    "prioritized_replay_alpha": 0.6,
    # Beta parameter for sampling from prioritized replay buffer.
    "prioritized_replay_beta": 0.4,
    # Fraction of entire training period over which the beta parameter is
    # annealed.
    "prioritized_replay_beta_annealing_timesteps": 20000,

    # Final value of beta
    "final_prioritized_replay_beta": 0.4,
    # Epsilon to add to the TD errors when updating priorities.
    "prioritized_replay_eps": 1e-6,
    # Whether to LZ4 compress observations
    "compress_observations": True,

    # === Optimization ===
    # Learning rate for adam optimizer
    "lr": 5e-4,
    # Learning rate schedule
    "lr_schedule": None,
    # Adam epsilon hyper parameter
    "adam_epsilon": 1e-8,
    # If not None, clip gradients during optimization at this value
    "grad_norm_clipping": 40,
    # How many steps of the model to sample before learning starts.
    "learning_starts": 1000,
    # Update the replay buffer with this many samples at once. Note that
    # this setting applies per-worker if num_workers > 1.
    "sample_batch_size": 4,
    # Size of a batched sampled from replay buffer for training. Note that
    # if async_updates is set, then each worker returns gradients for a
    # batch of this size.
    "train_batch_size": 32,

    # === Parallelism ===
    # Number of workers for collecting samples with. This only makes sense
    # to increase if your environment is particularly slow to sample, or if
    # you"re using the Async or Ape-X optimizers.
    "num_workers": 0,
    # Whether to compute priorities on workers.
    "worker_side_prioritization": False,
    # Prevent iterations from going lower than this time span
    "min_iter_time_s": 1,
})
# __sphinx_doc_end__
# yapf: enable


def make_policy_optimizer(workers, config):
    """
    Returns:
        SyncReplayOptimizer: Used for generic off-policy Trainers.
    """
    return SyncReplayOptimizer(
        workers,
        # TODO(sven): Move all PR-beta decays into Schedule components.
        learning_starts=config["learning_starts"],
        buffer_size=config["buffer_size"],
        prioritized_replay=config["prioritized_replay"],
        prioritized_replay_alpha=config["prioritized_replay_alpha"],
        prioritized_replay_beta=config["prioritized_replay_beta"],
        prioritized_replay_beta_annealing_timesteps=config[
            "prioritized_replay_beta_annealing_timesteps"],
        final_prioritized_replay_beta=config["final_prioritized_replay_beta"],
        prioritized_replay_eps=config["prioritized_replay_eps"],
        train_batch_size=config["train_batch_size"],
        **config["optimizer"])


def validate_config(config):
    """
    Checks and updates the config based on settings.
    Rewrites sample_batch_size to take into account n_step truncation.
    """
    # PyTorch check.
    if config["use_pytorch"]:
        raise ValueError("DQN does not support PyTorch yet! Use tf instead.")

    # Update effective batch size to include n-step
    adjusted_batch_size = max(config["sample_batch_size"],
                              config.get("n_step", 1))
    config["sample_batch_size"] = adjusted_batch_size

    # Backward compatibility of epsilon-exploration config AND beta-annealing
    # fraction settings (both based on schedule_max_timesteps, which is
    # deprecated).
    schedule_max_timesteps = None
    if "schedule_max_timesteps" in config:
        schedule_max_timesteps = config["schedule_max_timesteps"]
    if "exploration_final_eps" in config:
        deprecation_warning("exploration_final_eps",
                            "exploration.final_epsilon")
        config["exploration"] = config.get("exploration",
                                           {"type": EpsilonGreedy})
        config["exploration"]["final_p"] = config.pop("exploration_final_eps")
    if "exploration_fraction" in config:
        assert schedule_max_timesteps is not None
        deprecation_warning("exploration_fraction",
                            "exploration.final_timestep")
        config["exploration"] = config.get("exploration",
                                           {"type": EpsilonGreedy})
        config["exploration"]["final_timestep"] = config.pop(
            "exploration_fraction") * schedule_max_timesteps
    if "beta_annealing_fraction" in config:
        assert schedule_max_timesteps is not None
        deprecation_warning("beta_annealing_fraction",
                            "prioritized_replay_beta_annealing_timesteps")
        config["prioritized_replay_beta_annealing_timesteps"] = config.pop(
            "beta_annealing_fraction") * schedule_max_timesteps
    if "per_worker_exploration" in config:
        deprecation_warning(
            "per_worker_exploration",
            "exploration(must be type=EpsilonGreedy).fixed_per_worker_epsilon")
        config["exploration"]["fixed_per_worker_epsilon"] = config.pop(
            "per_worker_exploration")

    # Setup parameter noise.
    if config.get("parameter_noise", False):
        if config["batch_mode"] != "complete_episodes":
            raise ValueError("Exploration with parameter space noise requires "
                             "batch_mode to be complete_episodes.")
        if config.get("noisy", False):
            raise ValueError("Exploration with parameter space noise and "
                             "noisy network cannot be used at the same time.")

        start_callback = config["callbacks"].get("on_episode_start")

        def on_episode_start(info):
            # as a callback function to sample and pose parameter space
            # noise on the parameters of network
            policies = info["policy"]
            for pi in policies.values():
                pi.add_parameter_noise()
            if start_callback is not None:
                start_callback(info)

        config["callbacks"]["on_episode_start"] = on_episode_start

        end_callback = config["callbacks"].get("on_episode_end")

        def on_episode_end(info):
            # as a callback function to monitor the distance
            # between noisy policy and original policy
            policies = info["policy"]
            episode = info["episode"]
            model = policies[DEFAULT_POLICY_ID].model
            if hasattr(model, "pi_distance"):
                episode.custom_metrics["policy_distance"] = model.pi_distance
            if end_callback is not None:
                end_callback(info)

        config["callbacks"]["on_episode_end"] = on_episode_end


def get_initial_state(config):
    return {
        "last_target_update_ts": 0,
        "num_target_updates": 0,
    }


# TODO: Move this to generic Trainer/Policy. Every Algo should do this.
def before_train_step(trainer):
    """
    Sets epsilon exploration values in all policies
    to updated values (according to current time-step).

    Args:
        trainer (Trainer): The Trainer object for the DQN.
    """
    # Store some data for metrics after learning.
    # TODO(sven): Move this to generic Trainer (every Trainer should log this).
    global_timestep = trainer.optimizer.num_steps_sampled
    trainer.train_start_timestep = global_timestep

    # Get all current exploration-infos (from Policies, which cache this info).
    trainer.exploration_infos = trainer.workers.foreach_trainable_policy(
        lambda p, _: p.last_exploration_info)


def after_train_result(trainer, result):
    """
    Add some DQN specific metrics to results.
    """
    global_timestep = trainer.optimizer.num_steps_sampled
    result.update(
        timesteps_this_iter=global_timestep - trainer.train_start_timestep,
        info=dict({
            "exploration_infos": trainer.exploration_infos,
            "num_target_updates": trainer.state["num_target_updates"],
        }, **trainer.optimizer.stats()))


def after_optimizer_step(trainer, fetches):
    """
    Update the target network in configured intervals.
    """
    global_timestep = trainer.optimizer.num_steps_sampled
    if global_timestep - trainer.state["last_target_update_ts"] > \
            trainer.config["target_network_update_freq"]:
        trainer.workers.local_worker().foreach_trainable_policy(
            lambda p, _: p.update_target())
        trainer.state["last_target_update_ts"] = global_timestep
        trainer.state["num_target_updates"] += 1


GenericOffPolicyTrainer = build_trainer(
    name="GenericOffPolicyAlgorithm",
    default_policy=None,
    default_config=DEFAULT_CONFIG,
    validate_config=validate_config,
    get_initial_state=get_initial_state,
    make_policy_optimizer=make_policy_optimizer,
    before_train_step=before_train_step,
    after_optimizer_step=after_optimizer_step,
    after_train_result=after_train_result)

DQNTrainer = GenericOffPolicyTrainer.with_updates(
    name="DQN", default_policy=DQNTFPolicy, default_config=DEFAULT_CONFIG)

SimpleQTrainer = DQNTrainer.with_updates(default_policy=SimpleQPolicy)
