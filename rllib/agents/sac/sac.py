from ray.rllib.agents.trainer import with_common_config
from ray.rllib.agents.dqn.dqn import GenericOffPolicyTrainer
from ray.rllib.agents.sac.sac_tf_policy import SACTFPolicy

OPTIMIZER_SHARED_CONFIGS = [
    "buffer_size", "prioritized_replay", "prioritized_replay_alpha",
    "prioritized_replay_beta", "prioritized_replay_eps",
    "rollout_fragment_length", "train_batch_size", "learning_starts"
]

# yapf: disable
# __sphinx_doc_begin__
DEFAULT_CONFIG = with_common_config({
    # === Model ===
    "twin_q": True,
    "use_state_preprocessor": False,
    # RLlib model options for the Q function(s).
    "Q_model": {
        "fcnet_activation": "relu",
        "fcnet_hiddens": [256, 256],
    },
    # RLlib model options for the policy function.
    "policy_model": {
        "fcnet_activation": "relu",
        "fcnet_hiddens": [256, 256],
    },
    # Unsquash actions to the upper and lower bounds of env's action space.
    # Ignored for discrete action spaces.
    "normalize_actions": True,

    # === Learning ===
    # Disable setting done=True at end of episode. This should be set to True
    # for infinite-horizon MDPs (e.g., many continuous control problems).
    "no_done_at_end": False,
    # Update the target by \tau * policy + (1-\tau) * target_policy.
    "tau": 5e-3,
    # Initial value to use for the entropy weight alpha.
    "initial_alpha": 1.0,
    # Target entropy lower bound. If "auto", will be set to -|A| (e.g. -2.0 for
    # Discrete(2), -3.0 for Box(shape=(3,))).
    # This is the inverse of reward scale, and will be optimized automatically.
    "target_entropy": "auto",
    # N-step target updates.
    "n_step": 1,

    # Number of env steps to optimize for before returning.
    "timesteps_per_iteration": 100,

    # === Replay buffer ===
    # Size of the replay buffer. Note that if async_updates is set, then
    # each worker will have a replay buffer of this size.
    "buffer_size": int(1e6),
    # If True prioritized replay buffer will be used.
    "prioritized_replay": False,
    "prioritized_replay_alpha": 0.6,
    "prioritized_replay_beta": 0.4,
    "prioritized_replay_eps": 1e-6,
    "prioritized_replay_beta_annealing_timesteps": 20000,
    "final_prioritized_replay_beta": 0.4,

    "compress_observations": False,

    # === Optimization ===
    "optimization": {
        "actor_learning_rate": 3e-4,
        "critic_learning_rate": 3e-4,
        "entropy_learning_rate": 3e-4,
    },
    # If not None, clip gradients during optimization at this value.
    "grad_norm_clipping": None,
    # How many steps of the model to sample before learning starts.
    "learning_starts": 1500,
    # Update the replay buffer with this many samples at once. Note that this
    # setting applies per-worker if num_workers > 1.
    "rollout_fragment_length": 1,
    # Size of a batched sampled from replay buffer for training. Note that
    # if async_updates is set, then each worker returns gradients for a
    # batch of this size.
    "train_batch_size": 256,
    # Update the target network every `target_network_update_freq` steps.
    "target_network_update_freq": 0,

    # === Parallelism ===
    # Whether to use a GPU for local optimization.
    "num_gpus": 0,
    # Number of workers for collecting samples with. This only makes sense
    # to increase if your environment is particularly slow to sample, or if
    # you"re using the Async or Ape-X optimizers.
    "num_workers": 0,
    # Whether to allocate GPUs for workers (if > 0).
    "num_gpus_per_worker": 0,
    # Whether to allocate CPUs for workers (if > 0).
    "num_cpus_per_worker": 1,
    # Whether to compute priorities on workers.
    "worker_side_prioritization": False,
    # Prevent iterations from going lower than this time span.
    "min_iter_time_s": 1,
})
# __sphinx_doc_end__
# yapf: enable


def validate_config(config):
    """Checks and updates the config based on settings."""
    # Override from DQN.
    pass


def get_policy_class(config):
    if config.get("use_pytorch") is True:
        from ray.rllib.agents.sac.sac_torch_policy import SACTorchPolicy
        return SACTorchPolicy
    else:
        return SACTFPolicy


SACTrainer = GenericOffPolicyTrainer.with_updates(
    name="SAC",
    default_config=DEFAULT_CONFIG,
    default_policy=SACTFPolicy,
    validate_config=validate_config,
    get_policy_class=get_policy_class,
)
