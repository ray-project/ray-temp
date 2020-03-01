from ray.rllib.agents.trainer import with_common_config
from ray.rllib.agents.dqn.dqn import GenericOffPolicyTrainer
from ray.rllib.agents.sac.sac_policy import SACTFPolicy

OPTIMIZER_SHARED_CONFIGS = [
    "buffer_size", "prioritized_replay", "prioritized_replay_alpha",
    "prioritized_replay_beta", "prioritized_replay_eps", "sample_batch_size",
    "train_batch_size", "learning_starts"
]

# yapf: disable
# __sphinx_doc_begin__
DEFAULT_CONFIG = with_common_config({
    # === Model ===
    "twin_q": False,
    "use_state_preprocessor": False,
    #"policy": "GaussianLatentSpacePolicy",
    # RLlib model options for the Q function
    "Q_model": {
        "hidden_activation": "relu",
        "hidden_layer_sizes": (256, 256),
    },
    # RLlib model options for the policy function
    "policy_model": {
        "hidden_activation": "relu",
        "hidden_layer_sizes": (256, 256),
    },
    # Unsquash actions to the upper and lower bounds of env's action space.
    # Ignored for discrete action spaces.
    "normalize_actions": True,

    # === Learning ===
    # Update the target by \tau * policy + (1-\tau) * target_policy.
    "tau": 1.0,  # 5e-3,
    # Initial value to use for the entropy weight alpha.
    "initial_alpha": 10.0,
    # Target entropy lower bound. If "auto", will be set to -|A| (e.g. -2.0 for
    # Discrete(2), -3.0 for Box(shape=(3,))).
    # This is the inverse of reward scale, and will be optimized automatically.
    "target_entropy": "auto",
    # Disable setting done=True at end of episode.
    "no_done_at_end": False,
    # N-step target updates.
    "n_step": 1,

    ## The GumbelSoftmax temperature parameter to use, iff action space is
    ## discrete and SAC uses GumbelSoftmax to sample.
    #"gumbel_softmax_temperature": 1.0,

    # === Evaluation ===
    # The evaluation stats will be reported under the "evaluation" metric key.
    #"evaluation_interval": 1,
    # Number of episodes to run per evaluation period.
    #"evaluation_num_episodes": 1,
    # Extra configuration that disables exploration.
    #"evaluation_config": {
    #    "explore": False,
    #},

    # Number of env steps to optimize for before returning.
    "timesteps_per_iteration": 100,

    # === Replay buffer ===
    # Size of the replay buffer. Note that if async_updates is set, then
    # each worker will have a replay buffer of this size.
    "buffer_size": 1000000,
    # If True prioritized replay buffer will be used.
    "prioritized_replay": True,
    "prioritized_replay_alpha": 0.6,
    "prioritized_replay_beta": 0.4,
    "prioritized_replay_eps": 1e-6,
    "prioritized_replay_beta_annealing_timesteps": 20000,
    "final_prioritized_replay_beta": 0.4,

    "compress_observations": False,

    # === Optimization ===
    "optimization": {
        "actor_learning_rate": 5e-4,
        "critic_learning_rate": 5e-4,
        "entropy_learning_rate": 5e-5,
    },
    # If not None, clip gradients during optimization at this value.
    "grad_norm_clipping": 2.0,
    # How many steps of the model to sample before learning starts.
    "learning_starts": 1500,
    # Update the replay buffer with this many samples at once. Note that this
    # setting applies per-worker if num_workers > 1.
    "sample_batch_size": 1,
    # Size of a batched sampled from replay buffer for training. Note that
    # if async_updates is set, then each worker returns gradients for a
    # batch of this size.
    "train_batch_size": 128,
    # Update the target network every `target_network_update_freq` steps.
    "target_network_update_freq": 1000,

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


SACTrainer = GenericOffPolicyTrainer.with_updates(
    name="SAC",
    default_config=DEFAULT_CONFIG,
    default_policy=SACTFPolicy,
)
