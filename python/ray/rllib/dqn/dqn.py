from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time

import numpy as np
import pickle
import os
import random
import tensorflow as tf

import ray
from ray.rllib.agent import Agent
from ray.rllib.dqn import logger, models
from ray.rllib.dqn.common.wrappers import wrap_dqn
from ray.rllib.dqn.common.schedules import LinearSchedule
from ray.rllib.dqn.replay_buffer import ReplayBuffer, PrioritizedReplayBuffer
from ray.rllib.ppo.filter import RunningStat
from ray.tune.result import TrainingResult


"""The default configuration dict for the DQN algorithm.

    dueling: bool
        whether to use dueling dqn
    double_q: bool
        whether to use double dqn
    hiddens: array<int>
        hidden layer sizes of the state and action value networks
    model: dict
        config options to pass to the model constructor
    lr: float
        learning rate for adam optimizer
    schedule_max_timesteps: int
        max num timesteps for annealing schedules
    timesteps_per_iteration: int
        number of env steps to optimize for before returning
    buffer_size: int
        size of the replay buffer
    exploration_fraction: float
        fraction of entire training period over which the exploration rate is
        annealed
    exploration_final_eps: float
        final value of random action probability
    sample_batch_size: int
        update the replay buffer with this many samples at once
    num_workers: int
        the number of workers to use for parallel batch sample collection
    train_batch_size: int
        size of a batched sampled from replay buffer for training
    print_freq: int
        how often to print out training progress
        set to None to disable printing
    learning_starts: int
        how many steps of the model to collect transitions for before learning
        starts
    gamma: float
        discount factor
    grad_norm_clipping: int or None
        if not None, clip gradients during optimization at this value
    clip_loss_stdev:
        if not None, clips the objective at this stdev
    target_network_update_freq: int
        update the target network every `target_network_update_freq` steps.
    prioritized_replay: True
        if True prioritized replay buffer will be used.
    prioritized_replay_alpha: float
        alpha parameter for prioritized replay buffer
    prioritized_replay_beta0: float
        initial value of beta for prioritized replay buffer
    prioritized_replay_beta_iters: int
        number of iterations over which beta will be annealed from initial
        value to 1.0. If set to None equals to schedule_max_timesteps
    prioritized_replay_eps: float
        epsilon to add to the TD errors when updating priorities.
    num_cpu: int
        number of cpus to use for training
"""
DEFAULT_CONFIG = dict(
    async=False,
    dueling=True,
    double_q=True,
    hiddens=[256],
    model={},
    lr=5e-4,
    schedule_max_timesteps=100000,
    timesteps_per_iteration=1000,
    buffer_size=50000,
    exploration_fraction=0.1,
    exploration_final_eps=0.02,
    sample_batch_size=1,
    num_workers=1,
    use_gpu_for_workers=False,
    train_batch_size=32,
    sgd_batch_size=32,
    num_sgd_iter=1,
    print_freq=1,
    learning_starts=1000,
    gamma=1.0,
    grad_norm_clipping=10,
    clip_loss_stdev=None,
    tf_session_args={
        "device_count": {"CPU": 2},
        "log_device_placement": False,
        "allow_soft_placement": True,
        "inter_op_parallelism_threads": 1,
        "intra_op_parallelism_threads": 1,
    },
    target_network_update_freq=500,
    prioritized_replay=True,
    prioritized_replay_alpha=0.6,
    prioritized_replay_beta0=0.4,
    prioritized_replay_beta_iters=None,
    prioritized_replay_eps=1e-6,

    # Multi gpu options
    multi_gpu_optimize=False,
    devices=["/gpu:0"])


class Actor(object):
    def __init__(self, env_creator, config, logdir):
        env = env_creator()
        env = wrap_dqn(env, config["model"])
        self.env = env
        self.config = config

        tf_config = tf.ConfigProto(**config["tf_session_args"])
        self.sess = tf.Session(config=tf_config)
        self.dqn_graph = models.DQNGraph(env, config, logdir)

        # Create the replay buffer
        if config["prioritized_replay"]:
            self.replay_buffer = PrioritizedReplayBuffer(
                config["buffer_size"],
                alpha=config["prioritized_replay_alpha"])
            prioritized_replay_beta_iters = \
                config["prioritized_replay_beta_iters"]
            if prioritized_replay_beta_iters is None:
                prioritized_replay_beta_iters = \
                    config["schedule_max_timesteps"]
            self.beta_schedule = LinearSchedule(
                prioritized_replay_beta_iters,
                initial_p=config["prioritized_replay_beta0"],
                final_p=1.0)
        else:
            self.replay_buffer = ReplayBuffer(config["buffer_size"])
            self.beta_schedule = None
        # Create the schedule for exploration starting from 1.
        self.exploration = LinearSchedule(
            schedule_timesteps=int(
                config["exploration_fraction"] *
                config["schedule_max_timesteps"]),
            initial_p=1.0,
            final_p=config["exploration_final_eps"])

        # Initialize the parameters and copy them to the target network.
        self.sess.run(tf.global_variables_initializer())
        self.dqn_graph.update_target(self.sess)
        self.set_weights_time = RunningStat(())
        self.sample_time = RunningStat(())
        self.grad_time = RunningStat(())

        # Note that workers don't need target vars to be synced
        self.variables = ray.experimental.TensorFlowVariables(
            tf.group(self.dqn_graph.q_t, self.dqn_graph.q_tp1), self.sess)

        self.episode_rewards = [0.0]
        self.episode_lengths = [0.0]
        self.saved_mean_reward = None
        self.obs = self.env.reset()
        self.file_writer = tf.summary.FileWriter(logdir, self.sess.graph)

    def step(self, cur_timestep):
        # Take action and update exploration to the newest value
        action = self.dqn_graph.act(
            self.sess, np.array(self.obs)[None],
            self.exploration.value(cur_timestep))[0]
        new_obs, rew, done, _ = self.env.step(action)
        ret = (self.obs, action, rew, new_obs, float(done))
        self.obs = new_obs
        self.episode_rewards[-1] += rew
        self.episode_lengths[-1] += 1
        if done:
            self.obs = self.env.reset()
            self.episode_rewards.append(0.0)
            self.episode_lengths.append(0.0)
        return ret

    def collect_steps(self, num_steps, cur_timestep):
        steps = []
        for _ in range(num_steps):
            steps.append(self.step(cur_timestep))
        return steps

    def do_steps(self, num_steps, cur_timestep):
        for _ in range(num_steps):
            obs, action, rew, new_obs, done = self.step(cur_timestep)
            self.replay_buffer.add(obs, action, rew, new_obs, done)

    def do_multi_gpu_optimize(self, cur_timestep):
        dt = time.time()
        if self.config["prioritized_replay"]:
            experience = self.replay_buffer.sample(
                self.config["train_batch_size"],
                beta=self.beta_schedule.value(cur_timestep))
            (obses_t, actions, rewards, obses_tp1,
                dones, _, batch_idxes) = experience
        else:
            obses_t, actions, rewards, obses_tp1, dones = \
                self.replay_buffer.sample(self.config["train_batch_size"])
            batch_idxes = None
        replay_buffer_read_time = (time.time() - dt)
        dt = time.time()
        tuples_per_device = self.dqn_graph.multi_gpu_optimizer.load_data(
            self.sess,
            [obses_t, actions, rewards, obses_tp1, dones,
             np.ones_like(rewards)])
        per_device_batch_size = (
            self.dqn_graph.multi_gpu_optimizer.per_device_batch_size)
        num_batches = (int(tuples_per_device) // int(per_device_batch_size))
        data_load_time = (time.time() - dt)
        dt = time.time()
        for _ in range(self.config["num_sgd_iter"]):
            batches = list(range(num_batches))
            random.shuffle(batches)
            for i in batches:
                self.dqn_graph.multi_gpu_optimizer.optimize(
                    self.sess, i * per_device_batch_size)
        sgd_time = (time.time() - dt)
        dt = time.time()
        if self.config["prioritized_replay"]:
            dt = time.time()
            td_errors = self.dqn_graph.compute_td_error(
                self.sess, obses_t, actions, rewards, obses_tp1, dones,
                np.ones_like(rewards))
            dt = time.time()
            new_priorities = (
                np.abs(td_errors) + self.config["prioritized_replay_eps"])
            self.replay_buffer.update_priorities(
                batch_idxes, new_priorities)
        prioritization_time = (time.time() - dt)
        return (
            replay_buffer_read_time, data_load_time, sgd_time,
            prioritization_time)

    def compute_gradient(self, worker_id, cur_timestep, params, gradient_id):
        dt = time.time()
        self.set_weights(params)
        self.set_weights_time.push(time.time() - dt)
        dt = time.time()
        self.do_steps(self.config["sample_batch_size"], cur_timestep)
        self.sample_time.push(time.time() - dt)
        if (cur_timestep > self.config["learning_starts"] and
               len(self.replay_buffer) > self.config["train_batch_size"]):
            dt = time.time()
            gradient = self.get_gradient(cur_timestep)
            self.grad_time.push(time.time() - dt)
        else:
            gradient = None
        return gradient, {"id": worker_id, "gradient_id": gradient_id}

    def get_gradient(self, cur_timestep):
        if self.config["prioritized_replay"]:
            experience = self.replay_buffer.sample(
                self.config["sgd_batch_size"],
                beta=self.beta_schedule.value(cur_timestep))
            (obses_t, actions, rewards, obses_tp1,
                dones, _, batch_idxes) = experience
        else:
            obses_t, actions, rewards, obses_tp1, dones = \
                self.replay_buffer.sample(self.config["sgd_batch_size"])
            batch_idxes = None
        td_errors, grad = self.dqn_graph.compute_gradients(
            self.sess, obses_t, actions, rewards, obses_tp1, dones,
            np.ones_like(rewards))
        if self.config["prioritized_replay"]:
            new_priorities = (
                np.abs(td_errors) + self.config["prioritized_replay_eps"])
            self.replay_buffer.update_priorities(
                batch_idxes, new_priorities)
        return grad

    def apply_gradients(self, grad):
        self.dqn_graph.apply_gradients(self.sess, grad)

    def stats(self, num_timesteps):
        mean_100ep_reward = round(np.mean(self.episode_rewards[-101:-1]), 1)
        mean_100ep_length = round(np.mean(self.episode_lengths[-101:-1]), 1)
        exploration = self.exploration.value(num_timesteps)
        return (
            mean_100ep_reward,
            mean_100ep_length,
            len(self.episode_rewards),
            exploration,
            len(self.replay_buffer),
            float(self.set_weights_time.mean),
            float(self.sample_time.mean),
            float(self.grad_time.mean))

    def get_weights(self):
        return self.variables.get_weights()

    def set_weights(self, weights):
        self.variables.set_weights(weights)

    def save(self):
        return [
            self.beta_schedule,
            self.exploration,
            self.episode_rewards,
            self.episode_lengths,
            self.saved_mean_reward,
            self.obs,
            self.replay_buffer]

    def restore(self, data):
        self.beta_schedule = data[0]
        self.exploration = data[1]
        self.episode_rewards = data[2]
        self.episode_lengths = data[3]
        self.saved_mean_reward = data[4]
        self.obs = data[5]
        self.replay_buffer = data[6]


@ray.remote
class RemoteActor(Actor):
    def __init__(self, env_creator, config, logdir):
        Actor.__init__(self, env_creator, config, logdir)

    def stop(self):
        sys.exit(0)


@ray.remote(num_gpus=1)
class GPURemoteActor(Actor):
    def __init__(self, env_creator, config, logdir):
        Actor.__init__(self, env_creator, config, logdir)

    def stop(self):
        sys.exit(0)


class DQNAgent(Agent):
    _agent_name = "DQN"
    _default_config = DEFAULT_CONFIG

    def stop(self):
        for w in self.workers:
            w.stop.remote()

    def _init(self):
        self.actor = Actor(self.env_creator, self.config, self.logdir)
        if self.config["use_gpu_for_workers"]:
            remote_cls = GPURemoteActor
        else:
            remote_cls = RemoteActor
        # Use remote workers
        if self.config["num_workers"] > 1 or self.config["async"]:
            self.workers = [
                remote_cls.remote(self.env_creator, self.config, self.logdir)
                    for i in range(self.config["num_workers"])]
        else:
            # Use a single local worker and avoid object store overheads
            self.workers = []

        self.cur_timestep = 0
        self.num_iterations = 0
        self.num_target_updates = 0
        self.steps_since_update = 0
        self.file_writer = tf.summary.FileWriter(
            self.logdir, self.actor.sess.graph)
        self.saver = tf.train.Saver(max_to_keep=None)

    def _update_worker_weights(self):
        if self.workers:
            w = self.actor.get_weights()
            weights = ray.put(self.actor.get_weights())
            for w in self.workers:
                w.set_weights.remote(weights)

    def _train(self):
        if self.config["async"]:
            return self._train_async()
        else:
            return self._train_sync()

    def _train_async(self):
        apply_time = RunningStat(())
        wait_time = RunningStat(())
        gradient_lag = RunningStat(())
        iter_init_timesteps = self.cur_timestep
        num_gradients_applied = 0
        gradient_list = [
            worker.compute_gradient.remote(
                i, self.cur_timestep, self.actor.get_weights(),
                num_gradients_applied)
            for i, worker in enumerate(self.workers)]
        steps = self.config["sample_batch_size"] * len(gradient_list)
        self.cur_timestep += steps
        self.steps_since_update += steps

        while gradient_list:
            dt = time.time()
            gradient, info = ray.get(gradient_list[0])
            gradient_list = gradient_list[1:]
            wait_time.push(time.time() - dt)

            if gradient is not None:
                dt = time.time()
                self.actor.apply_gradients(gradient)
                apply_time.push(time.time() - dt)
                gradient_lag.push(num_gradients_applied - info["gradient_id"])
                num_gradients_applied += 1

            if (self.cur_timestep - iter_init_timesteps <
                    self.config["timesteps_per_iteration"]):
                worker_id = info["id"]
                gradient_list.append(
                    self.workers[info["id"]].compute_gradient.remote(
                        worker_id, self.cur_timestep,
                        self.actor.get_weights(), num_gradients_applied))
                self.cur_timestep += self.config["sample_batch_size"]
                self.steps_since_update += self.config["sample_batch_size"]

            if (self.cur_timestep > self.config["learning_starts"] and
                    self.steps_since_update >
                    self.config["target_network_update_freq"]):
                # Update target network periodically.
                self.actor.dqn_graph.update_target(self.actor.sess)
                self.steps_since_update -= (
                    self.config["target_network_update_freq"])
                self.num_target_updates += 1

        mean_100ep_reward = 0.0
        mean_100ep_length = 0.0
        num_episodes = 0
        buffer_size_sum = 0
        stats = ray.get(
            [w.stats.remote(self.cur_timestep) for w in self.workers])
        for stat in stats:
            mean_100ep_reward += stat[0]
            mean_100ep_length += stat[1]
            num_episodes += stat[2]
            exploration = stat[3]
            buffer_size_sum += stat[4]
            set_weights_time = stat[5]
            sample_time = stat[6]
            grad_time = stat[7]
        mean_100ep_reward /= self.config["num_workers"]
        mean_100ep_length /= self.config["num_workers"]

        info = [
            ("mean_100ep_reward", mean_100ep_reward),
            ("exploration_frac", exploration),
            ("steps", self.cur_timestep),
            ("episodes", num_episodes),
            ("buffer_sizes_sum", buffer_size_sum),
            ("target_updates", self.num_target_updates),
            ("mean_set_weights_time", set_weights_time),
            ("mean_sample_time", sample_time),
            ("mean_grad_time", grad_time),
            ("mean_apply_time", float(apply_time.mean)),
            ("mean_ray_wait_time", float(wait_time.mean)),
            ("gradient_lag_mean", float(gradient_lag.mean)),
            ("gradient_lag_stdev", float(gradient_lag.std)),
        ]

        for k, v in info:
            logger.record_tabular(k, v)
        logger.dump_tabular()

        result = TrainingResult(
            episode_reward_mean=mean_100ep_reward,
            episode_len_mean=mean_100ep_length,
            timesteps_this_iter=self.cur_timestep - iter_init_timesteps,
            info=info)

        return result

    def _train_sync(self):
        config = self.config
        sample_time, sync_time, learn_time, apply_time = 0, 0, 0, 0
        iter_init_timesteps = self.cur_timestep

        num_loop_iters = 0
        while (self.cur_timestep - iter_init_timesteps <
               config["timesteps_per_iteration"]):
            dt = time.time()
            if self.workers:
                worker_steps = ray.get([
                    w.collect_steps.remote(
                        config["sample_batch_size"] // len(self.workers),
                        self.cur_timestep)
                    for w in self.workers])
                for steps in worker_steps:
                    for obs, action, rew, new_obs, done in steps:
                        self.actor.replay_buffer.add(
                            obs, action, rew, new_obs, done)
            else:
                self.actor.do_steps(
                    config["sample_batch_size"], self.cur_timestep)
            num_loop_iters += 1
            self.cur_timestep += config["sample_batch_size"]
            self.steps_since_update += config["sample_batch_size"]
            sample_time += time.time() - dt

            if self.cur_timestep > config["learning_starts"]:
                if config["multi_gpu_optimize"]:
                    dt = time.time()
                    times = self.actor.do_multi_gpu_optimize(self.cur_timestep)
                    if num_loop_iters <= 1:
                        print("Multi-GPU times", times)
                    learn_time += (time.time() - dt)
                else:
                    # Minimize the error in Bellman's equation on a batch
                    # sampled from replay buffer.
                    for _ in range(max(1, config["train_batch_size"] // config["sgd_batch_size"])):
                        dt = time.time()
                        gradients = [
                            self.actor.get_gradient(self.cur_timestep)]
                        learn_time += (time.time() - dt)
                        dt = time.time()
                        for grad in gradients:
                            self.actor.apply_gradients(grad)
                        apply_time += (time.time() - dt)
                dt = time.time()
                self._update_worker_weights()
                sync_time += (time.time() - dt)

            if (self.cur_timestep > config["learning_starts"] and
                    self.steps_since_update >
                    config["target_network_update_freq"]):
                # Update target network periodically.
                self.actor.dqn_graph.update_target(self.actor.sess)
                self.steps_since_update -= config["target_network_update_freq"]
                self.num_target_updates += 1

        mean_100ep_reward = 0.0
        mean_100ep_length = 0.0
        num_episodes = 0
        buffer_size_sum = 0
        if not self.workers:
            stats = self.actor.stats(self.cur_timestep)
            mean_100ep_reward += stats[0]
            mean_100ep_length += stats[1]
            num_episodes += stats[2]
            exploration = stats[3]
            buffer_size_sum += stats[4]
        for mean_rew, mean_len, episodes, exploration, buf_sz in ray.get(
              [w.stats.remote(self.cur_timestep) for w in self.workers]):
            mean_100ep_reward += mean_rew
            mean_100ep_length += mean_len
            num_episodes += episodes
            buffer_size_sum += buf_sz
        mean_100ep_reward /= config["num_workers"]
        mean_100ep_length /= config["num_workers"]

        info = [
            ("mean_100ep_reward", mean_100ep_reward),
            ("exploration_frac", exploration),
            ("steps", self.cur_timestep),
            ("episodes", num_episodes),
            ("buffer_sizes_sum", buffer_size_sum),
            ("target_updates", self.num_target_updates),
            ("sample_time", sample_time),
            ("weight_sync_time", sync_time),
            ("apply_time", apply_time),
            ("learn_time", learn_time),
            ("samples_per_s",
                num_loop_iters * np.float64(config["sample_batch_size"]) /
                sample_time),
            ("learn_samples_per_s",
                num_loop_iters * np.float64(config["train_batch_size"]) /
                learn_time),
        ]

        for k, v in info:
            logger.record_tabular(k, v)
        logger.dump_tabular()

        result = TrainingResult(
            episode_reward_mean=mean_100ep_reward,
            episode_len_mean=mean_100ep_length,
            timesteps_this_iter=self.cur_timestep - iter_init_timesteps,
            info=info)

        return result

    def _save(self):
        checkpoint_path = self.saver.save(
            self.actor.sess,
            os.path.join(self.logdir, "checkpoint"),
            global_step=self.num_iterations)
        extra_data = [
            self.actor.save(),
            ray.get([w.save.remote() for w in self.workers]),
            self.cur_timestep,
            self.num_iterations,
            self.num_target_updates,
            self.steps_since_update]
        pickle.dump(extra_data, open(checkpoint_path + ".extra_data", "wb"))
        return checkpoint_path

    def _restore(self, checkpoint_path):
        self.saver.restore(self.actor.sess, checkpoint_path)
        extra_data = pickle.load(open(checkpoint_path + ".extra_data", "rb"))
        self.actor.restore(extra_data[0])
        ray.get([
            w.restore.remote(d) for (d, w)
            in zip(extra_data[1], self.workers)])
        self.cur_timestep = extra_data[2]
        self.num_iterations = extra_data[3]
        self.num_target_updates = extra_data[4]
        self.steps_since_update = extra_data[5]

    def compute_action(self, observation):
        return self.actor.dqn_graph.act(
            self.actor.sess, np.array(observation)[None], 0.0)[0]
