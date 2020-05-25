from gym.spaces import Box, Dict, Tuple
import numpy as np
import mlagents_envs
from mlagents_envs.environment import UnityEnvironment

from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.rllib.utils.annotations import override


class Unity3DEnv(MultiAgentEnv):
    """A wrapper for a single Unity3D instance acting as an ExternalEnv.

    Supports vectorized Unity3D examples and RLlib Multi-Agent Training.
    """

    def __init__(self,
                 file_name=None,
                 worker_id=0,
                 base_port=5004,
                 seed=0,
                 no_graphics=False,
                 timeout_wait=60,
                 episode_horizon=1000):
        """Initializes a Unity3DEnv object.

        Args:
            file_name (Optional[str]): Name of Unity environment binary.
            worker_id (int): Number to add to `base_port`. Used for
                asynchronous agent scenarios.
            base_port (int): Port number to connect to Unity environment.
                `worker_id` increments on top of this.
            seed (int): A random seed value to use.
            no_graphics (bool): Whether to run the Unity simulator in
                no-graphics mode. Default: False.
            timeout_wait (int): Time (in seconds) to wait for connection from environment.
            episode_horizon (int):
        """

        super().__init__()

        # Try connecting to the
        while True:
            self.worker_id = worker_id
            try:
                self.unity_env = UnityEnvironment(
                    file_name=file_name,
                    worker_id=worker_id,
                    base_port=base_port,
                    seed=seed,
                    no_graphics=no_graphics,
                    timeout_wait=timeout_wait,
                )
            except mlagents_envs.exception.UnityWorkerInUseException as e:
                worker_id += 1
                # Hard limit.
                if worker_id > 100:
                    raise e
            else:
                break

        # Reset entire env every this number of step calls.
        self.episode_horizon = episode_horizon
        self.global_timesteps = 0

        # Caches the last observation we made (after stepping or resetting).
        #self.last_observations = {}
        # Initial reset.
        #self.unity_env.reset()
        #behaviors = self.unity_env.get_behavior_names()

        ## Figure out observation and action spaces.
        #_, rewards, _ = self._get_step_results()
        #self.num_envs = len(self.unity_env.get_steps(behaviors[0])[0].reward)
        # Set of env IDs requiring actions to be set (before the next Unity3D
        # `step`).
        #self.envs_requiring_actions = {i for i in range(self.num_envs)}
        # Action cache for "unrequired" actions. Unity requires - before each
        # call to `step` - only certain actions to be set. The rest is cached
        # in this dict and used for the next call to `send_actions` so that
        # we don't infer actions for the same obs twice.
        #self.cached_actions = {}  #eid: {} for eid in range(self.num_envs)}

        #env_spec = next(iter(self.unity_env._env_specs.values()))
        ## TODO: (sven): Add support for more than one obs component (currently only [0] considered).
        #self.observation_space = Box(float("-inf"), float("inf"), shape=env_spec.observation_shapes[0])
        #if env_spec.action_type == mlagents_envs.base_env.ActionType.DISCRETE:
        #    self.action_space = Discrete(env_spec.action_shape)
        #else:
        #    self.action_space = Box(float("-inf"), float("inf"), shape=(env_spec.action_shape, ))

    """def poll(self):
        if self.pending is None:
            self.pending = {a.reset.remote(): a for a in self.actors}
    
        # Each keyed by env_id in [0, num_remote_envs).
        obs, rewards, dones, infos = {}, {}, {}, {}
        ready = []
    
        # Wait for at least 1 env to be ready here
        while not ready:
            ready, _ = ray.wait(
                list(self.pending),
                num_returns=len(self.pending),
                timeout=self.poll_timeout)
    
        # Get and return observations for each of the ready envs
        env_ids = set()
        for obj_id in ready:
            actor = self.pending.pop(obj_id)
            env_id = self.actors.index(actor)
            env_ids.add(env_id)
            ob, rew, done, info = ray_get_and_free(obj_id)
            obs[env_id] = ob
            rewards[env_id] = rew
            dones[env_id] = done
            infos[env_id] = info
    
        logger.debug("Got obs batch for actors {}".format(env_ids))
        return obs, rewards, dones, infos, {}
    """

    @override(MultiAgentEnv)
    def step(self, action_dict):
        """

        Args:
            action_dict (dict): Double keyed dict with:
                upper key (int): RLlib Episode (MLAgents: "agent") ID.
                lower key (str): RLlib Agent (MLAgents: "behavior") name.

        Returns:
            tuple:
                obs: Only those observations for which to get new actions.
                rewards: Rewards dicts matching the returned obs.
                dones: Done dicts matching the returned obs.
        """

        #behavior_names = self.unity_env.get_behavior_names()

        # All envs require actions. Set as np.array batch.
        #if len(self.envs_requiring_actions) == self.num_envs:
        #    for behavior_name in behavior_names:
        #        behavior_actions = []
        #        for eid in range(self.num_envs):
        #            # Action is provided in dict.
        #            if eid in action_dict:
        #                behavior_actions.append(action_dict[eid][behavior_name])
        #            # Action must have been cached.
        #            else:
        #                behavior_actions.append(self.cached_actions[eid][behavior_name])
        #                # Free cache.
        #                del self.cached_actions[eid][behavior_name]
        #        self.unity_env.set_actions(behavior_name, np.array(behavior_actions))
        #else:
        # Set only the required actions (from the DecisionSteps) in Unity3D.
        action_dict_copy = action_dict.copy()  # shallow copy
        for behavior_name in self.unity_env.get_behavior_names():
            for agent_id in self.unity_env.get_steps(behavior_name)[
                    0].agent_id_to_index.keys():
                key = behavior_name + "_{}".format(agent_id)
                #if key in action_dict:
                self.unity_env.set_action_for_agent(behavior_name, agent_id,
                                                    action_dict[key])
                #else:
                #    assert key in self.cached_actions
                #    self.unity_env.set_action_for_agent(
                #        behavior_name, agent_id, self.cached_actions[key])
                #    del self.cached_actions[key]
                #del action_dict_copy[key]
        #TODO: cache unused actions here, before(!) calling step.
        #for agent_id, a in action_dict_copy.items():
        #    self.cached_obs[agent_id] = a

        # Do the step.
        self.unity_env.step()

        obs, rewards, dones, infos = self._get_step_results()

        # Global horizon reached? -> Return __all__ done=True, so user
        # can reset.
        self.global_timesteps += 1
        if self.global_timesteps > self.episode_horizon:
            return obs, rewards, {"__all__": True}, infos

        return obs, rewards, dones, infos

    #def step(self, actions, text_actions=None, **kwargs):
    #    # MLAgents Envs don't like tuple-actions.
    #    if isinstance(actions[0], tuple):
    #        actions = [list(a) for a in actions]
    #    all_brain_info = self.mlagents_env.step(
    #        # TODO: Only support vector actions for now.
    #        vector_action=actions, memory=None, text_action=text_actions, value=None
    #    )
    #    self.last_state = self._get_state_from_brain_info(all_brain_info)
    #    r = self._get_reward_from_brain_info(all_brain_info)
    #    t = self._get_terminal_from_brain_info(all_brain_info)
    #    return self.last_state, r, t, None

    @override(MultiAgentEnv)
    def reset(self):
        self.global_timesteps = 0
        self.unity_env.reset()
        obs, _, _, _ = self._get_step_results()
        return obs
        ## Single-env reset, don't do anything on Unity, just return current
        ## obs for that sub-env.
        #return self.last_observations[env_id]

    #def reset(self, index=0):
    #    # Reset entire MLAgentsEnv iff global_done is True.
    #    if self.mlagents_env.global_done is True or self.last_state is None:
    #        self.reset_all()
    #    return self.last_state[index]

    #def reset_all(self):
    #    all_brain_info = self.mlagents_env.reset()
    #    self.last_state = self._get_state_from_brain_info(all_brain_info)
    #    return self.last_state

    #def stop(self):
    #    if self.actors is not None:
    #        for actor in self.actors:
    #            actor.__ray_terminate__.remote()

    #def terminate(self):
    #    self.mlagents_env.close()

    #def terminate_all(self):
    #    return self.terminate()

    #def _get_state_from_brain_info(self, all_brain_info):
    #    brain_info = all_brain_info[self.scene_key]
    #    if self.state_key is None:
    #        return {"vector": list(brain_info.vector_observations), "visual": list(brain_info.visual_observations),
    #                "text": list(brain_info.text_observations)}
    #    elif self.state_key == "vector":
    #        return list(brain_info.vector_observations)
    #    elif self.state_key == "visual":
    #        return list(brain_info.visual_observations)
    #    elif self.state_key == "text":
    #        return list(brain_info.text_observations)

    #def _get_reward_from_brain_info(self, all_brain_info):
    #    brain_info = all_brain_info[self.scene_key]
    #    return [np.array(r_, dtype=np.float32) for r_ in brain_info.rewards]

    #def _get_terminal_from_brain_info(self, all_brain_info):
    #    brain_info = all_brain_info[self.scene_key]
    #    return brain_info.local_done

    #@override(BaseEnv)
    #def stop(self):
    #    self.unity_env.close()

    def _get_step_results(self):
        #num_envs = self.num_envs if hasattr(self, "num_envs") else \
        #    len(next(iter(self.unity_env._env_state.values()))[0].reward)

        obs = {}  #{e: {} for e in range(num_envs)}
        rewards = {}  #{e: {} for e in range(num_envs)}
        #dones = {}
        infos = {}
        #all_done = True
        for behavior_name in self.unity_env.get_behavior_names():
            decision_steps, terminal_steps = self.unity_env.get_steps(
                behavior_name)
            #    self.unity_env._env_state[brain_name]
            # [0] = DecisionSteps
            # Important: Only update those sub-envs that are currently
            # available within _env_state.
            # NOTE: 'agent' in Unity3D translates to (vector) sub-env in RLlib.
            #available_obses = env_state[0].obs[0]
            # Loop through all envs ("agents") and fill in, whatever
            # information we have.
            #self.envs_requiring_actions = set()
            for agent_id, idx in decision_steps.agent_id_to_index.items():
                key = behavior_name + "_{}".format(agent_id)
                #for env_id in range(num_envs):
                #idx = decision_steps.agent_id_to_index.get(env_id)
                #if idx is not None:
                #if env_id not in obs:
                #    obs[env_id] = {}
                #    rewards[env_id] = {}
                os = tuple(o[idx] for o in decision_steps.obs)
                os = os[0] if len(os) == 1 else os
                obs[key] = os
                rewards[key] = decision_steps.reward[idx]  # rewards vector
                #all_done = False
            #self.envs_requiring_actions.add(env_id)
            ## Use last obs and 0.0 reward.
            #elif self.last_observations is not None:
            #    obs[env_id][brain_name] = self.last_observations[env_id][brain_name]  # [0] = TODO: (sven): Only use 1st obs comp for now.
            #    rewards[env_id][brain_name] = 0.0
            for agent_id, idx in terminal_steps.agent_id_to_index.items():
                key = behavior_name + "_{}".format(agent_id)
                #assert key not in obs
                #assert key not in rewards
                # Always create multi-agent __all__ key.
                #if "__all__" not in dones[env_id]:
                #    dones[env_id] = True
                # If one episode (agent) is still not done, set __all__
                # to False.
                #if idx is None:
                #dones[env_id][brain_name] = False
                #if idx is not None:
                #dones[behavior_name + "_{}".format(agent_id)] = True
                #if env_id not in obs:
                #    obs[env_id] = {}
                #    rewards[env_id] = {}
                #os = tuple(o[idx] for o in terminal_steps.obs)
                #os = os[0] if len(os) == 1 else os
                #obs[key] = os
                # Only overwrite rewards (last reward in episode), b/c as obs
                # here is the last obs (which doesn't matter anyways).
                rewards[key] = terminal_steps.reward[idx]  # rewards vector
                #self.envs_requiring_actions.add(env_id)
                # This env currently does not reuiqre a new action input.
                # Cache existing action.
                #if env_id not in self.envs_requiring_actions:
                #    assert action_dict is not None
                #    self.cached_actions[env_id][behavior_name] = action_dict[env_id][behavior_name]
                # This env (agent) is done.
                #else:
                #    dones[env_id][brain_name] = True

            # [1] = TerminalSteps (get done values for the different sub-envs).
            #for env_id, idx in env_state[1].agent_id_to_index.items():
            #    dones[env_id][brain_name] = True
        # Update our current observations.
        #for env_id in obs:
        #    self.last_observations[env_id] = obs[env_id]
        # Only use dones if all agents are done, then we should do a reset.
        return obs, rewards, {"__all__": False}, infos


#import numpy as np
#import gym

#import ray
#from ray.rllib.env.unity3d_wrapper import Unity3DWrapper

#ray.init(local_mode=True)

#unity_env = Unity3DWrapper(100)
#unity_env.reset()
#unity_env_specs = next(iter(unity_env._env_specs.values()))
"""
#!/usr/bin/env python
""Example of running a Unity3D instance against a RLlib Trainer

TODO: Unity3D should be started automatically by this script.
""

import argparse
from mlagents_envs.environment import UnityEnvironment
#from gym_unity.envs import UnityToGymWrapper

from ray.rllib.env.policy_client import PolicyClient
from ray.rllib.env.unity3d_env import Unity3DEnv

parser = argparse.ArgumentParser()
parser.add_argument(
    "--no-train", action="store_true", help="Whether to disable training.")
parser.add_argument(
    "--inference-mode", type=str, required=True, choices=["local", "remote"])
parser.add_argument(
    "--stop-reward",
    type=int,
    default=9999,
    help="Stop once the specified reward is reached.")



if __name__ == "__main__":
    args = parser.parse_args()

    # TODO(sven): Move all this logic into Unity3DWrapper class for RLlib
    #  that's already a PolicyClient, and just has to be "run".
    unity_env = UnityEnvironment()
    unity_env.reset()
    unity_env_spec = unity_env._env_specs
    # Don't wrap, only works with single agent Unity3D examples
    # (e.g. "Basic").
    # env = UnityToGymWrapper(unity_env, use_visual=False, uint8_visual=True)

    client = PolicyClient(
        "http://localhost:9900", inference_mode=args.inference_mode)
    eid = client.start_episode(training_enabled=not args.no_train)

    env = Unity3DEnv()
    env.try_reset()

    # Reset to set a first observation.
    unity_env.reset()
    # Get brain name.
    brain_name = list(unity_env._env_specs.keys())[0]
    num_agents = len(unity_env._env_state[brain_name][0].agent_id)
    obs_batch = unity_env._env_state[brain_name][0].obs[0]  # <- only take 0th component (assume observations are single-component obs).
    obs_batch = [obs_batch[i] for i in range(len(obs_batch))]
    episode_rewards = [0.0 for _ in range(len(obs_batch))]

    while True:
        action = client.get_action(eid, obs_batch)
        # Convert per-env + per-agent actions into Unity-readable action
        # vector.
        s, r, d, _ = env.send_actions(action)

        unity_actions = np.array([action[i]["agent0"] for i in range(len(action))])
        unity_env.set_actions(brain_name, unity_actions)
        unity_env.step()
        obs_batch, rewards, dones = _get_step_results(unity_env, brain_name)
        if len(rewards) != 0:
            episode_rewards += rewards
            client.log_returns(eid, rewards)
        if any(dones):
            print("Agents {} are done.".format(dones))
            print("Total reward:", rewards)
            if any(episode_rewards >= args.stop_reward):
                print("Target reward achieved, exiting.")
                exit(0)
            # Reset episode rewards for done agents.
            for i in dones:
                episode_rewards[i] = 0.0
            client.end_episode(eid, obs)
            obs = env.try_reset()
            obs = unity_env.reset()
            eid = client.start_episode(training_enabled=not args.no_train)
"""
