import numpy as np
from gym.envs.mujoco import HalfCheetahEnv, HopperEnv


class HalfCheetahWrapper(HalfCheetahEnv):
    """Wrapper for the MuJoCo HalfCheetah-v2 environment.

    Adds an additional `reward` method for some model-based RL algos (e.g.
    MB-MPO).
    """

    def reward(self, obs, action, obs_next):
        if obs.ndim == 2 and action.ndim == 2:
            assert obs.shape == obs_next.shape
            forward_vel = obs_next[:, 8]
            ctrl_cost = 0.1 * np.sum(np.square(action), axis=1)
            reward = forward_vel - ctrl_cost
            return np.minimum(np.maximum(-1000.0, reward), 1000.0)
        else:
            forward_vel = obs_next[8]
            ctrl_cost = 0.1 * np.square(action).sum()
            reward = forward_vel - ctrl_cost
            return np.minimum(np.maximum(-1000.0, reward), 1000.0)


class HopperWrapper(HopperEnv):
    """Wrapper for the MuJoCo Hopper-v2 environment.

    Adds an additional `reward` method for some model-based RL algos (e.g.
    MB-MPO).
    """

    def reward(self, obs, action, obs_next):
        alive_bonus = 1.0
        assert obs.ndim == 2 and action.ndim == 2
        assert obs.shape == obs_next.shape and action.shape[0] == obs.shape[0]
        vel = obs_next[:, 5]
        ctrl_cost = 1e-3 * np.sum(np.square(action), axis=1)
        reward = vel + alive_bonus - ctrl_cost
        return np.minimum(np.maximum(-1000.0, reward), 1000.0)


if __name__ == "__main__":
    env = HopperWrapper()
    env.reset()
    for _ in range(1000):
        env.step(env.action_space.sample())
