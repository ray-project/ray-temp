from gym.spaces import Box
import numpy as np

from ray.rllib.examples.policy.random_policy import RandomPolicy
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.view_requirement import ViewRequirement
from ray.rllib.utils.annotations import override


class EpisodeEnvAwarePolicy(RandomPolicy):
    """A Policy that always knows the current EpisodeID and EnvID and
    returns these in its actions."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.state_space = Box(-1.0, 1.0, (1, ))

        class _fake_model:
            pass

        self.model = _fake_model()
        self.model.time_major = True
        self.model.inference_view_requirements = {
            SampleBatch.AGENT_INDEX: ViewRequirement(),
            SampleBatch.EPS_ID: ViewRequirement(),
            "env_id": ViewRequirement(),
            "t": ViewRequirement(),
            SampleBatch.OBS: ViewRequirement(),
            SampleBatch.PREV_ACTIONS: ViewRequirement(
                SampleBatch.ACTIONS, space=self.action_space, shift=-1),
            SampleBatch.PREV_REWARDS: ViewRequirement(
                SampleBatch.REWARDS, shift=-1),
        }
        for i in range(2):
            self.model.inference_view_requirements["state_in_{}".format(i)] = \
                ViewRequirement(
                    "state_out_{}".format(i), shift=-1, space=self.state_space)
            self.model.inference_view_requirements[
                "state_out_{}".format(i)] = \
                ViewRequirement(space=self.state_space)

        self.view_requirements = dict(
            **{
                SampleBatch.NEXT_OBS: ViewRequirement(
                    SampleBatch.OBS, shift=1),
                SampleBatch.ACTIONS: ViewRequirement(space=self.action_space),
                SampleBatch.REWARDS: ViewRequirement(),
                SampleBatch.DONES: ViewRequirement(),
            },
            **self.model.inference_view_requirements)

    @override(Policy)
    def is_recurrent(self):
        return True

    @override(Policy)
    def compute_actions_from_input_dict(self,
                                        input_dict,
                                        explore=None,
                                        timestep=None,
                                        **kwargs):
        ts = input_dict["t"]
        print(ts)
        # Always return [episodeID, envID] as actions.
        actions = np.array([[
            input_dict[SampleBatch.AGENT_INDEX][i],
            input_dict[SampleBatch.EPS_ID][i], input_dict["env_id"][i]
        ] for i, _ in enumerate(input_dict["obs"])])
        states = [
            np.array([[ts[i]] for i in range(len(input_dict["obs"]))])
            for _ in range(2)
        ]
        return actions, states, {}

    @override(Policy)
    def postprocess_trajectory(self,
                               sample_batch,
                               other_agent_batches=None,
                               episode=None):
        sample_batch["postprocessed_column"] = sample_batch["obs"] * 2.0
        return sample_batch
