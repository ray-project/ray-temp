# Advantage Actor-Critic (A2C, A3C)

## Overview 

[Advantage Actor-Critic](https://arxiv.org/pdf/1602.01783.pdf) proposes two distributed model-free off-policy RL algorithms, A3C and A2C. These algorithms are distributed versions of DQN with different distributed execution patterns. The paper accelerates training via scaling data collection, i.e. introducing worker nodes, which carry copies of the central node's Q-network, that collect data from the environment in parallel. 

In A2C, the worker nodes synchronously collect data. The collected data forms a giant batch of data, from which the central node (the central policy) computes gradient updates. On the other hand, in A3C, the worker nodes generate data asynchronously, compute gradients from the data, and send computed gradients to the central node. Note that the workers in A3C may be slightly out-of-sync with the central node due to asynchrony, which may induce biases in learning.


## Documentation & Implementation:

1) A2C. 

    **[Detailed Documentation](https://docs.ray.io/en/master/rllib-algorithms.html#a3c)**

    **[Implementation](https://github.com/ray-project/ray/blob/master/rllib/agents/a3c/a2c.py)**

2) A3C.

    **[Detailed Documentation](https://docs.ray.io/en/master/rllib-algorithms.html#a3c)**

    **[Implementation](https://github.com/ray-project/ray/blob/master/rllib/agents/a3c/a3c.py)**