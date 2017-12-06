#!/bin/sh

GYM_ENV='CartPole-v0'

# TODO: Test AC3

# Test for DQN
ALG='DQN'
EXPERIMENT_NAME=$GYM_ENV'_'$ALG
python ~/workspace/ray/python/ray/rllib/train.py --run $ALG --env $GYM_ENV \
  --stop '{"training_iteration": 2}' --experiment-name $EXPERIMENT_NAME \
  --checkpoint-freq 1

EXPERIMENT_PATH='/tmp/ray/'$EXPERIMENT_NAME
CHECKPOINT_FOLDER=$(ls $EXPERIMENT_PATH)
CHECKPOINT=$EXPERIMENT_PATH'/'$CHECKPOINT_FOLDER'/checkpoint-0'

python ~/workspace/ray/python/ray/rllib/eval.py $CHECKPOINT --run $ALG \
  --env $GYM_ENV --hide

# Clean up
rm -rf $EXPERIMENT_PATH

# Test for PPO
ALG='PPO'
EXPERIMENT_NAME=$GYM_ENV'_'$ALG
python ~/workspace/ray/python/ray/rllib/train.py --run $ALG --env $GYM_ENV \
  --stop '{"training_iteration": 2}' --experiment-name $EXPERIMENT_NAME \
  --checkpoint-freq 1

EXPERIMENT_PATH='/tmp/ray/'$EXPERIMENT_NAME
CHECKPOINT_FOLDER=$(ls $EXPERIMENT_PATH)
CHECKPOINT=$EXPERIMENT_PATH'/'$CHECKPOINT_FOLDER'/checkpoint-1'

echo $CHECKPOINT
python ~/workspace/ray/python/ray/rllib/eval.py $CHECKPOINT --run $ALG \
  --env $GYM_ENV --hide

# Clean up
rm -rf $EXPERIMENT_PATH

