# Copyright 2016 The TensorFlow Authors. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================

"""ResNet Train/Eval module.
"""
import time
import sys

import cifar_input
import numpy as np
import resnet_model
import tensorflow as tf
import IPython
import ray
import uuid

FLAGS = tf.app.flags.FLAGS
tf.app.flags.DEFINE_string('dataset', 'cifar10', 'cifar10 or cifar100.')
tf.app.flags.DEFINE_string('mode', 'train', 'train or eval.')
tf.app.flags.DEFINE_string('train_data_path', '',
                           'Filepattern for training data.')
tf.app.flags.DEFINE_string('eval_data_path', '',
                           'Filepattern for eval data')
tf.app.flags.DEFINE_integer('image_size', 32, 'Image side length.')
tf.app.flags.DEFINE_string('train_dir', '',
                           'Directory to keep training outputs.')
tf.app.flags.DEFINE_string('eval_dir', '',
                           'Directory to keep eval outputs.')
tf.app.flags.DEFINE_integer('eval_batch_count', 50,
                            'Number of batches to eval.')
tf.app.flags.DEFINE_bool('eval_once', False,
                         'Whether evaluate the model only once.')
tf.app.flags.DEFINE_string('log_root', '',
                           'Directory to keep the checkpoints. Should be a '
                           'parent directory of FLAGS.train_dir/eval_dir.')
tf.app.flags.DEFINE_integer('num_gpus', 0,
                            'Number of gpus used for training. (0 or 1)')


@ray.remote
def get_test(dataset, path, size, mode):
  images, labels = cifar_input.build_input(dataset, path, size, mode)
  sess = tf.Session()
  coord = tf.train.Coordinator()
  tf.train.start_queue_runners(sess, coord=coord)
  batches = [sess.run([images, labels]) for _ in range(5)]
  coord.request_stop()
  return (np.concatenate([batches[i][0] for i in range(5)]), np.concatenate([batches[i][1] for i in range(5)]))

@ray.remote(num_return_vals=25)
def get_batches(dataset, path, size, mode):
  images, labels = cifar_input.build_input(dataset, path, size, mode)
  sess = tf.Session()
  coord = tf.train.Coordinator()
  tf.train.start_queue_runners(sess, coord=coord)
  batches = [sess.run([images, labels]) for _ in range(25)]
  coord.request_stop()
  return batches

@ray.remote
def compute_rollout(weights, batch):
  model, _ = ray.env.model
  rollouts = 10
  model.variables.set_weights(weights)
  placeholders = [model.x, model.labels]

  for i in range(rollouts):
    randlist = np.random.randint(0,batch[0].shape[0], 128)
    subset = (batch[0][randlist, :], batch[1][randlist, :])
    model.variables.sess.run(model.train_op, feed_dict=dict(zip(placeholders, subset))) 
  return model.variables.get_weights()  

@ray.remote
def accuracy(weights, batch):
  model, _ = ray.env.model
  model.variables.set_weights(weights)
  placeholders = [model.x, model.labels]
  batches = [(batch[0][128*i:128*(i+1)], batch[1][128*i:128*(i+1)]) for i in range(78)]
  return sum([model.variables.sess.run(model.precision, feed_dict=dict(zip(placeholders, batches[i]))) for i in range(78)]) / 78

def model_initialization():
  with tf.Graph().as_default():
    model = resnet_model.ResNet(hps, 'train')
    model.build_graph()
    sess = tf.Session()
    model.variables.set_session(sess)
    init = tf.global_variables_initializer()
    return model, init

def model_reinitialization(model):
  return model

def train(hps):
  """Training loop."""
  ray.init(num_workers=10)
  batches = get_batches.remote(
      FLAGS.dataset, FLAGS.train_data_path, hps.batch_size, FLAGS.mode)
  test_batch = get_test.remote(FLAGS.dataset, FLAGS.eval_data_path, hps.batch_size, FLAGS.mode)
  ray.env.model = ray.EnvironmentVariable(model_initialization, model_reinitialization)
  model, init = ray.env.model
  param_stats = tf.contrib.tfprof.model_analyzer.print_model_analysis(
      tf.get_default_graph(),
      tfprof_options=tf.contrib.tfprof.model_analyzer.
          TRAINABLE_VARS_PARAMS_STAT_OPTIONS)
  sys.stdout.write('total_params: %d\n' % param_stats.total_parameters)

  tf.contrib.tfprof.model_analyzer.print_model_analysis(
      tf.get_default_graph(),
      tfprof_options=tf.contrib.tfprof.model_analyzer.FLOAT_OPS_OPTIONS)



  summary_hook = tf.train.SummarySaverHook(
      save_steps=100,
      output_dir=FLAGS.train_dir,
      summary_op=tf.summary.merge([model.summaries,
                  tf.summary.scalar('Precision', model.precision)]))

  logging_hook = tf.train.LoggingTensorHook(
      tensors={'step': model.global_step,
               'loss': model.cost,
               'precision': model.precision},
      every_n_iter=100)
  class _LearningRateSetterHook(tf.train.SessionRunHook):
    """Sets learning_rate based on global step."""

    def begin(self):
      self._lrn_rate = 0.1

    def before_run(self, run_context):
      return tf.train.SessionRunArgs(
          model.global_step,  # Asks for global step value.
          feed_dict={model.lrn_rate: self._lrn_rate})  # Sets learning rate

    def after_run(self, run_context, run_values):
      train_step = run_values.results
      if train_step < 40000:
        self._lrn_rate = 0.1
      elif train_step < 60000:
        self._lrn_rate = 0.01
      elif train_step < 80000:
        self._lrn_rate = 0.001
      else:
        self._lrn_rate = 0.0001
  '''with tf.train.MonitoredTrainingSession(
      checkpoint_dir=FLAGS.log_root,
      hooks=[logging_hook, _LearningRateSetterHook()],
      chief_only_hooks=[summary_hook],
      # Since we provide a SummarySaverHook, we need to disable default
      # SummarySaverHook. To do that we set save_summaries_steps to 0.
      save_summaries_steps=0,
      config=tf.ConfigProto(allow_soft_placement=True)) as mon_sess:
    model.variables.set_session(mon_sess)
    mon_sess.run(init)
    while not mon_sess.should_stop():'''
  model.variables.sess.run(init)
  step = 0
  with open("results.txt", "w") as results:
    while True:
      print "Start of loop"
      weights = model.variables.get_weights()
      weight_id = ray.put(weights)
      rand_list = np.random.choice(25, 10, replace=False)
      print "Computing rollouts"
      all_weights = ray.get([compute_rollout.remote(weight_id, batches[i])  for i in rand_list])
      mean_weights = {k: sum([weights[k] for weights in all_weights]) / 10 for k in all_weights[0]}
      model.variables.set_weights(mean_weights)
      new_weights = ray.put(mean_weights)
      if step % 200 == 0:
        results.write(str(step) + " " + str(ray.get(accuracy.remote(new_weights, test_batch))) + "\n")
      step += 1

def evaluate(hps):
  """Eval loop."""
  images, labels = cifar_input.build_input(
      FLAGS.dataset, FLAGS.eval_data_path, hps.batch_size, FLAGS.mode)
  model = resnet_model.ResNet(hps, images, labels, FLAGS.mode)
  model.build_graph()
  saver = tf.train.Saver()
  summary_writer = tf.summary.FileWriter(FLAGS.eval_dir)

  sess = tf.Session(config=tf.ConfigProto(allow_soft_placement=True))
  tf.train.start_queue_runners(sess)

  best_precision = 0.0
  while True:
    time.sleep(60)
    try:
      ckpt_state = tf.train.get_checkpoint_state(FLAGS.log_root)
    except tf.errors.OutOfRangeError as e:
      tf.logging.error('Cannot restore checkpoint: %s', e)
      continue
    if not (ckpt_state and ckpt_state.model_checkpoint_path):
      tf.logging.info('No model to eval yet at %s', FLAGS.log_root)
      continue
    tf.logging.info('Loading checkpoint %s', ckpt_state.model_checkpoint_path)
    saver.restore(sess, ckpt_state.model_checkpoint_path)

    total_prediction, correct_prediction = 0, 0
    for _ in xrange(FLAGS.eval_batch_count):
      (summaries, loss, predictions, truth, train_step) = sess.run(
          [model.summaries, model.cost, model.predictions,
           model.labels, model.global_step])

      truth = np.argmax(truth, axis=1)
      predictions = np.argmax(predictions, axis=1)
      correct_prediction += np.sum(truth == predictions)
      total_prediction += predictions.shape[0]

    precision = 1.0 * correct_prediction / total_prediction
    best_precision = max(precision, best_precision)

    precision_summ = tf.Summary()
    precision_summ.value.add(
        tag='Precision', simple_value=precision)
    summary_writer.add_summary(precision_summ, train_step)
    best_precision_summ = tf.Summary()
    best_precision_summ.value.add(
        tag='Best Precision', simple_value=best_precision)
    summary_writer.add_summary(best_precision_summ, train_step)
    summary_writer.add_summary(summaries, train_step)
    tf.logging.info('loss: %.3f, precision: %.3f, best precision: %.3f\n' %
                    (loss, precision, best_precision))
    summary_writer.flush()

    if FLAGS.eval_once:
      break


def main(_):
  if FLAGS.num_gpus == 0:
    dev = '/cpu:0'
  elif FLAGS.num_gpus == 1:
    dev = '/gpu:0'
  else:
    raise ValueError('Only support 0 or 1 gpu.')

  if FLAGS.mode == 'train':
    batch_size = 128
  elif FLAGS.mode == 'eval':
    batch_size = 100

  if FLAGS.dataset == 'cifar10':
    num_classes = 10
  elif FLAGS.dataset == 'cifar100':
    num_classes = 100
  global hps
  hps = resnet_model.HParams(batch_size=batch_size,
                             num_classes=num_classes,
                             min_lrn_rate=0.0001,
                             lrn_rate=0.1,
                             num_residual_units=5,
                             use_bottleneck=False,
                             weight_decay_rate=0.0002,
                             relu_leakiness=0.1,
                             optimizer='mom')
  with tf.device(dev):
    if FLAGS.mode == 'train':
      train(hps)
    elif FLAGS.mode == 'eval':
      evaluate(hps)


if __name__ == '__main__':
  tf.app.run()
