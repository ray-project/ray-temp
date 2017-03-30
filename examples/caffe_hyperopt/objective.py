# Most of the tensorflow code is adapted from Tensorflow's tutorial on using
# CNNs to train MNIST
# https://www.tensorflow.org/versions/r0.9/tutorials/mnist/pros/index.html#build-a-multilayer-convolutional-network.

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
import numpy as np
import os
from caffe import layers as L, params as P
import caffe

import tempfile

def lenet(lmdb, batch_size):
    # our version of LeNet: a series of linear and simple nonlinear transformations
    n = caffe.NetSpec()
    n.data, n.label = L.Data(batch_size=batch_size, backend=P.Data.LMDB, source=lmdb,
                             transform_param=dict(scale=1./255), ntop=2)
    
    n.conv1 = L.Convolution(n.data, kernel_size=5, num_output=20, weight_filler=dict(type='xavier'))
    n.pool1 = L.Pooling(n.conv1, kernel_size=2, stride=2, pool=P.Pooling.MAX)
    n.conv2 = L.Convolution(n.pool1, kernel_size=5, num_output=50, weight_filler=dict(type='xavier'))
    n.pool2 = L.Pooling(n.conv2, kernel_size=2, stride=2, pool=P.Pooling.MAX)
    n.fc1 =   L.InnerProduct(n.pool2, num_output=500, weight_filler=dict(type='xavier'))
    n.relu1 = L.ReLU(n.fc1, in_place=True)
    n.score = L.InnerProduct(n.relu1, num_output=10, weight_filler=dict(type='xavier'))
    n.loss =  L.SoftmaxWithLoss(n.score, n.label)
    
    return n.to_proto()
    
# Define a remote function that takes a set of hyperparameters as well as the
# data, consructs and trains a network, and returns the validation accuracy.
@ray.remote
def train_cnn_and_compute_accuracy(params, steps, weights=None):
  # Extract the hyperparameters from the params dictionary.
  learning_rate = params["learning_rate"]
  batch_size = params["batch_size"]
  # Create the network and related variables.
  with tempfile.NamedTemporaryFile(mode="w+", delete=False) as f:
    f.write(str(lenet('mnist_train_lmdb', batch_size)))
    train = f.name
  with tempfile.NamedTemporaryFile(mode="w+", delete=False) as f:
    f.write(str(lenet('mnist_test_lmdb', 100)))
    test = f.name
  solver_config = [('train_net', "\"" + train + "\""),
		   ('test_net', "\"" + test + "\""), 
		   ('test_iter', 100), 
		   ('test_interval', 500),
		   ('base_lr', learning_rate),
		   ('momentum', 0.9),
		   ('weight_decay', 0.0005),
		   ('lr_policy', "\"inv\""),
		   ('gamma', 0.0001), 
		   ('power', 0.75),
                   ('display', 100),
		   ('max_iter', 10000)]

  with tempfile.NamedTemporaryFile(mode="w+", delete=False) as f:
    f.write("\n".join([param[0] + ": " + str(param[1]) for param in solver_config]))
    solver_name = f.name
  solver = None
  solver = caffe.SGDSolver(solver_name)
  niter = steps
  test_interval = 25
  # losses will also be stored in the log
  train_loss = np.zeros(niter)
  test_acc = np.zeros(int(np.ceil(niter / test_interval)))
  output = np.zeros((niter, 8, 10))

  # the main solver loop
  solver.step(niter)  # SGD by Caffe
      
  correct = 0
  for test_it in range(100):
      solver.test_nets[0].forward()
      correct += sum(solver.test_nets[0].blobs['score'].data.argmax(1) == solver.test_nets[0].blobs['label'].data)
  return correct / 1e4, 0
