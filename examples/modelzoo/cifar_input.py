
"""CIFAR dataset input module, with the majority taken from 
   tensorflow/models/resnet.
"""

import tensorflow as tf
import IPython
import numpy as np

def build_data(data_path, size):
  image_size = 32
  label_bytes = 1
  label_offset = 0
  num_classes = 10

  depth = 3
  image_bytes = image_size * image_size * depth
  record_bytes = label_bytes + label_offset + image_bytes

  data_files = tf.gfile.Glob(data_path)
  file_queue = tf.train.string_input_producer(data_files, shuffle=True)
  # Read examples from files in the filename queue.
  reader = tf.FixedLengthRecordReader(record_bytes=record_bytes)
  _, value = reader.read(file_queue)

  # Convert these examples to dense labels and processed images.
  record = tf.reshape(tf.decode_raw(value, tf.uint8), [record_bytes])
  label = tf.cast(tf.slice(record, [label_offset], [label_bytes]), tf.int32)
  # Convert from string to [depth * height * width] to [depth, height, width].
  depth_major = tf.reshape(tf.slice(record, [label_bytes], [image_bytes]),
                           [depth, image_size, image_size])
  # Convert from [depth, height, width] to [height, width, depth].
  image = tf.cast(tf.transpose(depth_major, [1, 2, 0]), tf.float32)
  queue = tf.train.shuffle_batch([image, label], size, size, 0, num_threads=16)
  return queue

def build_input(data, batch_size, train):
  """Build CIFAR image and labels.

  Args:
    data_path: Filename for cifar10 data.
    batch_size: Input batch size.
  Returns:
    images: Batches of images. [batch_size, image_size, image_size, 3]
    labels: Batches of labels. [batch_size, num_classes]
  Raises:
    ValueError: when the specified dataset is not supported.
  """
  images_constant = tf.constant(data[0])
  labels_constant = tf.constant(data[1])
  image_size = 32
  depth = 3
  num_classes = 10
  image, label = tf.train.slice_input_producer([images_constant, labels_constant])
  if train:
    image = tf.image.resize_image_with_crop_or_pad(
	image, image_size+4, image_size+4)
    image = tf.random_crop(image, [image_size, image_size, 3])
    image = tf.image.random_flip_left_right(image)
    # Brightness/saturation/constrast provides small gains .2%~.5% on cifar.
    # image = tf.image.random_brightness(image, max_delta=63. / 255.)
    # image = tf.image.random_saturation(image, lower=0.5, upper=1.5)
    # image = tf.image.random_contrast(image, lower=0.2, upper=1.8)
    image = tf.image.per_image_standardization(image)
    example_queue = tf.RandomShuffleQueue(
      capacity=16 * batch_size,
      min_after_dequeue=8 * batch_size,
      dtypes=[tf.float32, tf.int32],
      shapes=[[image_size, image_size, depth], [1]])
    num_threads = 16
  else:
    image = tf.image.resize_image_with_crop_or_pad(
	image, image_size, image_size)
    image = tf.image.per_image_standardization(image)
    example_queue = tf.FIFOQueue(
        3 * batch_size,
        dtypes=[tf.float32, tf.int32],
        shapes=[[image_size, image_size, depth], [1]])
    num_threads = 1


  example_enqueue_op = example_queue.enqueue([image, label])
  tf.train.add_queue_runner(tf.train.queue_runner.QueueRunner(
      example_queue, [example_enqueue_op] * num_threads))

  # Read 'batch' labels + images from the example queue.
  images, labels = example_queue.dequeue_many(batch_size)
  labels = tf.reshape(labels, [batch_size, 1])
  indices = tf.reshape(tf.range(0, batch_size, 1), [batch_size, 1])
  labels = tf.sparse_to_dense(
      tf.concat([indices, labels], 1),
      [batch_size, num_classes], 1.0, 0.0)

  assert len(images.get_shape()) == 4
  assert images.get_shape()[0] == batch_size
  assert images.get_shape()[-1] == 3
  assert len(labels.get_shape()) == 2
  assert labels.get_shape()[0] == batch_size
  assert labels.get_shape()[1] == num_classes

  return images, labels
