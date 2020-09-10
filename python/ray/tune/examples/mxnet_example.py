from functools import partial

import mxnet as mx
from ray import tune, logger
from ray.tune import CLIReporter
from ray.tune.integration.mxnet import TuneCheckpointCallback, \
    TuneReportCallback
from ray.tune.schedulers import ASHAScheduler


def train_mnist_mxnet(config, mnist, num_epochs=10):
    batch_size = config["batch_size"]
    train_iter = mx.io.NDArrayIter(
        mnist["train_data"], mnist["train_label"], batch_size, shuffle=True)
    val_iter = mx.io.NDArrayIter(mnist["test_data"], mnist["test_label"],
                                 batch_size)

    data = mx.sym.var("data")
    data = mx.sym.flatten(data=data)

    fc1 = mx.sym.FullyConnected(data=data, num_hidden=config["layer_1_size"])
    act1 = mx.sym.Activation(data=fc1, act_type="relu")

    fc2 = mx.sym.FullyConnected(data=act1, num_hidden=config["layer_2_size"])
    act2 = mx.sym.Activation(data=fc2, act_type="relu")

    # MNIST has 10 classes
    fc3 = mx.sym.FullyConnected(data=act2, num_hidden=10)
    # Softmax with cross entropy loss
    mlp = mx.sym.SoftmaxOutput(data=fc3, name="softmax")

    # create a trainable module on CPU
    mlp_model = mx.mod.Module(symbol=mlp, context=mx.cpu())
    mlp_model.fit(
        train_iter,
        eval_data=val_iter,
        optimizer="sgd",
        optimizer_params={"learning_rate": config["lr"]},
        eval_metric="acc",
        batch_end_callback=mx.callback.Speedometer(batch_size, 100),
        eval_end_callback=TuneReportCallback({
            "mean_accuracy": "accuracy"
        }),
        epoch_end_callback=TuneCheckpointCallback(
            filename="mxnet_cp", frequency=3),
        num_epoch=num_epochs)


def tune_mnist_mxnet(num_samples=10, num_epochs=10):
    logger.info("Downloading MNIST data...")
    mnist_data = mx.test_utils.get_mnist()
    logger.info("Got MNIST data, starting Ray Tune.")

    config = {
        "layer_1_size": tune.choice([32, 64, 128]),
        "layer_2_size": tune.choice([64, 128, 256]),
        "lr": tune.loguniform(1e-3, 1e-1),
        "batch_size": tune.choice([32, 64, 128])
    }

    scheduler = ASHAScheduler(
        metric="mean_accuracy",
        mode="max",
        max_t=num_epochs,
        grace_period=1,
        reduction_factor=2)

    reporter = CLIReporter(
        parameter_columns=["layer_1_size", "layer_2_size", "lr", "batch_size"])

    tune.run(
        partial(train_mnist_mxnet, mnist=mnist_data, num_epochs=num_epochs),
        resources_per_trial={
            "cpu": 1,
        },
        config=config,
        num_samples=num_samples,
        scheduler=scheduler,
        progress_reporter=reporter,
        name="tune_mnist_mxnet")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing")
    args, _ = parser.parse_known_args()

    if args.smoke_test:
        tune_mnist_mxnet(num_samples=1, num_epochs=1)
    else:
        tune_mnist_mxnet(num_samples=10, num_epochs=10)
