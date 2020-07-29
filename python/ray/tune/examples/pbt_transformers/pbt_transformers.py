# flake8: noqa
# yapf: disable

"""
Please note that this example requires Python >= 3.7 to run.
"""

# __import_begin__
import os
import ray
from ray.tune import CLIReporter
from ray.tune.schedulers import PopulationBasedTraining

from ray import tune
import trainer
from ray.tune.examples.pbt_transformers.utils import build_compute_metrics_fn, download_data

from transformers import AutoConfig, AutoModelForSequenceClassification, AutoTokenizer, EvalPrediction, GlueDataset
from transformers import GlueDataTrainingArguments as DataTrainingArguments
from transformers import (
    Trainer,
    TrainingArguments,
    glue_compute_metrics,
    glue_output_modes,
    glue_tasks_num_labels,
)
from transformers.trainer_utils import is_wandb_available


def get_trainer(model_name_or_path, train_dataset, eval_dataset, task_name, training_args, wandb_args=None):
    try:
        num_labels = glue_tasks_num_labels[task_name]
        output_mode = glue_output_modes[task_name]
    except KeyError:
        raise ValueError("Task not found: %s" % (task_name))

    config = AutoConfig.from_pretrained(
        model_name_or_path,
        num_labels=num_labels,
        finetuning_task=task_name,
    )

    model = AutoModelForSequenceClassification.from_pretrained(
        model_name_or_path,
        config=config,
    )
    tune_trainer = trainer.TuneTransformerTrainer(
        model=model,
        args=training_args,
        train_dataset=train_dataset,
        eval_dataset=eval_dataset,
        compute_metrics=build_compute_metrics_fn(task_name),
        wandb_args=wandb_args
    )

    return tune_trainer


# __train_begin__
def train_transformer(config, checkpoint=None):
    data_args = DataTrainingArguments(
        task_name=config["task_name"],
        data_dir=config["data_dir"]
    )
    tokenizer = AutoTokenizer.from_pretrained(config["model_name"])
    train_dataset = GlueDataset(data_args, tokenizer=tokenizer, mode="train", cache_dir=config["data_dir"])
    eval_dataset = GlueDataset(data_args, tokenizer=tokenizer, mode="dev", cache_dir=config["data_dir"])
    eval_dataset = eval_dataset[:len(eval_dataset) // 2]
    training_args = TrainingArguments(
        output_dir=tune.get_trial_dir(),
        learning_rate=config["learning_rate"],
        do_train=True,
        do_eval=True,
        evaluate_during_training=True,
        eval_steps=(len(train_dataset) // config["per_gpu_train_batch_size"]) + 1,
        save_steps=0,  # We explicitly set save here to 0, and do saving in evaluate instead
        num_train_epochs=config["num_epochs"],
        per_device_train_batch_size=config["per_gpu_train_batch_size"],
        per_device_eval_batch_size=config["per_gpu_val_batch_size"],
        warmup_steps=0,
        weight_decay=config["weight_decay"],
        logging_dir="./logs",
    )

    # Arguments for W&B.
    # name = f"{tune.get_trial_name()}-{os.path.basename(tune.get_trial_dir()[:-1])}"
    name = tune.get_trial_name()
    wandb_args = {
        "project_name": "transformers_pbt",
        "watch": "false",  # Either set to gradient, false, or all
        "run_name": name,
    }

    tune_trainer = get_trainer(config["model_name"], train_dataset, eval_dataset, config["task_name"], training_args,
                               wandb_args=wandb_args)
    tune_trainer.train(checkpoint if checkpoint is not None and len(checkpoint) > 0 else config["model_name"])


# __train_end__


# __tune_begin__
def tune_transformer(num_samples=8, gpus_per_trial=0, smoke_test=False):
    ray.init("auto", log_to_driver=False)
    data_dir = os.path.abspath(os.path.join(os.getcwd(), "./data"))
    if not os.path.exists(data_dir):
        os.mkdir(data_dir, 0o755)
    model_name = "bert-base-uncased"
    task_name = "rte"

    task_data_dir = os.path.join(data_dir, task_name.upper())

    # Download and cache tokenizer, model, and features
    print("Downloading and caching Tokenizer")

    # Triggers tokenizer download to cache
    AutoTokenizer.from_pretrained(model_name)
    print("Downloading and caching pre-trained model")

    # Triggers model download to cache
    AutoModelForSequenceClassification.from_pretrained(
        model_name,
    )

    # Download data.
    download_data(model_name, task_name, task_data_dir)

    config = {
        "model_name": model_name,
        "task_name": task_name,
        "data_dir": task_data_dir,
        "per_gpu_val_batch_size": 32,
        "per_gpu_train_batch_size": tune.choice([16, 32, 64]),
        "learning_rate": tune.uniform(1e-5, 5e-5),
        "weight_decay": tune.uniform(0.0, 0.3),
        "num_epochs": tune.choice([2, 3, 4, 5]) if not smoke_test else 1,
    }

    scheduler = PopulationBasedTraining(
        time_attr="training_iteration",
        metric="eval_acc",
        mode="max",
        perturbation_interval=1,
        hyperparam_mutations={
            "weight_decay": lambda: tune.uniform(0.0, 0.3).func(None),
            "learning_rate": lambda: tune.uniform(1e-5, 5e-5).func(None),
            "per_gpu_train_batch_size": [16, 32, 64],
        })

    reporter = CLIReporter(
        parameter_columns={
            "weight_decay": "w_decay",
            "learning_rate": "lr",
            "per_gpu_train_batch_size": "train_bs/gpu",
            "num_epochs": "num_epochs"},
        metric_columns=["eval_acc", "eval_loss", "epoch", "training_iteration"])

    analysis = tune.run(
        train_transformer,
        resources_per_trial={"cpu": 1, "gpu": gpus_per_trial},
        config=config,
        num_samples=num_samples,
        scheduler=scheduler,
        keep_checkpoints_num=3,
        checkpoint_score_attr="training_iteration",
        progress_reporter=reporter,
        local_dir="~/ray_results/5",
        name="tune_transformer_pbt")

    test_best_model(analysis, config["model_name"], config["task_name"], config["data_dir"])


# __tune_end__

def test_best_model(analysis, model_name, task_name, data_dir):
    data_args = DataTrainingArguments(
        task_name=task_name,
        data_dir=data_dir
    )

    tokenizer = AutoTokenizer.from_pretrained(model_name)

    best_config = analysis.get_best_config(metric="eval_acc", mode="max")
    print(best_config)
    best_checkpoint = analysis.get_best_trial(metric="eval_acc", mode="max").checkpoint.value
    print(best_checkpoint)
    best_model = AutoModelForSequenceClassification.from_pretrained(best_checkpoint).to("cuda")

    test_args = TrainingArguments(
        output_dir="./best_model_results",
    )
    test_dataset = GlueDataset(data_args, tokenizer=tokenizer, mode="dev", cache_dir=data_dir)
    test_dataset = test_dataset[len(test_dataset) // 2:]

    test_trainer = Trainer(best_model, test_args, compute_metrics=build_compute_metrics_fn(task_name))

    metrics = test_trainer.evaluate(test_dataset)
    print(metrics)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing")
    args, _ = parser.parse_known_args()

    if args.smoke_test:
        tune_transformer(num_samples=1, gpus_per_trial=0, smoke_test=True)
    else:
        # You can change the number of GPUs here:
        tune_transformer(num_samples=8, gpus_per_trial=1)
