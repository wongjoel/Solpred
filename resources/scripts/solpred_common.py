"""
Common code for running pytorch lightning modules in the solpred project
"""


# Imports
from pathlib import Path
from argparse import ArgumentParser
import datetime
import json

import torch.nn.functional as F
import pytorch_lightning as pl
import pandas as pd

from solpreddatamodule import SolpredDataModule

class SolpredModule(pl.LightningModule):
    @staticmethod
    def add_model_specific_args(parent_parser):
        parser = ArgumentParser(parents=[parent_parser], add_help=False)
        parser.add_argument('--input_terms', type=int, required=True)
        parser.add_argument('--model_name', type=str, required=True)
        parser.add_argument('--learning_rate', type=float, required=True)
        return parser

    def __init__(self, args):
        super().__init__()
        self.model_name = args.model_name
        self.batch_size = args.batch_size
        self.learning_rate = args.learning_rate
        self.test_results = []
        self.save_hyperparameters()

    def training_step(self, batch, batch_idx):
        img, in_data, target, diffuse_direct_irradiance, most_recent_clear_sky, target_clear_sky, json_data = batch
        pred = self(img, in_data, diffuse_direct_irradiance, most_recent_clear_sky, target_clear_sky)
        loss = F.mse_loss(pred, target)
        return loss

    def validation_step(self, batch, batch_idx):
        img, in_data, target, diffuse_direct_irradiance, most_recent_clear_sky, target_clear_sky, json_data = batch
        pred = self(img, in_data, diffuse_direct_irradiance, most_recent_clear_sky, target_clear_sky)
        loss = F.mse_loss(pred, target)
        self.log("val_loss", loss, batch_size=self.batch_size)
        return loss

    def test_step(self, batch, batch_idx):
        img, in_data, target, diffuse_direct_irradiance, most_recent_clear_sky, target_clear_sky, json_data = batch
        pred = self(img, in_data, diffuse_direct_irradiance, most_recent_clear_sky, target_clear_sky)
        loss = F.mse_loss(pred, target)
        self.log("test_loss", loss, batch_size=1)
        ghi_label = "globalcmp11physical" if "globalcmp11physical" in json_data[0]["inputs"][0] else "value"
        for i, d in enumerate(json_data):
            # Looping through each element in the batch
            time = d["id"] # Time is defined by id - typically when the prediction was made
            result = {"actual": target[i].item(), # The value we were trying to predict
                     self.model_name + "_pred": pred[i].item(), # the prediction from our model
                     "persist_pred": json_data[i]["inputs"][0][ghi_label]} # Taking the most recent observed value as persistence
            for key, value in result.items():
                # For each series in result
                self.test_results.append(
                    {"time": time,
                    "series": key,
                    "value": value})
        #step_df = pd.DataFrame(step_results, columns=["time", "series", "value"])
        #step_df.set_index("time") # I think we throw away the index at csv export anyway
        return loss

    def export_test_csv(self, output_path):
        pd.DataFrame(self.test_results, columns=["time", "series", "value"]).to_csv(output_path, index=False)
        #pd.concat(self.test_results, axis="index", ignore_index=True).to_csv(output_path, index=False)


def make_callbacks(args):
    x = [
        pl.callbacks.EarlyStopping(
            monitor="val_loss",
            patience=args.stopping_patience,
            min_delta=0.0,
            strict=True,
            verbose=True,
            mode="min",
        ),
        pl.callbacks.ModelCheckpoint(
            save_top_k=1,
            verbose=True,
            monitor="val_loss",
            filename=args.model_name + "_{epoch:02d}_{val_loss:.2f}",
            mode="min",
        ),
        pl.callbacks.ModelCheckpoint(
            save_top_k=1,
            monitor="val_loss",
            mode="min",
            dirpath="./",
            filename="latest_best",
        ),
    ]
    if args.use_stochastic_weight_averaging:
        x = x.append(pl.callbacks.StochasticWeightAveraging(swa_epoch_start=2))
    return x

def test_phase(model, trainer, data, test_output):
    with open("started_testing.json", "w") as f:
        print("Starting Testing")
        json.dump({"start_time": str(datetime.datetime.now())}, f)
    trainer.test(model, datamodule=data)
    print("exporting")
    model.export_test_csv(test_output)

def main(args, model):
    data = SolpredDataModule(args)
    if args.visualise:
        print("Visualising")
        data.setup()
        model.visualise_activations(data.test_dataloader())
    else:
        trainer = pl.Trainer.from_argparse_args(args, callbacks=make_callbacks(args))
        # Run Train
        if not args.test:
            if args.load_checkpoint and args.resume_train:
                print("Loaded checkpoint weights and training state")
                trainer.fit(model, data, ckpt_path=args.load_checkpoint)
            elif args.load_checkpoint:
                print("Loaded checkpoint weights but not training state")
                trainer.fit(model, data)
            else:
                print("No previous checkpoint data was loaded")
                trainer.fit(model, data)
        else:
            print("Skipping Training")
        # Run Test
        test_phase(model, trainer, data, args.test_output)
    print("Main Done")


def make_parser():
    parser = ArgumentParser()
    # add PROGRAM level args
    parser.add_argument('--load_checkpoint', type=Path, help="Load checkpoint from path")
    parser.add_argument('--resume_train', action='store_true', help="Restore training status from checkpoint")
    parser.add_argument('--visualise', action='store_true', help="Only run visualise_activations, no training")
    parser.add_argument('--test', action='store_true', help="Only run inference, no training")
    parser.add_argument('--test_output', type=str, default='test_out.csv.gz')
    parser.add_argument('--stopping_patience', type=int, default=10, help="Early Stopping Patience")
    parser.add_argument('--use_stochastic_weight_averaging', action='store_true', help="Run with SWA turned on")
    return parser

if __name__ == '__main__':
    print("This is only common code, this module is meant to be imported")
