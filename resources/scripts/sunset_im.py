""" SUNSET Model

Reimplementation of the SUNSET model as a [pytorch-lightning](https://github.com/PyTorchLightning/pytorch-lightning) module.

Takes [webdataset](https://github.com/tmbdev/webdataset) formatted tar files as input.

python3 sunset.py --ds_dir /data/blackmountain/shards_1m_7m_15m --train_ds "train_{0000..0036}.tar" --val_ds "val_{0000..0003}.tar" --test_ds "test_{0000..0014}.tar" --batch_size 8  --gpus 1
python3 sunset.py --ds_dir /data/blackmountain/shards_1m_7m_15m --train_ds "train_{0000..0036}.tar" --val_ds "val_{0000..0003}.tar" --test_ds "test_{0000..0014}.tar" --batch_size 8  --gpus 1 --test --load_checkpoint tb_logs/my_sunset_model/version_0/checkpoints/epoch=3-step=510222.ckpt

python3 sunset.py --model_name sunset --train_ds "/data/blackmountain/shards_2_20s/train_{0000..0036}.tar" --val_ds "/data/blackmountain/shards_2_20s/val_{0000..0003}.tar" --test_ds "/data/blackmountain/shards_2_20s/test_{0000..0014}.tar" --batch_size 8 --test_output "2_20s_out.csv.gz" --input_terms 2 --gpus 1

tensorboard --logdir lightning_logs/
"""


# Imports
from argparse import ArgumentParser
from pathlib import Path

import torch
from torch import nn
import pytorch_lightning as pl
import numpy as np
import pandas as pd

import einops
import matplotlib.pyplot as plt

from solpreddatamodule import SolpredDataModule
import solpred_common

class Sunset(solpred_common.SolpredModule):
    def __init__(self, args):
        super().__init__(args)

        self.conv_block1 = nn.Sequential(
            nn.Conv2d((3 * args.input_terms), 24, 3),
            nn.ReLU(),
            nn.BatchNorm2d(24),
            nn.MaxPool2d(2)
        )
        self.conv_block2 = nn.Sequential(
            nn.Conv2d(24, 48, 3),
            nn.ReLU(),
            nn.BatchNorm2d(48),
            nn.MaxPool2d(2)
        )
        self.flatten = nn.Flatten()
        self.linear_block = nn.Sequential(
            nn.Linear((9408), 1024),
            nn.ReLU(),
            nn.Dropout(0.4),
            nn.Linear(1024, 1024),
            nn.ReLU(),
            nn.Dropout(0.4),
            nn.Linear(1024, 1)
        )

    def forward(self, img, in_data, most_recent_irradiance, most_recent_clear_sky, target_clear_sky):
        conv1 = self.conv_block1(img)
        conv2 = self.conv_block2(conv1)
        conv2_flat = self.flatten(conv2)
        regression = self.linear_block(conv2_flat)
        return regression

    def configure_optimizers(self):
        return torch.optim.Adam(self.parameters(), lr=self.learning_rate)


def main(args):
    model = Sunset.load_from_checkpoint(args.load_checkpoint) if args.load_checkpoint else Sunset(args)
    solpred_common.main(args, model)


if __name__ == '__main__':
    parser = solpred_common.make_parser()
    # add model specific args
    parser = Sunset.add_model_specific_args(parser)
    parser = SolpredDataModule.add_data_specific_args(parser)
    parser = pl.Trainer.add_argparse_args(parser)
    main(parser.parse_args())
