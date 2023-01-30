""" Fully Conv Model

A Fully convolutional [pytorch-lightning](https://github.com/PyTorchLightning/pytorch-lightning) module.

Takes [webdataset](https://github.com/tmbdev/webdataset) formatted tar files as input. Expects image data 64x64 along with previous timeseries datapoints.

python3 fully_conv.py --ds_dir /data/blackmountain/shards_1m_7m_15m --train_ds "train_{0000..0036}.tar" --val_ds "val_{0000..0003}.tar" --test_ds "test_{0000..0014}.tar" --batch_size 8  --gpus 1
python3 fully_conv.py --ds_dir /data/blackmountain/shards_1m_7m_15m --train_ds "train_{0000..0036}.tar" --val_ds "val_{0000..0003}.tar" --test_ds "test_{0000..0014}.tar" --batch_size 8  --gpus 1 --test --load_checkpoint tb_logs/my_sunset_model/version_0/checkpoints/epoch=3-step=510222.ckpt

python3 fully_conv.py --model_name fully-conv --train_ds "/work/blackmountain/shards_2x20s_420s/fold1/train_{0000..0036}.tar" --val_ds "/work/blackmountain/shards_2x20s_420s/fold1/val_{0000..0003}.tar" --test_ds "/work/blackmountain/shards_2x20s_420s/test_{0000..0014}.tar" --batch_size 8 --test_output "2_20s_out.csv.gz" --input_terms 2 --gpus 1 --benchmark True

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

class FullyConv(solpred_common.SolpredModule):
    def __init__(self, args):
        super().__init__(args)

        self.conv_block1 = nn.Sequential(
            nn.Conv2d((3 * args.input_terms), 3, 3),
            nn.LeakyReLU(),
            nn.Conv2d(3, 3, 3),
            nn.LeakyReLU(),
            nn.Conv2d(3, 3, 3),
            nn.LeakyReLU(),
            nn.Conv2d(3, 3, 3),
            nn.LeakyReLU(),
            nn.Conv2d(3, 3, 3),
            nn.LeakyReLU(),
            nn.Conv2d(3, 3, 3),
            nn.LeakyReLU(),
            nn.Conv2d(3, 3, 3),
            nn.LeakyReLU(),
        )
        self.flatten = nn.Flatten()
        self.linear_block = nn.Sequential(
            nn.Linear((7500 + args.input_terms), 1024),
            nn.LeakyReLU(),
            nn.Dropout(0.4),
            nn.Linear(1024, 1024),
            nn.LeakyReLU(),
            nn.Dropout(0.4),
            nn.Linear(1024, 1)
        )

    def forward(self, img, in_data, diffuse_direct_irradiance, most_recent_clear_sky, target_clear_sky):
        conv1 = self.conv_block1(img)
        conv1_flat = self.flatten(conv1)
        conv1_aug = torch.cat((conv1_flat, in_data), dim=1)
        regression = self.linear_block(conv1_aug)
        return regression

    def configure_optimizers(self):
        return torch.optim.Adam(self.parameters(), lr=self.learning_rate)


def main(args):
    model = FullyConv.load_from_checkpoint(args.load_checkpoint) if args.load_checkpoint else FullyConv(args)
    solpred_common.main(args, model)


if __name__ == '__main__':
    parser = solpred_common.make_parser()
    # add model specific args
    parser = FullyConv.add_model_specific_args(parser)
    parser = SolpredDataModule.add_data_specific_args(parser)
    parser = pl.Trainer.add_argparse_args(parser)
    main(parser.parse_args())

