""" Fully Conv Model

A Fully convolutional [pytorch-lightning](https://github.com/PyTorchLightning/pytorch-lightning) module.

Takes [webdataset](https://github.com/tmbdev/webdataset) formatted tar files as input. Expects image data 64x64 along with previous timeseries datapoints.

python3 fully_conv.py --ds_dir /data/blackmountain/shards_1m_7m_15m --train_ds "train_{0000..0036}.tar" --val_ds "val_{0000..0003}.tar" --test_ds "test_{0000..0014}.tar" --batch_size 8  --gpus 1
python3 fully_conv.py --ds_dir /data/blackmountain/shards_1m_7m_15m --train_ds "train_{0000..0036}.tar" --val_ds "val_{0000..0003}.tar" --test_ds "test_{0000..0014}.tar" --batch_size 8  --gpus 1 --test --load_checkpoint tb_logs/my_sunset_model/version_0/checkpoints/epoch=3-step=510222.ckpt

python3 fully_conv.py --model_name fully-conv --train_ds "/work/blackmountain/shards_2_20s/train_{0000..0036}.tar" --val_ds "/work/blackmountain/shards_2_20s/val_{0000..0003}.tar" --test_ds "/work/blackmountain/shards_2_20s/test_{0000..0014}.tar" --batch_size 8 --test_output "2_20s_out.csv.gz" --input_terms 2 --gpus 1 --benchmark True

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
            nn.Conv2d((3 * args.input_terms), 40, 2, stride=2),
            nn.LeakyReLU(),
            nn.Conv2d(40, 40, 3),
            nn.LeakyReLU(),
            nn.Conv2d(40, 30, 3),
            nn.LeakyReLU(),
            nn.Conv2d(30, 20, 3),
            nn.LeakyReLU(),
            nn.Conv2d(20, 15, 3),
            nn.LeakyReLU(),
            nn.Conv2d(15, 10, 3),
            nn.LeakyReLU(),
            nn.Conv2d(10, 5, 3),
            nn.LeakyReLU(),
            nn.Conv2d(5, 3, 3),
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

    def visualise_forward(self, img, in_data):
        conv1 = self.conv_block1(img)
        conv1_flat = self.flatten(conv1)
        conv1_aug = torch.cat((conv1_flat, in_data), dim=1)
        regression = self.linear_block(conv1_aug)
        return regression, conv1

    @torch.no_grad()
    def visualise_activations(self, dataloader):
        out_dir = Path("fc_images")
        out_dir.mkdir(exist_ok=True)
        for json_data, img, in_data, target in dataloader:
            pred, conv1 = self.visualise_forward(img, in_data)
            conv1_HWC = einops.rearrange(conv1, 'b c h w -> h w (b c)')
            # conv1_HWC_scaled = conv1_HWC / 10
            img_CWC = einops.rearrange(img, 'b c h w -> (b c) h w')
            img_start = img_CWC[0:3]
            img_HWC = einops.rearrange(img_start, 'c h w -> h w c')
            # print(conv1_HWC.shape)
            # print(img_HWC.shape)

            # if np.max(conv1_HWC_scaled.numpy()) > 1:
            #     print(np.max(conv1_HWC.numpy()))
            #     print(np.min(conv1_HWC.numpy()))
            
            time = json_data[0]["id"]
            date = time[0:10]
            result = {"actual": target.item(),
                    self.model_name: pred.item(),
                    "persist": json_data[0]["inputs"][0]["value"]}
            px = 1/plt.rcParams['figure.dpi']  # pixel in inches
            fig, ax = plt.subplots(nrows=3, ncols=2, figsize=(1280*px, 720*px))
            fig.suptitle(time)
            ax[0, 0].imshow(img_HWC.numpy())
            ax[0, 0].axis("off")
            ax[0, 0].set_title("input")

            ax[1, 0].table(cellText=[["actual", self.model_name, "persist"], 
                                     [result["actual"], result[self.model_name], result["persist"]]])
            ax[1, 0].axis("off")

            for c in range(3):
                ax[c,1].imshow(conv1_HWC.numpy()[:, :, c])
                ax[c,1].axis("off")
                ax[c,1].set_title("conv out " + str(c))
            print("Saving fig")
            image_dir = out_dir / date
            image_dir.mkdir(exist_ok=True)
            fig.savefig(image_dir / (time + ".png"))
            plt.close(fig)
        print("out of loop")


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

