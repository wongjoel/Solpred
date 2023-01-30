""" SUNSET Model

Reimplementation of the SUNSET model as a [pytorch-lightning](https://github.com/PyTorchLightning/pytorch-lightning) module.

Takes [webdataset](https://github.com/tmbdev/webdataset) formatted tar files as input.

python3 sunset.py --ds_dir /data/blackmountain/shards_1m_7m_15m --train_ds "train_{0000..0036}.tar" --val_ds "val_{0000..0003}.tar" --test_ds "test_{0000..0014}.tar" --batch_size 8  --gpus 1
python3 sunset.py --ds_dir /data/blackmountain/shards_1m_7m_15m --train_ds "train_{0000..0036}.tar" --val_ds "val_{0000..0003}.tar" --test_ds "test_{0000..0014}.tar" --batch_size 8  --gpus 1 --test --checkpoint_load_path tb_logs/my_sunset_model/version_0/checkpoints/epoch=3-step=510222.ckpt

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
            nn.LeakyReLU(),
            nn.BatchNorm2d(24),
            nn.MaxPool2d(2)
        )
        self.conv_block2 = nn.Sequential(
            nn.Conv2d(24, 48, 3),
            nn.LeakyReLU(),
            nn.BatchNorm2d(48),
            nn.MaxPool2d(2)
        )
        self.flatten = nn.Flatten()
        self.linear_block = nn.Sequential(
            nn.Linear((9408 + args.input_terms), 1024),
            nn.LeakyReLU(),
            nn.Dropout(0.4),
            nn.Linear(1024, 1024),
            nn.LeakyReLU(),
            nn.Dropout(0.4),
            nn.Linear(1024, 1)
        )

    def forward(self, img, in_data, diffuse_direct_irradiance, most_recent_clear_sky, target_clear_sky):
        conv1 = self.conv_block1(img)
        conv2 = self.conv_block2(conv1)
        conv2_flat = self.flatten(conv2)
        conv2_aug = torch.cat((conv2_flat, in_data), dim=1)
        regression = self.linear_block(conv2_aug)
        return regression

    def configure_optimizers(self):
        return torch.optim.Adam(self.parameters(), lr=self.learning_rate)

    def visualise_forward(self, img, in_data):
        conv1 = self.conv_block1(img)
        conv2 = self.conv_block2(conv1)
        conv2_flat = self.flatten(conv2)
        conv2_aug = torch.cat((conv2_flat, in_data), dim=1)
        regression = self.linear_block(conv2_aug)
        return regression, conv2

    @torch.no_grad()
    def visualise_activations(self, dataloader):
        out_dir = Path("sunset_images")
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
            fig, ax = plt.subplots(nrows=5, ncols=10, figsize=(1280*px, 720*px))
            fig.suptitle(time)
            ax[3, 9].imshow(img_HWC.numpy())
            ax[3, 9].axis("off")
            ax[3, 9].set_title("input")

            ax[4, 9].table(cellText=[["actual", self.model_name, "persist"], 
                                     [result["actual"], result[self.model_name], result["persist"]]])
            ax[4, 9].axis("off")

            for col in range(10):
                for row in range(5):
                    index = row + (5 * col)
                    if index >= 48:
                        break
                    else:
                        ax[row,col].imshow(conv1_HWC.numpy()[:, :, index])
                        ax[row,col].axis("off")
                        ax[row,col].set_title("conv out " + str(index))
            print("Saving fig")
            image_dir = out_dir / date
            image_dir.mkdir(exist_ok=True)
            fig.savefig(image_dir / (time + ".png"))
            plt.close(fig)
        print("out of loop")

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
