""" Dataloader inspector

Run the dataloader and see what it's feeding in to the model

python3 dataset_inspector.py --train_ds "/work/blackmountain-round-64/shards_16x60s_420s/fold1/train_{0000..0036}.tar" --val_ds "/work/blackmountain/shards_2x20s_420s/fold1/val_{0000..0003}.tar" --test_ds "/work/blackmountain/shards_2x20s_420s/test_{0000..0014}.tar" --batch_size 8 --img_width 64
"""

# Imports
from argparse import ArgumentParser

from solpreddatamodule import SolpredDataModule

import torch


def main(args):
    data = SolpredDataModule(args)
    data.setup()

    dataset = data.train_dataloader()

    for img, in_data, target, most_recent_irradiance, most_recent_clear_sky, target_clear_sky, json_data in dataset:
        print("json_data")
        print(json_data)
        print("img size")
        print(img.size())
        print("in_data")
        print(in_data)
        print(in_data.size())
        print("target")
        print(target)
        print(target.size())
        print("most_recent_irradiance")
        print(most_recent_irradiance)
        print("most_recent_clear_sky")
        print(most_recent_clear_sky)
        print("target_clear_sky")
        print(target_clear_sky)
        print(torch.cat([most_recent_clear_sky, target_clear_sky], dim=1))
        break


if __name__ == '__main__':
    parser = ArgumentParser()
    parser = SolpredDataModule.add_data_specific_args(parser)
    main(parser.parse_args())
