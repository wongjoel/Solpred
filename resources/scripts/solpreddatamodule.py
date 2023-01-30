""" 
Common Data Loading code, using the pytorch_lightning framework

Takes [webdataset](https://github.com/tmbdev/webdataset) formatted tar files as input.
"""

# Imports
from pathlib import Path
import io
import json
from argparse import ArgumentParser

import torch
import torchvision
import webdataset as wds
import pytorch_lightning as pl
import numpy as np
import pandas as pd
from PIL import Image


class SolpredDataModule(pl.LightningDataModule):
    @staticmethod
    def add_data_specific_args(parent_parser):
        parser = ArgumentParser(parents=[parent_parser], add_help=False)
        parser.add_argument("--train_ds", type=str, default="/data/default/train_data_{00000..00012}.tar")
        parser.add_argument("--val_ds", type=str, default="/data/default/val_data_{00000..00012}.tar")
        parser.add_argument("--test_ds", type=str, default="/data/default/test_data_{00000..00012}.tar")
        parser.add_argument("--batch_size", type=int, default=8)
        parser.add_argument("--partial_batch", action='store_true', help="Allow partial batches")
        parser.add_argument("--num_workers", type=int, default=1)
        parser.add_argument("--img_width", type=int, required=True, help="Image width")
        return parser

    def __init__(self, args):
        super().__init__()
        self.train_ds_path = args.train_ds
        self.val_ds_path = args.val_ds
        self.test_ds_path = args.test_ds
        self.batch_size = args.batch_size
        self.partial_batch = args.partial_batch
        self.num_workers = args.num_workers
        self.transform = torchvision.transforms.Compose([
            torchvision.transforms.Resize((args.img_width, args.img_width)),
            torchvision.transforms.ToTensor(),
        ])

    def decode_webp(self, sample):
        for key, value in sample.items():
            if ".webp" in key:
                with io.BytesIO(value) as img_data:
                    sample[key] = self.transform(Image.open(img_data))
        return sample

    def stack_images(self, sample):
        keys = sorted([key for key in sample if ".webp" in key])
        stacked_image_TCHW = torch.stack([sample[key] for key in keys], dim=0)
        sample["stacked_image"] = torch.flatten(stacked_image_TCHW, 0, 1)
        return sample

    def extract_json(self, sample):
        data = json.loads(sample["data.json"])
        input_data = sorted(data["inputs"], key=lambda record: record["distance"])
        sample["data.json"] = data # This is required for the test phase
        ghi_label = "globalcmp11physical" if "globalcmp11physical" in data["inputs"][0] else "value"
        dni_label = "directchp1physical" if "directchp1physical" in data["inputs"][0] else "value"
        dhi_label = "diffusecmp11physical" if "diffusecmp11physical" in data["inputs"][0] else "value"
        
        
        sample["input_data"] = torch.tensor([x[ghi_label] for x in input_data])
        sample["diffuse_direct_irradiance"] = torch.tensor([x[dhi_label] for x in input_data] + [x[dni_label] for x in input_data])
        
        sample["most_recent_clear_sky"] = torch.tensor([input_data[0]["clearskyghi"]]) if "clearskyghi" in input_data[0] else torch.tensor(0)

        target_data = sorted(data["targets"], key=lambda record: record["horizon"])
        sample["target_data"] = torch.tensor([x["value"] for x in target_data])
        sample["target_clear_sky"] = torch.tensor([x["clearskyghi"] for x in target_data]) if "clearskyghi" in target_data[0] else torch.tensor([0 for x in target_data])
        return sample

    def decode_pipeline(self, sample):
        sample = self.decode_webp(sample)
        sample = self.stack_images(sample)
        sample = self.extract_json(sample)
        return sample

    def setup(self, stage=None):
        self.train_dataset = (wds.WebDataset(self.train_ds_path, shardshuffle=True)
            .shuffle(5000)
            .map(self.decode_pipeline)
            .to_tuple("stacked_image", "input_data", "target_data", "diffuse_direct_irradiance", "most_recent_clear_sky", "target_clear_sky", "data.json")
            .batched(self.batch_size, partial=self.partial_batch))
        self.val_dataset = (wds.WebDataset(self.val_ds_path, shardshuffle=True)
            .map(self.decode_pipeline)
            .to_tuple("stacked_image", "input_data", "target_data", "diffuse_direct_irradiance", "most_recent_clear_sky", "target_clear_sky", "data.json")
            .batched(self.batch_size, partial=self.partial_batch))
        self.test_dataset = (wds.WebDataset(self.test_ds_path, shardshuffle=True)
            .map(self.decode_pipeline)
            .to_tuple("stacked_image", "input_data", "target_data", "diffuse_direct_irradiance", "most_recent_clear_sky", "target_clear_sky", "data.json")
            .batched(1, partial=self.partial_batch))

    def train_dataloader(self):
        # return DataLoader(self.train_dataset, num_workers=self.num_workers, batch_size=None)
        return wds.WebLoader(
            self.train_dataset,
            batch_size=None,
            shuffle=False,
            num_workers=self.num_workers,
            pin_memory=True
        )

    def val_dataloader(self):
        # return DataLoader(self.val_dataset, num_workers=self.num_workers, batch_size=None)
        return wds.WebLoader(
            self.val_dataset,
            batch_size=None,
            shuffle=False,
            num_workers=self.num_workers,
            pin_memory=True
        )
    
    def test_dataloader(self):
        # return DataLoader(self.test_dataset, num_workers=self.num_workers, batch_size=None)
        return wds.WebLoader(
            self.test_dataset,
            batch_size=None,
            shuffle=False,
            num_workers=self.num_workers,
            pin_memory=True
        )
