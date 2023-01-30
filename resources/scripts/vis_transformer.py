""" Visual Transformer Model

Implementation of a [Visual transformer](https://arxiv.org/abs/2006.03677) model as a [pytorch-lightning](https://github.com/PyTorchLightning/pytorch-lightning) module. 

https://analyticsindiamag.com/hands-on-vision-transformers-with-pytorch/
https://github.com/tahmid0007/VisualTransformers/blob/main/ResViT.py

Takes [webdataset](https://github.com/tmbdev/webdataset) formatted tar files as input.

python3 vis_transformer.py --model_name vis-transform --train_ds "/work/blackmountain/shards_2_20s/train_{0000..0036}.tar" --val_ds "/work/blackmountain/shards_2_20s/val_{0000..0003}.tar" --test_ds "/work/blackmountain/shards_2_20s/test_{0000..0014}.tar" --input_terms 2 --gpus 1 --benchmark True

tensorboard --logdir lightning_logs/
"""


# Imports
from pathlib import Path
from argparse import ArgumentParser

import torch
import torchvision
import einops
from torch import nn
import torch.nn.functional as F
import pytorch_lightning as pl
from pytorch_lightning.loggers import TestTubeLogger
from pytorch_lightning.callbacks import EarlyStopping, ModelCheckpoint
import numpy as np
import pandas as pd
from PIL import Image

from solpreddatamodule import SolpredDataModule
import solpred_common

class LambdaLayer(nn.Module):
    def __init__(self, lambd):
        super(LambdaLayer, self).__init__()
        self.lambd = lambd

    def forward(self, x):
        return self.lambd(x)


class BasicBlock(nn.Module):
    expansion = 1

    def __init__(self, in_planes, planes, stride=1, option='A'):
        super(BasicBlock, self).__init__()
        self.conv1 = nn.Conv2d(in_planes, planes, kernel_size=3, stride=stride, padding=1, bias=False)
        self.bn1 = nn.BatchNorm2d(planes)
        self.conv2 = nn.Conv2d(planes, planes, kernel_size=3, stride=1, padding=1, bias=False)
        self.bn2 = nn.BatchNorm2d(planes)

        self.shortcut = nn.Sequential()
        if stride != 1 or in_planes != planes:
            if option == 'A':
                """
                For CIFAR10 ResNet paper uses option A.
                """
                self.shortcut = LambdaLayer(lambda x:
                                            F.pad(x[:, :, ::2, ::2], (0, 0, 0, 0, planes//4, planes//4), "constant", 0))
            elif option == 'B':
                self.shortcut = nn.Sequential(
                     nn.Conv2d(in_planes, self.expansion * planes, kernel_size=1, stride=stride, bias=False),
                     nn.BatchNorm2d(self.expansion * planes)
                )

    def forward(self, x):
        out = F.relu(self.bn1(self.conv1(x)))
        out = self.bn2(self.conv2(out))
        out += self.shortcut(x)
        out = F.relu(out)
        #print(out.size())
        return out



class Residual(nn.Module):
    def __init__(self, fn):
        super().__init__()
        self.fn = fn
    def forward(self, x, **kwargs):
        return self.fn(x, **kwargs) + x

class LayerNormalize(nn.Module):
    def __init__(self, dim, fn):
        super().__init__()
        self.norm = nn.LayerNorm(dim)
        self.fn = fn
    def forward(self, x, **kwargs):
        return self.fn(self.norm(x), **kwargs)

class MLP_Block(nn.Module):
    def __init__(self, dim, hidden_dim, dropout = 0.1):
        super().__init__()
        self.nn1 = nn.Linear(dim, hidden_dim)
        torch.nn.init.xavier_uniform_(self.nn1.weight)
        torch.nn.init.normal_(self.nn1.bias, std = 1e-6)
        self.af1 = nn.GELU()
        self.do1 = nn.Dropout(dropout)
        self.nn2 = nn.Linear(hidden_dim, dim)
        torch.nn.init.xavier_uniform_(self.nn2.weight)
        torch.nn.init.normal_(self.nn2.bias, std = 1e-6)
        self.do2 = nn.Dropout(dropout)
        
    def forward(self, x):
        x = self.nn1(x)
        x = self.af1(x)
        x = self.do1(x)
        x = self.nn2(x)
        x = self.do2(x)
        
        return x

class Attention(nn.Module):
    def __init__(self, dim, heads = 8, dropout = 0.1):
        super().__init__()
        self.heads = heads
        self.scale = dim ** -0.5  # 1/sqrt(dim)

        self.to_qkv = nn.Linear(dim, dim * 3, bias = True) # Wq,Wk,Wv for each vector, thats why *3
        torch.nn.init.xavier_uniform_(self.to_qkv.weight)
        torch.nn.init.zeros_(self.to_qkv.bias)
        
        self.nn1 = nn.Linear(dim, dim)
        torch.nn.init.xavier_uniform_(self.nn1.weight)
        torch.nn.init.zeros_(self.nn1.bias)        
        self.do1 = nn.Dropout(dropout)
        

    def forward(self, x, mask = None):
        b, n, _, h = *x.shape, self.heads
        qkv = self.to_qkv(x) #gets q = Q = Wq matmul x1, k = Wk mm x2, v = Wv mm x3
        q, k, v = einops.rearrange(qkv, 'b n (qkv h d) -> qkv b h n d', qkv = 3, h = h) # split into multi head attentions

        dots = torch.einsum('bhid,bhjd->bhij', q, k) * self.scale

        if mask is not None:
            mask = F.pad(mask.flatten(1), (1, 0), value = True)
            assert mask.shape[-1] == dots.shape[-1], 'mask has incorrect dimensions'
            mask = mask[:, None, :] * mask[:, :, None]
            dots.masked_fill_(~mask, float('-inf'))
            del mask

        attn = dots.softmax(dim=-1) #follow the softmax,q,d,v equation in the paper

        out = torch.einsum('bhij,bhjd->bhid', attn, v) #product of v times whatever inside softmax
        out = einops.rearrange(out, 'b h n d -> b n (h d)') #concat heads into one matrix, ready for next encoder block
        out =  self.nn1(out)
        out = self.do1(out)
        return out

class Transformer(nn.Module):
    def __init__(self, dim, depth, heads, mlp_dim, dropout):
        super().__init__()
        self.layers = nn.ModuleList([])
        for _ in range(depth):
            self.layers.append(nn.ModuleList([
                Residual(LayerNormalize(dim, Attention(dim, heads = heads, dropout = dropout))),
                Residual(LayerNormalize(dim, MLP_Block(dim, mlp_dim, dropout = dropout)))
            ]))
    def forward(self, x, mask = None):
        for attention, mlp in self.layers:
            x = attention(x, mask = mask) # go to attention
            x = mlp(x) #go to MLP_Block
        return x


class VisTransformer(solpred_common.SolpredModule):
    @staticmethod
    def add_model_specific_args(parent_parser):
        parser = ArgumentParser(parents=[parent_parser], add_help=False)
        parser.add_argument('--input_terms', type=int, default=16)
        parser.add_argument('--model_name', type=str, default='model-name')

        parser.add_argument('--dim', type=int, default=128)
        parser.add_argument('--num_tokens', type=int, default=8)
        parser.add_argument('--mlp_dim', type=int, default=256)
        parser.add_argument('--heads', type=int, default=8)
        parser.add_argument('--depth', type=int, default=6)
        parser.add_argument('--emb_dropout', type=float, default=0.1)
        parser.add_argument('--dropout', type=float, default=0.1)
        return parser

    def __init__(self, args):
        super().__init__(args)
        self.save_hyperparameters()

        self.in_planes = 16
        
        self.conv_block1 = nn.Sequential(
            nn.Conv2d((3 * args.input_terms), 16, kernel_size=3, stride=1, padding=1, bias=False),
            nn.BatchNorm2d(16),
            nn.ReLU()
        )
        self.layer1 = self._make_layer(16, 3, stride=1)
        self.layer2 = self._make_layer(32, 3, stride=2)
        self.layer3 = self._make_layer(64, 3, stride=2) #8x8 feature maps (64 in total)
        
        
        # Tokenization
        self.token_wA = nn.Parameter(torch.empty((args.batch_size), args.num_tokens, 64), requires_grad = True) #Tokenization parameters
        torch.nn.init.xavier_uniform_(self.token_wA)
        self.token_wV = nn.Parameter(torch.empty(args.batch_size, 64, args.dim), requires_grad = True) #Tokenization parameters
        torch.nn.init.xavier_uniform_(self.token_wV)        
             
        
        self.pos_embedding = nn.Parameter(torch.empty(1, (args.num_tokens + 1), args.dim))
        torch.nn.init.normal_(self.pos_embedding, std = .02) # initialized based on the paper

        #self.patch_conv= nn.Conv2d(64,dim, self.patch_size, stride = self.patch_size) 

        self.cls_token = nn.Parameter(torch.zeros(1, 1, args.dim)) #initialized based on the paper
        self.dropout = nn.Dropout(args.emb_dropout)

        self.transformer = Transformer(args.dim, args.depth, args.heads, args.mlp_dim, args.dropout)

        self.to_cls_token = nn.Identity()

        self.final = nn.Sequential(
            nn.Linear(args.dim, args.dim),
            nn.ReLU(),
            nn.Linear(args.dim, 1),
        )

    def _make_layer(self, planes, num_blocks, stride):
        strides = [stride] + [1]*(num_blocks-1)
        layers = []
        for stride in strides:
            layers.append(BasicBlock(self.in_planes, planes, stride))
            self.in_planes = planes * BasicBlock.expansion

        return nn.Sequential(*layers)

    def forward(self, img, in_data, most_recent_irradiance, most_recent_clear_sky, target_clear_sky):
        x = self.conv_block1(img)
        x = self.layer1(x)
        x = self.layer2(x)  
        x = self.layer3(x) 

        x = einops.rearrange(x, 'b c h w -> b (h w) c') # 64 vectors each with 64 points. These are the sequences or word vectors like in NLP

        #Tokenization 
        wa = einops.rearrange(self.token_wA, 'b h w -> b w h') #Transpose
        try:
            A = torch.einsum('bij,bjk->bik', x, wa)
        except:
            print("--------------")
            print(x.size())
            print(wa.size())
            print("--------------")
            raise
        A = einops.rearrange(A, 'b h w -> b w h') #Transpose
        A = A.softmax(dim=-1)

        VV = torch.einsum('bij,bjk->bik', x, self.token_wV)       
        T = torch.einsum('bij,bjk->bik', A, VV)  
        #print(T.size())

        cls_tokens = self.cls_token.expand(T.shape[0], -1, -1)

        x = torch.cat((cls_tokens, T), dim=1)
        x += self.pos_embedding
        x = self.dropout(x)
        x = self.transformer(x) #main game
        x = self.to_cls_token(x[:, 0])       
        x = self.final(x)

        return x


    def configure_optimizers(self):
        return torch.optim.Adam(self.parameters(), lr=self.learning_rate)

def main(args):
    model = VisTransformer.load_from_checkpoint(args.load_checkpoint) if args.load_checkpoint else VisTransformer(args)
    solpred_common.main(args, model)


if __name__ == '__main__':
    parser = ArgumentParser()
    # add PROGRAM level args
    parser.add_argument('--visualise', action='store_true', help="Only run visualise_activations, no training")
    parser.add_argument('--test', action='store_true', help="Only run inference, no training")
    parser.add_argument('--test_output', type=str, default='test_out.csv.gz')
    # add model specific args
    parser = VisTransformer.add_model_specific_args(parser)
    parser = SolpredDataModule.add_data_specific_args(parser)
    parser = pl.Trainer.add_argparse_args(parser)
    main(parser.parse_args())
