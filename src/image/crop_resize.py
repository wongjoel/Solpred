"""
Crops and resizes an image

python3 crop_resize.py --infile test.jpg --outfile out.webp --left 10 --upper 0 --lower 200 --right 210 --width 100 --height 100
python3 crop_resize.py --infile test1.jpg test2.jpg --outfile out1.webp out2.webp --left 10 --upper 0 --lower 200 --right 210 --width 100 --height 100
"""

from pathlib import Path
import argparse
import json
from zipfile import ZipFile

from PIL import Image

def parse_args():
    parser = argparse.ArgumentParser(description="CLI script to crop and resize images")
    parser.add_argument('instr_file', type=Path, help="Json file with instructions")
    return parser.parse_args()

def main(args):
    with open(args.instr_file) as f:
        instr = json.load(f)
    with ZipFile(instr["image-zip"], "r") as imgzip:
        for params in instr["images"]:
            box = (params["left"], params["upper"], params["right"], params["lower"])
            size=(params["width"], params["height"])
            path = Path(instr["tmp-image-dir"]) / params["filename"]
            with imgzip.open(params["original-filename"]) as imgfile:
                with Image.open(imgfile) as img:
                    img.crop(box).resize(size).save(path)
                    

if __name__ == '__main__':
    main(parse_args())
