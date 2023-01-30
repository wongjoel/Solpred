"""

"""

from pathlib import Path
import argparse
import json

from PIL import Image, ImageDraw

def drawCircle(image, point):
    draw = ImageDraw.Draw(image)
    p1 = tuple(map(lambda x: x-20, point))
    p2 = tuple(map(lambda x: x+20, point))
    draw.ellipse([p1, p2], fill=128)

def main():
    with open("/work/coords.json") as f:
        coords = json.load(f)
    


    # row = 40
    # col = 400
    # p1 = (col, row)
    with Image.open("/work/img1.jpg") as im:
        for i, point in enumerate(coords):
            p1 = (point["col"], point["row"])
            #p1 = (point["row"], point["col"])
            drawCircle(im, p1)
            im.save(f"/work/out{i}.png")
            #im.save(f"/work/out{i}_{point['x']:.1f}_{point['y']:.1f}_{point['z']:.1f}.png")

                     

if __name__ == '__main__':
    main()