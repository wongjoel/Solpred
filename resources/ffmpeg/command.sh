#!/bin/bash
# Command to run a two pass VP9 encoder on images in image-%04.png format
# Target bitrate chosen by -b:v 1500k
# Output video scaled with -vf scale=640:-1
# Framerate set with -framerate 10
# first arg = basedir

echo started script with arguments $@
date

# two pass VP9 encoder
#ffmpeg -i $1/image-%04d.png -c:v libvpx-vp9 -b:v 1024k -pass 1 -an -f webm /dev/null && ffmpeg -i $1/image-%04d.png -c:v libvpx-vp9 -b:v 1024k -pass 2 $1.webm

# two pass VP9 encoder with framerate set
#ffmpeg -framerate 10 -i $1/image-%04d.png -vf "fps=10,scale=640:-1" -c:v libvpx-vp9 -b:v 1024k -pass 1 -an -f webm /dev/null && \
#ffmpeg -framerate 10 -i $1/image-%04d.png -vf "fps=10,scale=640:-1" -c:v libvpx-vp9 -b:v 1024k -pass 2 $2

# NVidia h264 hardware encoder
#ffmpeg -i $1/image-%04d.png -c:v h264_nvenc -profile high444p -pixel_format yuv444p -preset default $1.mkv

# NVidia hevc hardware encoder
ffmpeg -i $1/image-%04d.png -c:v hevc_nvenc $1.mkv
