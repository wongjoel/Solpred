from pathlib import Path
import json
import argparse

def parse_args():
    parser = argparse.ArgumentParser(description="CLI script to process job files")
    parser.add_argument('--job_file', type=Path, help="job file (*.json format)")
    args = parser.parse_args()
    return args

if __name__ == '__main__':
    # Parameters
    args = parse_args()
    print("hello world")
    print(Path.cwd())
    with args.job_file.open() as job_file:
        job = json.load(job_file)
        print(job)
