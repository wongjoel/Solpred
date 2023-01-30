# solpred

Code used to create the solpred dataset in the webdataset format, run the solpred models, and collect and organise the results

## Development environment

The development environment used to run the code is encapsulated in an apptainer container. Even if you don't use the container, inspecting the build file will allow you to see the state of the environment when the code was developed and tested.

## Usage

To Install Python Deps (from pyproject.toml and poetry.lock):
```bash
poetry install --no-root
```

To generate requirements.txt (for use with pip):
```bash
poetry export -f requirements.txt --output requirements.txt
```

To run CIDER nrepl (and download dependencies from deps.edn)
```bash
clj -M:cider-clj -P
```

To Build Jar:
```bash
clj -T:build clean && clj -T:build uber
```

To Use Jar:
```bash
java -XX:+UseG1GC -XX:ParallelGCThreads=8 -cp solpred.jar clojure.main -m solpred.core dataset --config-path config.edn 

java -XX:+UseG1GC -XX:ParallelGCThreads=8 -cp solpred.jar clojure.main -m solpred.core clean --threshold-mb 10.1 --base-dir x

java -XX:+UseG1GC -XX:ParallelGCThreads=8 -cp solpred.jar clojure.main -m solpred.core clean-results --base-path /work --dry-run

java -XX:+UseG1GC -XX:ParallelGCThreads=8 -cp solpred.jar clojure.main -m solpred.core verify --dataset blackmountain

java -XX:+UseG1GC -XX:ParallelGCThreads=8 -cp solpred.jar clojure.main -m solpred.core launch --dataset blackmountain-round --model-name fully-conv --script-name fully_conv.py --run-dir bmr-fc-run --launch-dir bmr-fc-launch --term-list "[2 16]" --spacing-list "[60]" --horizon-list "[420]" --img-width 64 --crop-size 768 --folds 3 --lr-list "[3e-6]"
```

To build apptainer container:
```bash
sudo singularity build solpred.sif build.def
```

To run apptainer container:
```bash
apptainer shell \
    --nv \
    --writable-tmpfs \
    --pwd  <working dir> \
    --bind /tmp:/tmp \
    --bind <Solpred Folder>:/app \
    --bind <Data folder>:/data \
    --bind <Results Folder>:/results \
    --bind <Work Folder>:/work \
    --bind <Output Folder>:/output \
    solpred.sif
```


## Examples

...

### Bugs

...

