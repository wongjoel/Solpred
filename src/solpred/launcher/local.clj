(ns solpred.launcher.local
  "Runs code locally in a way that is compatible with SLURM"
  (:require
   [solpred.util.file :as file]
   [clojure.string :as str]))

(defn generate-launch-script
  #_(generate-launch-script {:host-workdir "/scratch1/won10v/solpred"
                             :python-command "python3 test.py $@"})
  "Make a Launch Script"
  [{:keys [host-workdir python-command]}]
  (str/join
   "\n"
   ["#!/bin/bash"
    ""
    "#actual script"
    "echo started script"
    "date"
    "pwd"
    ""
    (str "cd " host-workdir)
    "pwd"
    ""
    (str "echo " python-command)
    python-command
    ""
    "date"
    "echo end script"
    ""]))

(defn determine-script
  "Determine which script to launch based on filesystem checks"
  [{:keys [launcher-path checkpoint-file output-file test-started-file force-test]}]
  (cond
    (file/exists? output-file)     (str "echo " output-file " already exists, doing nothing")
    (or (file/exists? test-started-file) 
        force-test)                (str launcher-path " --load_checkpoint " (file/filename checkpoint-file) " --test")
    (file/exists? checkpoint-file) (str launcher-path " --load_checkpoint " (file/filename checkpoint-file) " --resume_train")
    :else                          launcher-path))

