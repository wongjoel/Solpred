(ns solpred.launcher.slurm
  "Generates slurm scripts"
  (:require
   [solpred.util.file :as file]
   [clojure.string :as str]))

(defn generate-launch-script
  #_(generate-launch-script {:job-name "SUNSET"
                             :job-time "1-00:00:00"
                             :job-mem  "20gb"
                             :job-cpus 6
                             :host-workdir "/scratch1/won10v/solpred"
                             :bind-app "/scratch1/won10v/phd_work/solpred"})
  "Make a Launch Script"
  [{:keys [job-name job-time job-mem job-cpus
           host-workdir bind-app bind-data bind-work bind-output
           sif-file python-command]}]
  (let [command (str
                 "apptainer exec --nv --writable-tmpfs "
                 "--pwd " bind-app " "
                 "--bind " bind-app ":/app "
                 "--bind " bind-data ":/data "
                 "--bind " bind-work ":/work "
                 "--bind " bind-output ":/output "
                 sif-file " "
                 python-command)]
    (str/join
     "\n"
     ["#!/bin/bash"
      (str "#SBATCH --job-name=" job-name)
      (str "#SBATCH --time=" job-time)
      (str "#SBATCH --mem=" job-mem)
      "#SBATCH --gres=gpu:1"
      "#SBATCH --ntasks=1"
      (str "#SBATCH --cpus-per-task=" job-cpus)
      "#SBATCH --signal=SIGUSR1@90"
      ""
      "#actual script"
      "echo started script"
      "date"
      "pwd"
      ""
      (str "cd " host-workdir)
      "pwd"
      ""
      "module load cuda-driver/current"
      "module load apptainer/1.0.1"
      ""
      (str "echo " command)
      command
      ""
      "date"
      "echo end script"
      ""])))

(defn determine-script
  "Determine which script to launch based on filesystem checks"
  [{:keys [launcher-path checkpoint-file output-file test-started-file force-test]}]
  (cond
    (file/exists? output-file)     (str "echo " output-file " already exists, doing nothing")
    (or (file/exists? test-started-file) 
        force-test)                (str "sbatch " launcher-path " --enable_progress_bar False" " --load_checkpoint " (file/filename checkpoint-file) " --test")
    (file/exists? checkpoint-file) (str "sbatch " launcher-path " --enable_progress_bar False" " --load_checkpoint " (file/filename checkpoint-file) " --resume_train")
    :else                          (str "sbatch " launcher-path " --enable_progress_bar False")))

