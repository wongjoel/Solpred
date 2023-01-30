(ns solpred.launcher.launcher
  "Makes supporting folders, launch scripts, and submits scripts to the job queue"
  (:require
   [clojure.data.json :as json]
   [clojure.string :as str]
   [clojure.tools.logging :as log]
   [solpred.util.file :as file]
   [solpred.util.runtime-check :as runtime-check]
   [solpred.launcher.slurm :as slurm]
   [solpred.launcher.local :as local]))

(defn make-python-command
  "Create the python command to invoke"
  [{:keys [out-file input-terms script-name model-name train-ds val-ds test-ds img-width learning-rate job-cpus]}]
  (str
   "python3 " script-name " "
   "--model_name " model-name " "
   "--train_ds \"" train-ds "\" "
   "--val_ds \"" val-ds "\" "
   "--test_ds \"" test-ds "\" "
   "--img_width " img-width " "
   "--max_epochs -1 "
   "--batch_size 8 "
   "--accelerator gpu "
   "--learning_rate " learning-rate " "
   "--test_output " out-file " "
   "--input_terms " input-terms " "
   "--benchmark True "
   "--num_workers " job-cpus " "
   "--precision 16 "
   "$@"))

(defn make-scripts!
  "Makes the launch script folder and populates it"
  [runs]
  (run! (fn [{:keys [launcher-path launch-script]}]
          (file/make-dirs (file/parent launcher-path))
          (spit launcher-path launch-script)
          (file/set-executable-bit launcher-path))
        runs))

(defn make-run-folders!
  "Makes the folders where the runs happen"
  [runs]
  (run! (fn [{:keys [run-folder] :as run}]
          (file/make-dirs (file/parent run-folder))
          (file/copy-recursive "resources/scripts" run-folder)
          (spit (file/resolve-path [run-folder "run-params.json"]) (json/write-str run)))
        runs))

(defn replace-old-checkpoint!
  "Replace the older checkpoint file with the newer one if they both exist"
  [checkpoint-dir checkpoint-name replacement-checkpoint-name]
  (let [checkpoint (file/resolve-path [checkpoint-dir checkpoint-name])
        replacement (file/resolve-path [checkpoint-dir replacement-checkpoint-name])]
    (when (and (file/exists? checkpoint) (file/exists? replacement))
      (println (str "Replacing " checkpoint " with " replacement))
      (file/delete checkpoint)
      (file/move replacement checkpoint))))

(defn replace-old-checkpoints!
  "Replace the older checkpoint files with the newer ones"
  [runs]
  (run! (fn [{:keys [host-basedir run-folder]}]
          (replace-old-checkpoint! (file/resolve-path [host-basedir run-folder]) "latest_best.ckpt" "latest_best-v1.ckpt"))
        runs))

(defn make-launcher-script!
  "Launch the scripts"
  [runs]
  (let [launch-dir (:launch-dir (first runs))
        launch-file (file/resolve-path [launch-dir "launch.sh"])
        commands
        (->> runs
             (map (fn [{:keys [host-basedir run-folder launcher-path out-file engine] :as run}]
                    (let [args {:launcher-path (file/resolve-path [host-basedir launcher-path])
                                :checkpoint-file (file/resolve-path [host-basedir run-folder "latest_best.ckpt"])
                                :output-file (file/resolve-path [host-basedir run-folder out-file])
                                :test-started-file (file/resolve-path [host-basedir run-folder "started_testing.json"])}
                          command (case engine
                                    "local" (local/determine-script args)
                                    "slurm" (slurm/determine-script args))]
                      (assoc run :command command))))
             (map (fn [{:keys [command]}]
                    command)))]
    (spit launch-file (str/join "\n" commands))
    (file/set-executable-bit launch-file)
    (log/debug (str "Commands written to " launch-file))))

(defn launch!
  "Makes launch scripts and the supporting folders"
  [runs]
  (make-scripts! runs)
  (make-run-folders! runs)
  (replace-old-checkpoints! runs)
  (make-launcher-script! runs))

(defn make-runs
  [{:keys [term-list spacing-list lr-list folds horizon-list term-list spacing-list num-workers] :as args}]
  (runtime-check/throw-on-true (< 10 folds) (str "More than 10 folds not supported, recieved " folds))
  (runtime-check/map-contains? args [:term-list :spacing-list :lr-list :folds :horizon-list :term-list :spacing-list :num-workers])
  (->> (for [terms term-list
             spacing spacing-list
             fold (range 0 folds)
             learning-rate lr-list
             horizon horizon-list]
         (assoc args
                :input-terms terms
                :input-spacing spacing
                :horizon horizon
                :fold fold
                :learning-rate learning-rate
                :job-time (if (< terms 8) "3-00:00:00" "5-00:00:00")
                :job-mem  "20gb"
                :job-cpus num-workers
                :base-disk "/scratch2"
                :bind-data "/datastore/won10v"
                :run-name (str terms "x" spacing "s_" horizon "s")))
       (map (fn [{:keys [run-name model-name base-disk fold dataset img-width run-dir launch-dir learning-rate crop-size engine] :as run}]
              (assoc run
                     :host-basedir (case engine
                                     "local" "/app"
                                     "slurm" (file/resolve-path [base-disk "won10v" "phd_work" "solpred"]))
                     :bind-work (file/resolve-path [base-disk "won10v" "work"])
                     :bind-output (file/resolve-path [base-disk "won10v" "output"])
                     :sif-file (file/resolve-path [base-disk "won10v" "phd_work" "solpred" "env" "solpred.sif"])
                     :job-name (str run-name "_" model-name)
                     :out-file (str run-name "_" model-name "_out.csv.gz")
                     :run-folder (file/resolve-path [run-dir (str "crop_" crop-size) (str "lr_" learning-rate) (str "fold" fold) run-name])
                     :launcher-path (file/resolve-path [launch-dir (str "lr_" learning-rate) (str "fold" fold) (str "launch_" run-name ".sh")])
                     :ds-dir (file/resolve-path ["/work" (str dataset "-" crop-size "-" img-width) (str "shards_" run-name)]))))
       (map (fn [{:keys [host-basedir run-folder ds-dir fold train-ds val-ds test-ds] :as run}]
              (assoc run
                     :host-workdir (file/resolve-path [host-basedir run-folder])
                     :bind-app (file/resolve-path [host-basedir run-folder])
                     :train-ds (file/resolve-path [ds-dir (str "fold" fold) train-ds])
                     :val-ds (file/resolve-path [ds-dir (str "fold" fold)  val-ds])
                     :test-ds (file/resolve-path [ds-dir test-ds]))))
       (map (fn [run] (assoc run :python-command (make-python-command run))))
       (map (fn [{:keys [engine] :as run}]
              (assoc run
                     :launch-script (case engine
                                      "slurm" (slurm/generate-launch-script run)
                                      "local" (local/generate-launch-script run)))))
       ))

(defn make-bm-runs
  [args]
  (let [bm-args (assoc args
                       :train-ds "train_{0000..0036}.tar"
                       :val-ds "val_{0000..0003}.tar"
                       :test-ds "test_{0000..0014}.tar")]
    (make-runs bm-args)))

(defn make-sunset-runs
  [args]
  (let [sunset-args (assoc args
                           :train-ds "train_{0000..0039}.tar"
                           :val-ds "val_{0000..0003}.tar"
                           :test-ds "test_{0000..0003}.tar")]
    (make-runs sunset-args)))

(defn main
  [{:keys [dataset] :as args}]
  (let [runs (case dataset
              "blackmountain" (make-bm-runs args)
              "blackmountain-intermittent" (make-bm-runs args)
              "blackmountain-round" (make-bm-runs args)
              "blackmountain-ramp" (make-bm-runs args)
              "blackmountain-ramp-round" (make-bm-runs args)
              "sunset" (make-sunset-runs args))]
    (launch! runs)))

(comment
  (def args
    {:dataset "blackmountain"
     :dry-run true
     :model-name "fully-conv"
     :script-name "fully-conv.py"
     :run-dir "conv-run"
     :launch-dir "conv-launch"
     :engine "slurm"
     :folds 3
     :img-width 64
     :lr-list [3e-6]})

  (main args)
  )
