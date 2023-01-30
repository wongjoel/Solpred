(ns solpred.util.cleaner
  "Clean up Results
   * Remove extra checkpoints
   * Remove __pycache__
   * Remove started_testing.json
  "
  (:require
   [clojure.tools.logging :as log]
   [solpred.util.runner :as runner]
   [solpred.util.file :as file]))


(defn bytes->megabytes
  "Name the conversion from bytes to megabytes"
  [size-bytes]
  (/ size-bytes (* 1024 1024)))


(defn delete-subdirs!
  "Delete all things which aren't files in the given dir"
  [{:keys [base-dir dry-run] :or {dry-run? false}}]
  (let [jobs
        (->> (file/list-children base-dir)
             (filter file/dir?)
             (map (fn [child] (log/debug (str "Marked " child)) child))
             (map (fn [child] #(file/delete-recursive child))))]
    (if dry-run
      (log/info "Dry Run - skipping delete")
      (runner/run-jobs! {:jobs jobs}))))


(defn delete-small-files!
  "delete files below a threshold"
  [{:keys [base-dir threshold-mb dry-run] :or {dry-run? false}}]
  (let [jobs
        (->> (file/list-children base-dir)
             (filter file/file?)
             (filter (fn [child] (> threshold-mb (bytes->megabytes (file/file-size child)))))
             (map (fn [child] (log/debug (str "Marked " child)) child))
             (map (fn [child] #(file/delete child))))]
    (if dry-run
      (log/info "Dry Run - skipping delete")
      (runner/run-jobs! {:jobs jobs}))))

(defn main
  ""
  [args]
  (log/debug "Main"))
