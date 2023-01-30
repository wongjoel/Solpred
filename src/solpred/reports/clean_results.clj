(ns solpred.reports.clean-results
  "Processes the folder of raw results"
  (:require
   [clojure.string :as str]
   [solpred.util.file :as file]
   [solpred.util.runner :as runner]
   [solpred.util.runtime-check :as runtime-check]
   [solpred.util.pprint-edn :as edn-out]
   [solpred.util.simple-yaml :as simple-yaml]
   [clojure.tools.logging :as log]))

(defn find-hparams
  #_(find-hparams "/results/solpred")
  "Searches a directory tree for hparams.yaml inside an auto-process folder"
  [base-path]
  (->> (file/walk base-path)
       (filter (fn [child] (str/includes? (file/filename child) "hparams.yaml")))
       (filter (fn [child] (str/includes? (str child) "auto-process")))
       ))

(defn parse-checkpoint-name
  #_(parse-checkpoint-name "fully-conv_epoch=03-val_loss=11529.01.ckpt")
  #_(parse-checkpoint-name "fully-conv_epoch=48-val_loss=6925.77.ckpt")
  [ckpt-name]
  (let [clean-name
        (-> ckpt-name
            (str/replace #".ckpt" "")
            (str/replace #"val_loss" "")
            (str/replace #"[_\\-]" "")
            (str/replace #"epoch" ""))
        splits (str/split clean-name #"=")]
    (runtime-check/throw-on-false (= 3 (count splits)) (str "Expected 3 splits, found " splits " " ckpt-name))
    (runtime-check/throw-on-false (parse-long (nth splits 1)) (str "Can't parse epochs " splits " " ckpt-name))
    (runtime-check/throw-on-false (parse-double (nth splits 2)) (str "Can't parse val_loss " splits " " ckpt-name))
    {:model-id (nth splits 0)
     :epochs (parse-long (nth splits 1))
     :val_loss (parse-double (nth splits 2))})
  )

(defn parse-dir-structure
  #_(parse-dir-structure "/results/solpred/blackmountain/auto-process/sunset/run_01/crop_full/lr_3.0E-6/fold1/4x40s_420s")
  #_(parse-dir-structure "/results/solpred/stanford/auto-process/fully-conv/crop_full/lr_3.0E-6/fold0/16x60s_420s")
  [path]
  #_(log/debug path)
  (let [splits (take-last 8 (file/as-seq path))]
    {:dataset-id (nth splits 0)
     :model-id (nth splits 2)
     :run-id (str/replace (nth splits 3) #"run_" "")
     :crop-size (str/replace (nth splits 4) #"crop_" "")
     :learn-rate (str/replace (nth splits 5) #"lr_" "")
     :fold-id (str/replace (nth splits 6) #"fold" "")
     :inout-config (nth splits 7)}))

(defn delete-files!
  [candidate-files]
  (let [num-files (count candidate-files)]
    (log/debug (str "Found " num-files " files to delete"))
    (run! file/delete-recursive candidate-files)
    num-files)
  )

(defn delete-logs-without-checkpoints!
  #_(delete-logs-without-checkpoints!  "/results/solpred")
  "Deletes log folders without checkpoints, as we can't resume without a checkpoint file"
  [base-path]
  {:missing-dir
   (delete-files!
    (->> (find-hparams base-path)
         (filter #(file/not-exists? (file/resolve-sibling % "checkpoints")))
         (map file/parent)))

   :empty-dir
   (delete-files!
    (->> (find-hparams base-path)
         (filter #(= 0 (count (file/list-children (file/resolve-sibling % "checkpoints")))))
         (map file/parent)))}
  )

(defn delete-intermediate-checkpoints!
  #_(delete-intermediate-checkpoints! "/results/solpred")
  "Deletes intermediate checkpoints, only keeping the best one"
  [base-path]
  {:intermediate-dir
   (delete-files!
    (->> (find-hparams base-path)
         (group-by #(file/parent (file/parent %)))
         (reduce-kv (fn [acc k v]
                      (conj acc {:log-dir k
                                 :checkpoints (map #(file/resolve-sibling % "checkpoints") v)}))
                    [])
         (filter #(< 1 (count (:checkpoints %))))
         (mapcat (fn [run]
                   (->> (:checkpoints run)
                        (mapcat (fn [ckpt-dir] (file/list-children ckpt-dir)))
                        (map (fn [ckpt-file] (merge {:path (str ckpt-file)}
                                                    (parse-checkpoint-name (file/filename ckpt-file)))))
                        vec
                        (sort-by :val_loss)
                        (drop 1)
                        (map :path)
                        (map file/parent)
                        (map file/parent))))))}
  )

(defn compute-run-descriptions
  #_(compute-run-descriptions "/results/solpred")
  [base-path]
  (->> (find-hparams base-path)
         (group-by #(file/parent (file/parent %)))
         (reduce-kv (fn [acc k v]
                      (conj acc {:log-dir k
                                 :hparams-path v
                                 :checkpoints (map #(file/resolve-sibling % "checkpoints") v)}))
                    [])
         (map (fn [{:keys [log-dir hparams-path checkpoints]}]
                (if (= 1 (count checkpoints))
                  {:run-dir (file/parent log-dir)
                   :hparams-path (str (first hparams-path))
                   :checkpoint-dir (first checkpoints)}
                  (runtime-check/throw-ill-arg (str "Multiple checkpoint dirs found: " (vec checkpoints))))))
         (map (fn [{:keys [checkpoint-dir] :as run}]
                (let [ckpt-dir-children (file/list-children checkpoint-dir)]
                  (if (= 1 (count ckpt-dir-children))
                    (assoc run :checkpoint-path (str (first ckpt-dir-children)))
                    (runtime-check/throw-ill-arg (str "Multiple checkpoints found: " (vec ckpt-dir-children)))))))
         (map #(merge % (parse-checkpoint-name (file/filename (:checkpoint-path %)))))
         (map #(merge % (parse-dir-structure (:run-dir %))))
         (map #(assoc % :hparams (simple-yaml/parse-file (:hparams-path %)))))
  )

(defn add-run-descriptions!
  #_(add-run-descriptions! (compute-run-descriptions "/results/solpred"))
  [runs]
  (run! (fn [run]
          (edn-out/spit-pprint run
                               (file/resolve-path [(:run-dir run) "run-description.edn"])))
        runs))

(defn delete-intermediate-files!
  #_(delete-intermediate-files! (compute-run-descriptions "/results/solpred"))
  [runs]
  {:intermediate-files 
   (delete-files!
    (->> runs
         (map :run-dir runs)
         (mapcat (fn [run-dir] (map #(file/resolve-path [run-dir %])
                                    ["latest_best.ckpt" "__pycache__" "started_testing.json"])))
         (filter file/exists?)))}
  )

(defn main
  #_(main "/results/solpred")
  [base-path]
  (let [logs1 (delete-logs-without-checkpoints! base-path)
        logs2 (delete-intermediate-checkpoints! base-path)
        run-desc (compute-run-descriptions base-path)
        intermediate (delete-intermediate-files! run-desc)]
    (add-run-descriptions! run-desc)
    (merge logs1 logs2 intermediate)))
