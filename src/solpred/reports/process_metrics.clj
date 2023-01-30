(ns solpred.reports.process-metrics
  "Processes the output of run"
  (:require
   [clojure.string :as str]
   [clojure.edn :as edn]
   [clojure.tools.logging :as log]
   [clojure.java.io :as io]
   [clojure.data.json :as json]
   [solpred.util.file :as file]
   [solpred.util.runner :as runner]
   [solpred.util.external :as extern]
   [solpred.util.runtime-check :as runtime-check]
   [solpred.reports.metrics :as metrics]
   [tech.v3.dataset :as ds]
   [tablecloth.api :as tc])
  (:import
   (org.apache.commons.math3.stat.descriptive DescriptiveStatistics)))

(defn make-detail-dataset
  #_(make-detail-dataset "/work/processed-runs/blackmountain/categories/All/edn")
  #_(make-detail-dataset "/work/processed-runs/blackmountain/case-studies/5-feb-rolling/edn")
  "Make a detail dataset from edn files
  Read edn files so that we read from cache instead of having to wait for metrics to calculate"
  [in-dir]
  (let [data 
        (->> (file/list-children in-dir)
             (map (fn [x] (edn/read-string (slurp (str x)))))
             (map (fn [x]
                    {:model-id (:model-id x)
                     :run-id (:run-id x)
                     :fold-id (:fold-id x)
                     :crop-size (:crop-size x)
                     :learn-rate (:learn-rate x)
                     :input-terms (:input-terms x)
                     :input-spacing (:input-spacing x)
                     :horizon (:horizon x)
                     :test-data (or (:category-name x) (:case-study-id x))
                     :persist-rmse (get-in x [:persist-metrics :rmse])
                     :rmse (get-in x [:model-metrics :rmse])
                     :mae (get-in x [:model-metrics :mae])
                     :rmse-skill (get-in x [:model-skills :rmse-skill])
                     :mae-skill (get-in x [:model-skills :mae-skill])
                     :r2 (get-in x [:model-metrics :r2])
                     :rdi (double (get-in x [:ramp-metrics :rdi]))
                     :rdi-ramps (get-in x [:ramp-metrics :rdi-ramps])
                     :fri (double (get-in x [:ramp-metrics :fri]))
                     :fri-no-ramps (get-in x [:ramp-metrics :fri-no-ramps])
                     :epochs (:epochs x)})))
        data-with-null
        (->> data
             (map #(reduce-kv (fn [acc k v] (conj acc v)) [] %))
             (map #(filter (fn [x] (nil? x)) %))
             (filter #(seq %))
             )
        ]
    (runtime-check/throw-on-false (empty? data-with-null) (str "Found null when processing " in-dir))
    (ds/->dataset data {:dataset-name in-dir})) 
  )

(defn format-detail-dataset
  #_(format-detail-dataset (make-detail-dataset "/work/processed-runs/blackmountain/categories/All/edn"))
  #_(format-detail-dataset (make-detail-dataset "/work/processed-runs/blackmountain/case-studies/19-dec-high/edn"))
  "Sort a detail dataset"
  [dataset]
  (-> dataset
      (tc/reorder-columns [:model-id :run-id :crop-size :input-terms :input-spacing :horizon])
      (tc/order-by [:model-id :run-id :crop-size :horizon :input-terms :input-spacing])
      (tc/update-columns #{:rmse-skill :mae-skill :rdi :fri}
                         (partial map #(format "%.1f%%" (* 100 %))))
      (tc/update-columns #{:persist-rmse :rmse :mae :r2}
                         (partial map #(format "%.2f" %)))
      ) 
  )

(defn metric-edn->ensemble-metrics
  [metric-edn]
  (let [ensemble-metric-file (file/resolve-path [(file/parent (file/parent (:metric-file metric-edn)))
                                                 "ensemble"
                                                 "metrics"
                                                 (str (:grouping-description-string metric-edn) ".edn")])]
    (runtime-check/throw-on-false (file/exists? ensemble-metric-file)
                                  (str "Can't find ensemble file " ensemble-metric-file " from " metric-edn))
    (edn/read-string (slurp (str ensemble-metric-file)))))

(defn group-run-folds
  #_(group-run-folds "/work/processed-runs/blackmountain-round/All/edn")
  "Get groups containing all the folds of each run
  Read edn files so that we read from cache instead of having to wait for metrics to calculate"
  [in-dir]
  (->> (file/list-children in-dir)
       (map (fn [x] (edn/read-string (slurp (str x)))))
       (group-by :grouping-description-string)
       (map (fn [[_ v]] v))))

(defn calc-stat-values
  #_(calc-stat-values [{:model-id "test" :model-metrics {:mae 1 :rmse 1}}])
  "Calculate statistics for a run e.g. the median rmse of all the folds"
  [folds]
  (let [rmse-stats (DescriptiveStatistics.)
        mae-stats (DescriptiveStatistics.)
        r2-stats (DescriptiveStatistics.)]
    (run! (fn [fold]
            (.addValue rmse-stats (get-in fold [:model-metrics :rmse]))
            (.addValue mae-stats (get-in fold [:model-metrics :mae]))
            (.addValue r2-stats (get-in fold [:model-metrics :r2])))
          folds)
    (let [rmse {:median (.getPercentile rmse-stats 50)
                :max (.getMax rmse-stats)
                :min (.getMin rmse-stats)}
          mae {:median (.getPercentile mae-stats 50)
               :max (.getMax mae-stats)
               :min (.getMin mae-stats)}
          r2 {:median (.getPercentile r2-stats 50)
               :max (.getMax r2-stats)
               :min (.getMin r2-stats)}
          rmse-skill (reduce-kv (fn [acc k v]
                                  (assoc acc k (metrics/get-skill (get-in (first folds) [:persist-metrics :rmse]) v)))
                                {} rmse)]
      (assoc (first folds)
             :folds (count folds)
             :rmse rmse
             :rmse-skill rmse-skill
             :mae mae
             :r2 r2))))

(defn make-median-dataset
  #_(make-median-dataset "/work/processed-runs/blackmountain/categories/All/edn")
  "Make statistical data summary"
  [in-dir]
  (let [data (->> (group-run-folds in-dir)
                  (map calc-stat-values)
                  (map (fn [x]
                         (let [ensemble (metric-edn->ensemble-metrics x)]
                           {:model-id (:model-id x)
                            :run-id (:run-id x)
                            :folds (:folds x)
                            :crop-size (:crop-size x)
                            :learn-rate (:learn-rate x)
                            :input-terms (:input-terms x)
                            :input-spacing (:input-spacing x)
                            :horizon (:horizon x)
                            :test-data (or (:category-name x) (:case-study-id x))
                            :persist-rmse (get-in x [:persist-metrics :rmse])
                            :rmse-median (get-in x [:rmse :median])
                            :rmse-min (get-in x [:rmse :min])
                            :r2-median (get-in x [:r2 :median])
                            :rmse-skill-median (get-in x [:rmse-skill :median])
                            :within-confidence-interval (get-in ensemble [:ensemble-metrics :fraction-inside])
                            :ensemble-rmse (get-in ensemble [:model-metrics :rmse])
                            :ensemble-skill (get-in ensemble [:model-skills :rmse-skill])
                            }))))]
    (ds/->dataset data)
    ))

(defn format-median-dataset
  #_(format-median-dataset (make-median-dataset "/work/processed-runs/blackmountain/categories/All/edn"))
  "Sort a median dataset"
  [dataset]
  (-> dataset
      (tc/reorder-columns [:model-id :run-id :crop-size :input-terms :input-spacing :horizon
                           :rmse-median :rmse-min :r2-median :within-confidence-interval :test-data])
      (tc/order-by [:model-id :run-id :crop-size :horizon :input-terms :input-spacing])
      (tc/update-columns #{:rmse-skill-median :within-confidence-interval :ensemble-skill}
                         (partial map #(format "%.1f%%" (* 100 %))))
      (tc/update-columns #{:persist-rmse :rmse-median :rmse-min :r2-median :ensemble-rmse}
                         (partial map #(format "%.2f" %)))
      ))

(defn get-category-edn-metrics-folder
  #_(get-category-edn-metrics-folder "/work/processed-runs")
  "Find folders containing edn metrics inside the category sub-dir"
  [base-dir]
  (->> (file/walk base-dir)
       (filter file/dir?)
       (filter #(= "edn" (file/filename %)))
       (filter #(= "categories" (first (take-last 3 (file/as-seq %)))))))

(defn get-case-study-edn-metrics-folder
  #_(get-case-study-edn-metrics-folder "/work/processed-runs")
  "Find folders containing edn metrics inside the case-study sub-dir"
  [base-dir]
  (->> (file/walk base-dir)
       (filter file/dir?)
       (filter #(= "edn" (file/filename %)))
       (filter #(= "case-studies" (first (take-last 3 (file/as-seq %)))))))

(defn edn-metrics->detail-csv-files!
  #_(edn-metrics->detail-csv-files! "/work/processed-runs")
  "Combine edn files into csv files - one line per file"
  [base-dir]
  (let [folders (concat (get-category-edn-metrics-folder base-dir) (get-case-study-edn-metrics-folder base-dir))
        jobs (->> folders
                  (map (fn [folder-path]
                         #(let [csv-file (file/resolve-path [(file/parent folder-path) "details.csv"])
                                data (format-detail-dataset (make-detail-dataset folder-path))]
                            (ds/write! data csv-file))
                         )))]
    (runner/run-jobs! {:jobs jobs})))

(defn edn-metrics->statistic-csv-files!
  #_(edn-metrics->statistic-csv-files! "/work/processed-runs")
  "combine edn file into csv files - statistical summary"
  [base-dir]
  (let [folders (concat (get-category-edn-metrics-folder base-dir) (get-case-study-edn-metrics-folder base-dir))
        jobs (->> folders
                  (map (fn [folder-path]
                         #(let [csv-file (file/resolve-path [(file/parent folder-path) "stats.csv"])
                                data (format-median-dataset (make-median-dataset folder-path))]
                            (ds/write! data csv-file))
                         )))]
    (runner/run-jobs! {:jobs jobs})))

(defn edn-metrics->static-plots!
  #_(edn-metrics->static-plots! "/work/processed-runs")
  "make static plots"
  [base-dir]
  (let [runs
        (->> (concat (get-category-edn-metrics-folder base-dir) (get-case-study-edn-metrics-folder base-dir))
             (mapcat (fn [folder] (->> (file/list-children folder)
                                       (filter #(file/file? %))
                                       (map #(assoc (edn/read-string (slurp (str %)))
                                                    :plot-dir (file/resolve-path [(file/parent folder) "plots"])))
                                       (map #(assoc %
                                                    :plot-path (file/resolve-path [(:plot-dir %) (str (:run-description-string %) "_rmse="  (format "%.2f" (get-in % [:model-metrics :rmse])) ".png")])))))))
        group
        (group-by #(if (file/exists? (:plot-path %)) :skip :process) runs)
        skip-jobs
        (->> (:skip group)
             (map (fn [{:keys [plot-path]}] #(log/debug (str "Skipping " plot-path " because it exists")))))
        proc-jobs
        (->> (:process group)
             (map (fn [{:keys [plot-path run-description-string metric-dataset-path tmp-dir] :as run}]
                    #(let [python-instr-file (file/resolve-path [tmp-dir (str run-description-string "_" (rand-int 1000) ".json")])
                           python-args
                           {:title (str run-description-string "_rmse="  (format "%.2f" (get-in run [:model-metrics :rmse])))
                            :out-dir (file/parent plot-path)
                            :path metric-dataset-path}]
                       (runtime-check/map-contains? run [:plot-path :run-description-string :metric-dataset-path :tmp-dir])
                       (log/debug (str "Making " plot-path))
                       (file/make-dirs (file/parent plot-path))
                       (file/make-dirs tmp-dir)
                       (spit python-instr-file (json/write-str python-args))
                       (-> (extern/make-executor ["python3" "/app/src/plotting/json-to-static.py" python-instr-file])
                           (.exitValue (int 0))
                           (.directory (io/file "/app/src/plotting/"))
                           (.execute))
                       (file/delete python-instr-file)
                       ))))]
   (runner/run-jobs! {:jobs (into skip-jobs proc-jobs)})))

(defn main
  #_(main "/work/processed-runs")
  "main method of the ns"
  [base-dir]
  (edn-metrics->detail-csv-files! base-dir)
  (edn-metrics->statistic-csv-files! base-dir)
  (edn-metrics->static-plots! base-dir))

(comment
  (for
      [base-dir ["/work/processed-runs/blackmountain/wide"]
       fold (range 0 10)]
    (let [name (str "blackmountain_fully-conv_4x120s_420s_run_02_crop_full_lr_3.0E-6_fold_" fold "_wide.parquet")
          path (file/resolve-path [base-dir name])
          count (if (file/exists? path)
                  (tc/row-count (ds/->dataset path))
                  0)]
      {:name name
       :count count}
      )
    )
  )
