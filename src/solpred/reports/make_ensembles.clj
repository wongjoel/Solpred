(ns solpred.reports.make-ensembles
  "Takes runs with multiple folds and makes an ensemble out of the multiple folds"
  (:require
   [clojure.string :as str]
   [clojure.tools.logging :as log]
   [clojure.edn :as edn]
   [clojure.math :as math]
   [clojure.set :as set]
   [clojure.java.io :as io]
   [clojure.data.json :as json]
   [solpred.reports.metrics :as metrics]
   [solpred.util.pprint-edn :as edn-out]
   [solpred.util.file :as file]
   [solpred.util.external :as extern]
   [solpred.util.runtime-check :as runtime-check]
   [solpred.util.runner :as runner]
   [tablecloth.api :as tc]
   [tech.v3.dataset :as ds]
   [tech.v3.libs.parquet :as parquet])
  (:import
   (org.apache.commons.math3.stat.descriptive DescriptiveStatistics)))

(defn group-run-folds
  #_(group-run-folds "/work/processed-runs/blackmountain-round/All/edn")
  "Get groups containing all the folds of each run
  Read edn files so that we read from cache instead of having to wait for metrics to calculate"
  [in-dir]
  (->> (file/list-children in-dir)
       (map #(edn/read-string (slurp (str %))))
       (map #(assoc % :group-dir in-dir))
       (group-by :grouping-description-string)
       (map (fn [[_ v]] v))
       )
  )

(defn find-ensembles
  #_(find-ensembles "/work/processed-runs/" "categories")
  #_(find-ensembles "/work/processed-runs/" "case-studies")
  [base-dir subdir]
  (let [dataset-folders (file/list-children base-dir)
        category-folders (->> dataset-folders
                              (map #(file/resolve-path [% subdir]))
                              (filter #(file/exists? %))
                              (mapcat file/list-children)
                              (map #(file/resolve-path [% "edn"])))
        groupings (mapcat group-run-folds category-folders)]
    groupings)
  )

(defn calc-ensemble-metadata
  #_(calc-ensemble-metadata "/work/processed-runs/")
  [base-dir]
  (->> (concat (find-ensembles base-dir "categories") (find-ensembles base-dir "case-studies"))
       (map (fn [ensemble]
              (let [x (first ensemble)
                    ensemble-name (:grouping-description-string x)
                    ensemble-base-path (file/resolve-path [(file/parent (:group-dir x)) "ensemble"])]
                (merge
                 (select-keys x
                              [:clear-sky-irradiance-cache
                               :dataset-id
                               :horizon
                               :crop-size
                               :input-spacing
                               :input-terms
                               :learn-rate
                               :run-id
                               :category-name
                               :case-study-id
                               :tmp-dir])
                 {:ensemble-name ensemble-name
                  :paths {:base-path ensemble-base-path
                          :metadata-path (file/resolve-path [ensemble-base-path "metadata" (str ensemble-name ".edn")])
                          :data-path (file/resolve-path [ensemble-base-path "wide" (str ensemble-name ".parquet")])
                          :metric-path (file/resolve-path [ensemble-base-path "metrics" (str ensemble-name ".edn")])
                          :plot-path (file/resolve-path [ensemble-base-path "plots" (str ensemble-name ".html")])}
                  :cols {:persist-col (str "persist_" (:inout-config x) "_run_" (:run-id x))
                         :model-col ensemble-name
                         :upper-col (str ensemble-name "_upper")
                         :lower-col (str ensemble-name "_lower")}
                  :ensemble-data
                  (->> ensemble
                       (map #(select-keys % [:metric-dataset-path
                                             :model-col
                                             :persist-col]))
                       (map #(set/rename-keys % {:metric-dataset-path :ensemble-dataset-path})))
                  }))))
       ))

(defn write-ensemble-metadata!
  #_(write-ensemble-metadata! "/work/processed-runs/")
  [base-dir]
  (let [jobs (->> (calc-ensemble-metadata base-dir)
                  (map (fn [ensemble]
                         #(let [ensemble-metadata-path (get-in ensemble [:paths :metadata-path])]
                            (file/make-dirs (file/parent ensemble-metadata-path))
                            (edn-out/spit-pprint ensemble ensemble-metadata-path)))))]
    (runner/run-jobs! {:jobs jobs})))

(defn find-ensemble-metadata-files
  #_(find-ensemble-metadata-files "/work/processed-runs" "categories")
  [base-dir subdir]
  (->> (file/list-children base-dir)
       (map #(file/resolve-path [% subdir]))
       (filter #(file/exists? %))
       (mapcat file/list-children)
       (map #(file/resolve-path [% "ensemble" "metadata"]))
       (mapcat file/list-children)
       (map str))
  )

(defn read-ensemble-metadata
  #_(read-ensemble-metadata "/work/processed-runs")
  [base-dir]
  (->> (concat (find-ensemble-metadata-files base-dir "categories") (find-ensemble-metadata-files base-dir "case-studies"))
       (map #(edn/read-string (slurp (str %))))))

(defn reduce-join-models
  #_(reduce reduce-join-models [{:data (ds/->dataset "data.parquet")} {:data (ds/->dataset "data.parquet")}])
  [left right]
  (let [ds1 (:data left)
        ds2 (tc/select-columns (:data right) #{"id-time" (:model-col right)})
        joined (tc/inner-join ds1 ds2 "id-time")
        right-cols (set (tc/column-names (:data right)))]
    (runtime-check/throw-on-false
     (contains? right-cols (:model-col right)) (str "data is not in expected format: wrong col name " (:model-col right) " vs " right-cols))
    (runtime-check/throw-on-false
     (= (tc/row-count joined) (tc/row-count ds1) (tc/row-count ds2))
     (str "Datasets don't have matching number of rows: "
          "\n" (tc/dataset-name ds1) " " (tc/row-count ds1)
          "\n" (tc/dataset-name ds2) " " (tc/row-count ds2)))
    {:data joined
     :col-names (conj (:col-names left) (:model-col right))
     :persist-col (or (:persist-col left) (:persist-col right))}
    ))

(defn combine-models
  #_(combine-models (for [fold (range 1)]
              {:ensemble-dataset-path (str "/work/processed-runs/blackmountain/wide/blackmountain_fully-conv_2x20s_420s_run_02_crop_full_lr_3.0E-6_fold_" fold "_wide.parquet")
               :model-col(str "fully-conv_2x20s_420s_run_02_fold_" fold)
               :persist-col (str "persist_2x20s_420s_run_02_fold_" fold)
               }))
  [datasets]
  (->> datasets
       (map #(assoc % :data (ds/->>dataset (:ensemble-dataset-path %))))
       (map #(assoc % :data (tc/drop-missing (:data %))))
       (map #(assoc % :col-names [(:model-col %)]))
       (sort-by :model-col)
       (reduce reduce-join-models)
       ))

(defn calc-confidence-interval-t
  #_(calc-confidence-interval-t [1 2 3 4])
  [samples]
  (if (= 1 (count samples))
    {:upper (first samples)
     :mean (first samples)
     :lower (first samples)}
    (let [stats (DescriptiveStatistics.)
          mean-of-observed (do (run! (fn [target] (.addValue stats target)) samples)
                               (.getMean stats))
          sample-std-dev (.getStandardDeviation stats)
          num-samples (.getN stats)
          t-crit-table [12.71 4.303 3.182 2.776 2.571 2.447 2.365 2.306 2.262 2.228]
          t-crit (nth t-crit-table (- num-samples 2)) ;-1 for deg of freedom, -1 for 0 based indexing
          ]
      {:upper (+ mean-of-observed (* t-crit (/ sample-std-dev (math/sqrt num-samples))))
       :mean mean-of-observed
       :lower (- mean-of-observed (* t-crit (/ sample-std-dev (math/sqrt num-samples))))}))
  )

(defn ensemble-models
  #_(ensemble-models {:ensemble-name "abc"
                      :persist-col "def"
                      :ensemble-data (for [fold (range 10)]
                                       {:ensemble-dataset-path (str "/work/processed-runs/blackmountain/wide/blackmountain_fully-conv_2x30s_120s_run_03_crop_1024_lr_3.0E-6_fold_" fold "_wide.parquet")
                                        :model-col (str "fully-conv_2x30s_120s_run_03_fold_" fold)
                                        :persist-col (str "persist_2x30s_120s_run_03_fold_" fold)
                                        })})
  [{:keys [ensemble-data ensemble-name cols]}]
  (let [combined (combine-models ensemble-data)]
    (->> (ds/mapseq-reader (:data combined))
         (map (fn [row] {:id-time (get row "id-time")
                         :predicted-time (get row "predicted-time")
                         :actual (get row "actual")
                         :persist (get row (:persist-col combined))
                         :vals (map #(get row %) (:col-names combined))}))
         (map #(merge % (calc-confidence-interval-t (:vals %))))
         (map #(dissoc % :vals))
         (map #(set/rename-keys %
                                {:upper (:upper-col cols)
                                 :mean (:model-col cols)
                                 :lower (:lower-col cols)
                                 :persist (:persist-col cols)})))
    ))

(defn write-ensemble-data!
  #_(write-ensemble-data! "/work/processed-runs")
  [base-dir]
  (let [group
        (group-by #(if (file/exists? (get-in % [:paths :data-path]))
                     :skip :process)
                  (read-ensemble-metadata base-dir))
        skip-jobs
        (->> (:skip group)
             (map (fn [x] #(log/debug (str "Skipping " (get-in x [:paths :data-path]) " because it exists")))))
        proc-jobs
        (->> (:process group)
             (map (fn [{:keys [paths ensemble-name] :as ensemble}]
                    #(let [ensemble-mapseq (ensemble-models ensemble)
                           ensemble-ds (ds/->dataset ensemble-mapseq)
                           ensemble-data-path (:data-path paths)]
                       (log/debug (str "Making " ensemble-data-path))
                       (runtime-check/throw-on-false (= 0 (tc/row-count (tc/select-missing ensemble-ds)))
                                                     (str "Ensemble has empty rows: " ensemble-name))
                       (file/make-dirs (file/parent ensemble-data-path))
                       (parquet/ds->parquet ensemble-ds
                                            ensemble-data-path
                                            {:compression-codec :gzip}))))
             )]
    (runner/run-jobs! {:jobs (into proc-jobs skip-jobs)}))
  )

(defn write-ensemble-metrics!
  #_(write-ensemble-metrics! "/work/processed-runs")
  [base-dir]
  (let [group
        (group-by #(if (file/exists? (get-in % [:paths :metric-path]))
                     :skip :process)
                  (read-ensemble-metadata base-dir))
        skip-jobs
        (->> (:skip group)
             (map (fn [x] #(log/debug (str "Skipping " (get-in x [:paths :metric-path]) " because it exists")))))
        proc-jobs
        (->> (:process group)
             (map (fn [{:keys [paths] :as ensemble}]
                    #(let [ensemble-metrics (merge ensemble (metrics/ensemble-metrics ensemble))
                           ensemble-metrics-path (:metric-path paths)]
                       (log/debug (str "Making " ensemble-metrics-path))
                       (file/make-dirs (file/parent ensemble-metrics-path))
                       (edn-out/spit-pprint ensemble-metrics ensemble-metrics-path)
                       )))
             )]
    (runner/run-jobs! {:jobs (into proc-jobs skip-jobs)}))
  )

(defn ensembles->interact-plots!
  #_(ensembles->interact-plots! "/work/processed-runs")
  "make interactive plots"
  [base-dir]
  (let [group
        (group-by #(if (file/exists? (get-in % [:paths :plot-path]))
                     :skip :process)
                  (read-ensemble-metadata base-dir))
        skip-jobs
        (->> (:skip group)
             (map (fn [{:keys [ensemble-name]}] #(log/debug (str "Skipping plotting " ensemble-name " because it exists")))))
        proc-jobs
        (->> (:process group)
             (map (fn [{:keys [paths ensemble-name tmp-dir] :as ensemble}]
                    #(let [python-instr-file (file/resolve-path [tmp-dir (str ensemble-name "_" (rand-int 1000) ".json")])
                           python-args
                           {:title ensemble-name
                            :out-dir (file/parent (:plot-path paths))
                            :path (:data-path paths)}]
                       (file/make-dirs (file/parent (:plot-path paths)))
                       (file/make-dirs (:tmp-dir ensemble))
                       (spit python-instr-file (json/write-str python-args))
                       (-> (extern/make-executor ["python3" "/app/src/plotting/json-to-interact.py" python-instr-file])
                           (.exitValue (int 0))
                           (.directory (io/file "/app/src/plotting/"))
                           (.execute))
                       (file/delete python-instr-file)
                       ))))]
    (runner/run-jobs! {:jobs (into skip-jobs proc-jobs)})))

(defn main
  #_(main "/work/processed-runs/")
  [base-dir]
  (write-ensemble-metadata! base-dir)
  (write-ensemble-data! base-dir)
  (write-ensemble-metrics! base-dir)
  (ensembles->interact-plots! base-dir)
  )


