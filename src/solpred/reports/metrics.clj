(ns solpred.reports.metrics
  (:require
   [clojure.pprint :as pprint]
   [clojure.string :as str]
   [clojure.tools.logging :as log]
   [clojure.math :as math]
   [clojure.edn :as edn]
   [solpred.util.runtime-check :as runtime-check]
   [solpred.util.time :as time]
   [solpred.util.file :as file]
   [tablecloth.api :as tc]
   [tech.v3.dataset :as ds]
   [tech.v3.libs.parquet :as parquet])
  (:import
   (org.apache.commons.math3.stat.descriptive DescriptiveStatistics)))

(defn square
  #_(square -4)
  "Square a number"
  [number]
  (* number number))

(defn apache-mae
  #_(apache-mae [1 2 3 4] [1 2 7 8])
  "Calculate MAE with Apache stats"
  [target-seq predicted-seq]
  (let [stats (DescriptiveStatistics.)
        combined (map vector target-seq predicted-seq)]
    (->> combined
         (map (fn [[target predicted]] (abs (- predicted target))))
         (run! (fn [average-error] (.addValue stats average-error))))
    (.getMean stats)))

(defn apache-mse
  #_(apache-mse [1 2 3 4] [1 2 7 8])
  "Calculate MSE with Apache stats"
  [target-seq predicted-seq]
  (let [stats (DescriptiveStatistics.)
        combined (map vector target-seq predicted-seq)]
    (->> combined
         (map (fn [[target predicted]] (square (- predicted target))))
         (run! (fn [squared-error] (.addValue stats squared-error))))
    (.getMean stats)))

(defn apache-r2
  #_(apache-r2 [1 2 3 4] [1 2 7 8])
  "Calculate R2 (coefficient of determination) with Apache stats"
  [target-seq predicted-seq]
  (let [stats (DescriptiveStatistics.)
        mean-of-observed (do (run! (fn [target] (.addValue stats target)) target-seq)
                             (.getMean stats))
        combined (map vector target-seq predicted-seq)
        residual-sum-of-squares (->> combined
                                     (map (fn [[target predicted]] (square (- predicted target))))
                                     (reduce +))
        total-sum-of-squares (->> target-seq
                                  (map (fn [target] (square (- target mean-of-observed))))
                                  (reduce +))]
    (- 1 (/ residual-sum-of-squares total-sum-of-squares))))



(defn get-mae
  "redirect mae"
  [target predicted]
  #_(metrics/mean_absolute_error target predicted)
  (apache-mae target predicted))

(defn get-mse
  "redirect mse"
  [target predicted]
  #_(metrics/mean_squared_error target predicted)
  (apache-mse target predicted))

(defn get-rmse
  "redirect rmse"
  [target predicted]
  (math/sqrt (get-mse target predicted)))

(defn get-r2
  "redirect r2"
  [target predicted]
  #_(metrics/r2_score target predicted)
  (apache-r2 target predicted))

(defn get-skill
  #_(get-skill 20.1 15.8)
  "Calculate the skill vs a reference metric (e.g. skill vs persistence)"
  [reference-metric forecast-metric]
  (- 1 (/ forecast-metric reference-metric)))

(defn get-skills
  #_(get-skills {:mae 10 :mse 25 :rmse 5} {:mae 7 :mse 21 :rmse 3})
  "Convenience method that gets skill vs reference for mae, mse and rmse"
  [reference-metrics forecast-metrics]
  {:mae-skill (get-skill (:mae reference-metrics) (:mae forecast-metrics))
   :mse-skill (get-skill (:mse reference-metrics) (:mse forecast-metrics))
   :rmse-skill (get-skill (:rmse reference-metrics) (:rmse forecast-metrics))})

(defn get-standard-metrics
  #_(get-metrics [1 2 3] [2 2 2])
  "Calculates metrics"
  [target predicted]
  {:mae (get-mae target predicted)
   :mse (get-mse target predicted)
   :rmse (get-rmse target predicted)
   :r2 (get-r2 target predicted)})

(defn get-standard-metrics-wide-dataset
  "Takes a dataset in wide format, the target series, the predicted series and returns a map containing the metrics"
  #_(get-standard-metrics-wide-dataset (tc/dataset "/work/processed-runs/blackmountain/wide/91x10s_420s_extra-trees_run_01_fold_0_wide.csv.gz") "actual" "extra-trees_91x10s_420s_run_00_fold_0")
  #_(get-standard-metrics-wide-dataset (tc/dataset "/work/processed-runs/blackmountain/wide/91x10s_420s_extra-trees_run_01_fold_0_wide.csv.gz") "actual" "persist_91x10s_420s_run_00_fold_0")
  [data-wide target-col pred-col]
  (let [data-clean (tc/drop-missing (tc/select-columns data-wide #{target-col pred-col}))
        actual (into [] (mapcat identity) (ds/value-reader (tc/select-columns data-clean #{target-col})))
        pred (into [] (mapcat identity) (ds/value-reader (tc/select-columns data-clean #{pred-col})))]
    (get-standard-metrics actual pred)))

(defn ramp-values-chu
  "Caclulate the values of the ramps
  ramps defined as in chu et al.
  difference between start of interval and observation"
  [{:keys [start-irrad end-irrad pred-irrad] :as interval}]
  (runtime-check/map-contains? interval [:start-irrad :end-irrad :pred-irrad])
  {:actual-ramp (- end-irrad start-irrad)
   :pred-ramp (- pred-irrad start-irrad)
   })

(defn calc-rdi-obs
  "RDI (Ramp Detection Index) Observation
  (1) Did a ramp actually occur? (no-ramp)
  (2) If so, was a ramp predicted in the same direction? (hit)
  (3) Else, the ramp was missed (miss)

  RDI is then the number of hits divided by the number of ramps
  RDI=1 all ramps hit, no misses
  RDI=0 no ramps hit, all misses
  i.e. RDI = % of ramps hit
  "
  [{:keys [actual-ramp pred-ramp clr-irrad] :as interval}]
  (runtime-check/map-contains? interval [:actual-ramp :pred-ramp :clr-irrad])
  (let [actual-mag (abs actual-ramp)
        pred-mag (abs pred-ramp)
        clr-threshold (* 0.1 clr-irrad)
        same-sign (< 0 (* actual-ramp pred-ramp))]
   (cond
     (< actual-mag clr-threshold) :no-ramp
     (and same-sign (> pred-mag clr-threshold)) :hit
     :else :miss)))

(defn calc-rdi
  "RDI (Ramp Detection Index): the number of hits divided by the number of ramps"
  [ramps]
  (let [groups (group-by :rdi-obs ramps)
        hit-count (count (:hit groups))
        miss-count (count (:miss groups))
        ramp-count (+ hit-count miss-count)]
    {:rdi-hits hit-count
     :rdi-ramps ramp-count
     :rdi (if (zero? ramp-count)
            1
            (/ hit-count ramp-count))}))

(defn calc-fri-obs
  "FRI (False Ramp Index) Observation
  (1) Did no ramp actually occur (ramp)
  (2) If so, was a false ramp prediction made (false-ramp)
  (3) Else, it's a true no-ramp prediction (true-no-ramp)

  FRI is then the number of false ramp predictions divided by the number of no-ramps
  FRI=1 all false ramps, no true no ramps
  FRI=0 no false ramps, all true no ramps
  i.e. FRI = % of false ramps when no ramp happening
  "
  [{:keys [actual-ramp pred-ramp clr-irrad] :as interval}]
  (runtime-check/map-contains? interval [:actual-ramp :pred-ramp :clr-irrad])
   (let [actual-mag (abs actual-ramp)
        pred-mag (abs pred-ramp)
        clr-threshold (* 0.1 clr-irrad)]
   (cond
     (> actual-mag clr-threshold) :ramp
     (> pred-mag clr-threshold) :false-ramp
     :else :true-no-ramp)))

(defn calc-fri
  "FRI (False Ramp Index): the number of false ramp predictions divided by the number of no-ramps"
  [ramps]
  (let [groups (group-by :fri-obs ramps)
        false-ramp-count (count (:false-ramp groups))
        true-no-ramp-count (count (:true-no-ramp groups))
        no-ramp-count (+ false-ramp-count true-no-ramp-count)]
    {:fri-false-ramps false-ramp-count
     :fri-no-ramps no-ramp-count
     :fri (if (zero? no-ramp-count)
            0
            (/ false-ramp-count no-ramp-count))}))

(defn ramp-classifications
  #_(ramp-classifications {:start-irrad 1000 :end-irrad 1001 :pred-irrad 800 :clr-irrad 1000})
  #_(ramp-classifications {:start-irrad 1000 :end-irrad 1001 :pred-irrad 950 :clr-irrad 1000})
  #_(ramp-classifications {:start-irrad 1000 :end-irrad 800 :pred-irrad 800 :clr-irrad 1000})
  #_(ramp-classifications {:start-irrad 1000 :end-irrad 800 :pred-irrad 1000 :clr-irrad 1000})
  "Ramp classification metrics from chu et al."
  [interval]
  (let [values (ramp-values-chu interval)]
    (merge
     values
     {:rdi-obs (calc-rdi-obs (merge interval values))
      :fri-obs (calc-fri-obs (merge interval values))})))

(defn get-ramp-metrics-wide-dataset
  "Takes a dataset in wide format, the target series, the predicted series and returns a map containing the metrics"
  #_(get-ramp-metrics-wide-dataset
     (tc/inner-join
      (ds/->dataset "/work/processed-runs/blackmountain/wide/blackmountain_fully-conv_2x20s_420s_run_02_crop_full_lr_3.0E-6_fold_0_wide.parquet")
      (ds/->dataset "/work/processed-runs/blackmountain/clear-sky-irradiance-cache.parquet")
      "id-time")
     "actual"
     "persist_2x20s_420s_run_02_fold_0"
     "fully-conv_2x20s_420s_run_02_fold_0")
  [data-wide target-col persist-col pred-col]
  (let [data-clean (tc/rename-columns
                    (tc/drop-missing data-wide)
                    {target-col :end-irrad
                     persist-col :start-irrad
                     pred-col :pred-irrad
                     "ClearSkyGHI" :clr-irrad})
        ramps (map ramp-classifications (ds/mapseq-reader data-clean))]
    (merge (calc-rdi ramps) (calc-fri ramps))))

(defn get-empty-ramp-metrics
  "Returns empty ramp metrics for datasets where calculating ramp metrics is not possible"
  []
  {:rdi-hits 0
   :rdi-ramps 0
   :rdi 0
   :fri-false-ramps 0
   :fri-no-ramps 0
   :fri 0})

(defn get-category-dates
  #_(get-category-dates "Intermittent" {})
  "Get the dates for a category. Returns a set."
  [category {:keys [mapping-file category-col date-col]
             :or {mapping-file "resources/datasets/blackmountain-test.csv"
                  category-col "Category"
                  date-col "Date"}}]
  (->> (tc/dataset mapping-file)
       (ds/mapseq-reader)
       (filter (fn [x] (= category (get x category-col))))
       (map (fn [x] (get x date-col)))
       set))

(defn select-data-from-category
  "Selects data from the dataset that belongs to the category"
  [{:keys [wide-dataset-path category-name category-file]}]
  (let [dates (get-category-dates category-name {:mapping-file category-file})]
    (tc/select-rows (ds/->dataset (str wide-dataset-path))
                    (fn [row]
                      (contains? dates (time/string->localdate (get row "id-time") "yyyy-MM-dd_HH-mm-ss"))))))

(defn get-data-from-category
  [{:keys [wide-dataset-path category-name] :as run}]
  (runtime-check/map-contains? run [:wide-dataset-path :category-name])
  (if (not= category-name "All")
    (select-data-from-category run)
    (ds/->dataset (str wide-dataset-path))))

(defn select-case-study-data
  #_(->> (read-run-summaries "/work/processed-runs")
         (mapcat (fn [{:keys [case-studies] :as run}]
                   (map #(merge run %) case-studies)))
         first
         get-case-study-data
         tc/row-count)
  [{:keys [wide-dataset-path case-study-start-dt case-study-end-dt] :as run}]
  (runtime-check/map-contains? run [:wide-dataset-path :case-study-start-dt :case-study-end-dt])
  #_(log/debug (:case-study-id run))
  (tc/select-rows (ds/->dataset (str wide-dataset-path))
                  (fn [row]
                    (time/in-order? (time/string->datetime case-study-start-dt "yyyy-MM-dd_HH-mm-ss")
                                    (time/string->datetime (get row "predicted-time") "yyyy-MM-dd_HH-mm-ss")
                                    (time/string->datetime case-study-end-dt "yyyy-MM-dd_HH-mm-ss")))))

(defn get-case-study-data
  #_(get-case-study-data
     (assoc
      (edn/read-string
       (slurp "/work/processed-runs/blackmountain/case-studies/5-feb-rolling/edn/blackmountain_fully-conv_1x10s_120s_run_03_crop_1024_lr_3.0E-6_fold_0_metrics.edn"))
      :case-study-id "All"
      ))
  #_(get-case-study-data
     (assoc
      (edn/read-string
       (slurp "/work/processed-runs/blackmountain/case-studies/5-feb-rolling/edn/blackmountain_fully-conv_2x60s_120s_run_99_crop_1024_lr_3.0E-6_fold_5_metrics.edn"))
      :case-study-id "All"
      ))
  [{:keys [case-study-id] :as run}]
  (runtime-check/map-contains? run [:case-study-id])
  (if (not= case-study-id "All")
    (select-case-study-data run)
    (if-let
        [result (transduce
                 (comp
                  (mapcat (fn [{:keys [case-studies] :as r}] (map #(merge r %) case-studies)))
                  (filter #(not= (:case-study-id %) "All"))
                  (map select-case-study-data)
                  )
                 tc/concat
                 (tc/dataset) ;;initial value for reduction
                 [run])]
      result
      (tc/dataset nil {:column-names [:id-time :actual]}))
    ))

(defn calculate-standard-metrics
  #_(calculate-standard-metrics {:model-col "" :persist-col ""} (ds/->dataset ""))
  "Calculate standard metrics for a run"
  [{:keys [model-col persist-col] :as run} data]
  (let [model-metrics (get-standard-metrics-wide-dataset data "actual" model-col)
        persist-metrics (get-standard-metrics-wide-dataset data "actual" persist-col)
        model-skill (get-skills persist-metrics model-metrics)]
    {:model-metrics model-metrics
     :persist-metrics persist-metrics
     :model-skills model-skill}))

(defn return-empty-standard-metrics
  []
  {:model-metrics {:mae ##NaN :mse ##NaN :rmse ##NaN :r2 ##NaN}
   :persist-metrics {:mae ##NaN :mse ##NaN :rmse ##NaN :r2 ##NaN}
   :model-skills {:mae-skill ##NaN :mse-skill ##NaN :rmse-skill ##NaN}})

(defn ensemble-metrics
  #_(ensemble-metrics (edn/read-string (slurp "/work/processed-runs/blackmountain/case-studies/5-feb-rolling/ensemble/metadata/blackmountain_fully-conv_2x30s_120s_run_03_crop_1024_lr_3.0E-6.edn")))
  [metadata]
  (let [upper-col (get-in metadata [:cols :upper-col])
        lower-col (get-in metadata [:cols :lower-col])
        data (ds/->dataset (get-in metadata [:paths :data-path]))
        stats
        (reduce (fn [acc curr]
                  (let [actual (get curr "actual")
                        upper (get curr upper-col)
                        lower (get curr lower-col)]
                    (if (< lower actual upper)
                      (assoc acc :inside (inc (:inside acc)))
                      (assoc acc :outside (inc (:outside acc))))))
                {:inside 0 :outside 0} (ds/mapseq-reader data))]
    (merge
     {:ensemble-metrics (assoc stats :fraction-inside (double (/ (:inside stats) (+ (:inside stats) (:outside stats)))))}
     (calculate-standard-metrics (:cols metadata) data))))

(defn calculate-ramp-metrics
  #_(calculate-ramp-metrics {:model-col "" :persist-col ""} (ds/->dataset ""))
  "Calculate ramp metrics for a run"
  [{:keys [clear-sky-irradiance-cache model-col persist-col] :as run} data]
  (runtime-check/map-contains? run [:clear-sky-irradiance-cache :model-col :persist-col])
  (runtime-check/throw-on-false (contains? (set (tc/column-names data)) "id-time")
                                (str "id-time column not found in data"))
  (if (file/exists? clear-sky-irradiance-cache)
    (let [clear-sky-irrad-data (ds/->dataset clear-sky-irradiance-cache)
          combined (tc/inner-join data clear-sky-irrad-data "id-time")]
      {:ramp-metrics (get-ramp-metrics-wide-dataset combined "actual" persist-col model-col)})
    {:ramp-metrics (get-empty-ramp-metrics)}
    ))

(defn return-empty-ramp-metrics
  []
  {:rdi-hits ##NaN
   :rdi-ramps ##NaN
   :rdi ##NaN
   :fri-false-ramps ##NaN
   :fri-no-ramps ##NaN
   :fri ##NaN})


(defn calculate-metrics
  #_(calculate-standard-metrics {:wide-dataset-path "" :category-name "All" :model-col "" :persist-col ""} (ds/->dataset ""))
  "Calculate the metrics for a run"
  [{:keys [wide-dataset-path category-name model-col persist-col] :as run} data]
  (let [cols (set (tc/column-names data))]
    (runtime-check/throw-on-false (contains? cols model-col) (str model-col " not found in " wide-dataset-path))
    (runtime-check/throw-on-false (contains? cols persist-col) (str persist-col " not found in " wide-dataset-path))
    (if (= 0 (tc/row-count data))
      (do
        (log/warn (str wide-dataset-path " is empty for category: " category-name " \n Dataset is: " data))
        (merge (return-empty-standard-metrics) (return-empty-ramp-metrics)))
      (merge (calculate-standard-metrics run data) (calculate-ramp-metrics run data)))))

(comment

  (ds/->dataset "/work/processed-runs/blackmountain/wide/blackmountain_fully-conv_2x60s_120s_run_99_crop_1024_lr_3.0E-6_fold_9_wide.parquet")

  (let [ramps
        (get-ramp-metrics-wide-dataset
         (tc/inner-join
          (ds/->dataset "/work/processed-runs/blackmountain-round/wide/blackmountain-round_fully-conv_16x60s_420s_run_02_crop_768_lr_3.0E-6_fold_0_wide.parquet")
          (ds/->dataset "/work/processed-runs/blackmountain-round/clear-sky-irradiance-cache.parquet")
          "id-time")
         "actual"
         "persist_16x60s_420s_run_02_fold_0"
         "fully-conv_16x60s_420s_run_02_fold_0")]
    (calc-rdi ramps)
    )
   )
