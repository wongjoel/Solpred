(ns solpred.reports.process-et
  "Processes the output of a extra trees run, ready for further analysis
  "
  (:require
   [clojure.string :as str]
   [clojure.edn :as edn]
   [clojure.data.csv :as csv]
   [clojure.java.io :as io]
   [solpred.util.file :as file]
   [solpred.util.runner :as runner]
   [solpred.util.time :as time]
   [solpred.reports.metrics :as metrics]
   [tablecloth.api :as tc]
   [clojure.tools.logging :as log]
   [tech.v3.dataset :as ds]
   [tech.v3.libs.parquet :as parquet])
  (:import
   (java.time LocalDate LocalDateTime Duration)
   (java.time.format DateTimeFormatter)))


(defn match-columns
  "Process columns to match convention used in this project"
  [dataset]
  (-> dataset
      (tc/select-columns #{"column-0" "SP_42"})
      (tc/map-columns "predicted-time" #{"column-0"}
                       (fn [row] (let [parsed-time (time/string->datetime row "yyyy-MM-dd HH:mm:ss")]
                                   (time/datetime->string parsed-time "yyyy-MM-dd_HH-mm-ss"))))
      (tc/select-columns #{"predicted-time" "SP_42"})
      (tc/reorder-columns "predicted-time" "SP_42")
      (tc/rename-columns {"SP_42" "extra-trees_91x10s_420s_run_00_fold_0"})))

(defn add-persist-from-other
  "Add in persistence from existing results"
  [dataset other other-model-label other-persist-label]
  (-> dataset
      (tc/left-join other "predicted-time")
      (tc/drop-missing)
      (tc/drop-columns #{other-model-label})
      (tc/drop-columns #(str/includes? % "wide.parquet.predicted-time"))
      (tc/rename-columns {other-persist-label "persist_91x10s_420s_run_00_fold_0"})
      (tc/order-by "predicted-time")
      ))

(defn process-et-data!
  "Get extra-trees output and format it as a run"
  #_(process-et-data!
     "/results/solpred/blackmountain/extra-trees/full/BM_PO_10s-910s/SkyCamRF/SPA/_ET140/(2015-01-01 to 2015-12-31)-PredictionValuesOnTest.csv"
     "/work/processed-runs/blackmountain/wide/blackmountain_extra-trees_91x10s_420s_run_00_crop_full_lr_1_fold_0_wide.parquet")

  #_(process-et-data!
     "/data/results/2021/blackmountain/extra-trees/reduced/BM_PO_10s-910s/SkyCamRF/SPA/_ET140/(2015-01-01 to 2015-12-31)-PredictionValuesOnTest.csv"
     "/work/processed-runs/blackmountain/wide/91x10s_420s_extra-trees_run_01_fold_0_wide.parquet")
  [in-path out-path]
  (-> (ds/->dataset in-path)
      (match-columns)
      (add-persist-from-other (ds/->dataset "/work/processed-runs/blackmountain/wide/blackmountain_fully-conv_2x20s_420s_run_02_crop_full_lr_3.0E-6_fold_0_wide.parquet")
                              "fully-conv_2x20s_420s_run_02_fold_0"
                              "persist_2x20s_420s_run_02_fold_0"
                              )
      (parquet/ds->parquet out-path
                           {:compression-codec :gzip}))
  )


(comment 
  (keys (ds/group-by (tc/dataset "/work/processed-runs/blackmountain/wide/91x10s_420s_random-forest_run_00.csv.gz")
                     (fn [row] (let [time-field (get row "time")]
                                 (time/string->localdate time-field "yyyy-MM-dd_HH-mm-ss")))))

  (keys (ds/group-by (tc/dataset "/work/processed-runs/blackmountain/wide/2x20s_7m_fully-conv-ar_run_00_wide.csv.gz")
                     (fn [row] (let [time-field (get row "time")]
                                 (time/string->localdate time-field "yyyy-MM-dd_HH-mm-ss")))))

  (def x (let [rf-data (tc/dataset "/work/processed-runs/blackmountain/wide/91x10s_420s_extra-trees_run_00_fold_0_wide.csv.gz")]
           (-> rf-data
               (add-persist-from-other (tc/dataset "/work/processed-runs/blackmountain/wide/2x20s_420s_fully-conv_run_02_fold_0_wide.csv.gz")
                                       "fully-conv_2x20s_420s_run_02_fold_0"
                                       "persist_2x20s_420s_run_02_fold_0")
               )))

  (tc/write! x "/work/test.csv")

  (metrics/get-metrics-wide-dataset x "actual" "random-forest_91x10s_420s_run_00")

  (file/exists? "/results/solpred/blackmountain/extra-trees/full/BM_PO_10s-910s/SkyCamRF/SPA/_ET140/(2015-01-01 to 2015-12-31)-PredictionValuesOnTest.csv")

  
  (ds/->dataset "/work/processed-runs/blackmountain/wide/blackmountain_extra-trees_91x10s_420s_run_00_crop_full_lr_1_fold_0_wide.parquet")
  
  )
