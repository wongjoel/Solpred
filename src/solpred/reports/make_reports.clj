(ns solpred.reports.make-reports
  "Processes the folder of raw results"
  (:require
   [clojure.string :as str]
   [clojure.tools.logging :as log]
   [solpred.reports.clean-results :as clean-results]
   [solpred.reports.make-summaries :as make-summaries]
   [solpred.reports.process-summaries :as process-summaries]
   [solpred.reports.process-metrics :as process-metrics]
   [solpred.reports.make-ensembles :as make-ensembles]
   ))

(defn main
  #_(main)
  []
  (clean-results/main "/results/solpred")
  
  (make-summaries/main [{:dataset-id "blackmountain"
                          :round false
                          :categories ["All" "Intermittent" "Sunny" "Overcast" "Mixed"]
                          :category-file "resources/datasets/blackmountain-test.csv"
                         :case-study-file "resources/datasets/bm-casestudies.edn"}
                        {:dataset-id "blackmountain-2016"
                          :round false
                          :categories ["All"]
                          :category-file nil
                          :case-study-file nil}
                         #_{:dataset-id "blackmountain-intermittent"
                            :round false
                            :categories ["All" "Intermittent" "Sunny" "Overcast" "Mixed"]
                            :category-file "resources/datasets/blackmountain-test.csv"}
                         #_{:dataset-id "blackmountain-round"
                          :round true
                          :categories ["All" "Intermittent" "Sunny" "Overcast" "Mixed"]
                          :category-file "resources/datasets/blackmountain-test.csv"
                          :case-study-file "resources/datasets/bm-casestudies.edn"}
                         #_{:dataset-id "blackmountain-ramp-round"
                            :round true
                            :categories ["All" "Intermittent" "Sunny" "Overcast" "Mixed"]
                            :category-file "resources/datasets/blackmountain-test.csv"}
                         #_{:dataset-id "blackmountain-round_2_blackmountain-ramp-round"
                            :round true
                            :categories ["All" "Intermittent" "Sunny" "Overcast" "Mixed"]
                            :category-file "resources/datasets/blackmountain-test.csv"}
                         #_{:dataset-id "bm-round_on_bm-ramp-round"
                            :round true
                            :categories ["All" "Intermittent" "Sunny" "Overcast" "Mixed"]
                            :category-file "resources/datasets/blackmountain-test.csv"}
                        {:dataset-id "stanford"
                         :round false
                         :categories ["All"]
                         :category-file nil
                         :case-study-file nil}])

  (process-summaries/main "/work/processed-runs")

  (make-ensembles/main "/work/processed-runs")
  
  (process-metrics/main "/work/processed-runs")

  )
