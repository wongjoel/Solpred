(ns solpred.reports.make-summaries
  "Processes the folder of raw results into summary edn files"
  (:require
   [clojure.string :as str]
   [clojure.edn :as edn]
   [solpred.util.file :as file]
   [solpred.util.runner :as runner]
   [solpred.util.runtime-check :as runtime-check]
   [solpred.util.pprint-edn :as edn-out]
   [tablecloth.api :as tc]
   [clojure.tools.logging :as log]))

(defn make-grouping-description-string
  #_(make-grouping-description-string {:dataset-id "BMR" :model-id "fully-conv" :input-terms 16 :input-spacing 60 :horizon 420 :run-id 0})
  "Standard function for making a run description string - prevents multiple formats of the string creation from being in the codebase"
  [{:keys [dataset-id model-id input-terms input-spacing horizon run-id crop-size learn-rate] :as args}]
  (runtime-check/map-contains?
   args [:dataset-id :model-id :input-terms :input-spacing :horizon :run-id :crop-size :learn-rate])
  (str dataset-id "_" model-id "_" input-terms "x" input-spacing "s_" horizon "_run_" run-id "_crop_" crop-size "_lr_" learn-rate))

(defn make-run-description-string
  #_(make-run-description-string {:dataset-id "BMR" :model-id "fully-conv" :input-terms 16 :input-spacing 60 :horizon 420 :run-id 0 :fold-id 1})
  "Standard function for making a run description string - prevents multiple formats of the string creation from being in the codebase"
  [{:keys [fold-id] :as args}]
  (runtime-check/map-contains?
   args [:fold-id])
  (str (make-grouping-description-string args) "_fold_" fold-id))

(defn parse-inout-config-string
  #_(parse-inout-config-string "16x60s_420s")
  "Parse a combined inout-config string into component parts"
  [inout-string]
  (let [splits (str/split inout-string #"_")
        [terms spacing] (str/split (nth splits 0) #"x")]
    {:input-terms (parse-long terms)
     :input-spacing (parse-long (.replaceAll spacing "[^0-9]" ""))
     :horizon (nth splits 1 "unknown-horizon")}))

(defn parse-horizon-string
  #_(parse-horizon-string "420s")
  [horizon-string]
  (cond
    (str/includes? horizon-string "s") (parse-long (str/replace horizon-string #"s" ""))
    (str/includes? horizon-string "steps") (* 10 (parse-long (str/replace horizon-string #"steps" "")))
    (str/includes? horizon-string "m") (* 60 (parse-long (str/replace horizon-string #"m" "")))
    :else (runtime-check/throw-ill-arg (str "Cannot parse horizon string: " horizon-string))
    ))


(defn calculate-dataset-details
  #_(calculate-dataset-details
     {:run-dir "/data/results/2021/blackmountain/auto-process/sunset/run_01/lr_3.0E-6/fold1/16x60s_420s"
      :inout-config "16x60_420s"
      :out-dir "/work/processed-runs/blackmountain"
      :model-id "sunset"
      :dataset-id "blackmountain"
      :old-model-col "sunset_pred"
      :old-persist-col "persist_pred"
      :run-id "00"
      :learn-rate "3E-6"
      :fold-id "1"
      :input-terms 16
      :input-spacing 60
      :horizon "420s"})
  "Calculates the parameters for widen-rename-cols based on the run folder's name"
  [{:keys [run-dir inout-config out-dir model-id old-model-col old-persist-col run-id fold-id] :as exp-data}]
  (runtime-check/map-contains?
   exp-data [:run-dir :inout-config :out-dir :model-id :old-model-col :old-persist-col :run-id :fold-id])
  (let [new-description (str "_" inout-config "_run_" run-id "_fold_" fold-id)
        model-col (str model-id new-description)
        persist-col (str "persist" new-description)
        run-description-string (make-run-description-string exp-data)]
    {:run-description-string run-description-string
     :grouping-description-string (make-grouping-description-string exp-data)
     :wide-dataset-path (file/resolve-path [out-dir "wide" (str run-description-string "_wide.parquet")])
     :long-dataset-path (file/resolve-path [run-dir (str inout-config "_" model-id "_out.csv.gz")])
     :model-col model-col
     :persist-col persist-col
     :rename-col-map {old-model-col model-col
                      old-persist-col persist-col}}))


(defn read-runs-from-disk
  #_(time (read-runs-from-disk {:base-dir "/results/solpred/blackmountain/auto-process"
                                :out-dir "/work/processed-runs/blackmountain"}))
  "Extract run data from folders and filenames"
  [{:keys [base-dir] :as args}]
  (sequence
   (comp
    (filter (fn [child] (str/includes? (file/filename child) "run-description.edn")))
    (map str)
    (map slurp)
    (map edn/read-string)
    (map #(merge % args))
    (map #(merge % {:old-model-col (str (:model-id %) "_pred")
                    :old-persist-col "persist_pred"}))
    (map #(merge % (parse-inout-config-string (:inout-config %))))
    (map #(assoc % :horizon-seconds (parse-horizon-string (:horizon %))))
    (map #(merge % (calculate-dataset-details %)))
    (map #(merge % {:summary-file-path (file/resolve-path [(:out-dir %) "summary" (str (:run-description-string %) ".edn")])
                    :clear-sky-irradiance-cache (file/resolve-path [(:out-dir %) "clear-sky-irradiance-cache.parquet"])
                    :plots-dir (file/resolve-path [(:out-dir %) "plots"])
                    :tmp-dir "/tmp/solpred"})))
   (file/walk base-dir)
   ))

(defn add-category-metric-files-to-run
  [runs]
  (map
   (fn [{:keys [categories out-dir run-description-string] :as summary}]
     (runtime-check/map-contains?
      summary [:categories :out-dir :run-description-string])
     (assoc summary
            :category-metric-files
            (map (fn [category]
                   {:category-name category
                    :metric-file (file/resolve-path
                                  [out-dir "categories" category "edn"
                                   (str run-description-string "_metrics.edn")])})
                 categories)))
   runs))

(defn add-case-studies-to-run
  [runs]
  (map
   (fn [{:keys [out-dir case-study-file run-description-string] :as summary}]
     (runtime-check/map-contains?
      summary [:out-dir :case-study-file :run-description-string])
     (if case-study-file
       (let [case-studies (edn/read-string (slurp case-study-file))]
         (assoc summary
                :case-studies
                (map (fn [{:keys [case-study-id] :as case-study}]
                       (assoc case-study
                              :metric-file (file/resolve-path
                                            [out-dir "case-studies" case-study-id "edn"
                                             (str run-description-string "_metrics.edn")])))
                     case-studies)))
       (assoc summary :case-studies nil)
       ))
   runs))

(defn runs->run-summaries!
  #_(runs->run-summaries! [{:base-dir "/results/solpred/blackmountain-round/auto-process"
                            :dataset-id "blackmountain-round"
                            :out-dir "/work/processed-runs/blackmountain-round"
                            :categories ["All"]
                            :case-study-file "resources/datasets/bm-casestudies.edn"}])
  "Read runs from disk and write out edn files with the data parsed"
  [run-list]
  (log/debug "Making run summaries")
  (let [jobs
        (map (fn [run]
               #(->> (read-runs-from-disk run)
                     add-category-metric-files-to-run
                     add-case-studies-to-run
                     (run! (fn [{:keys [summary-file-path] :as summary}]
                             (file/make-dirs (file/parent summary-file-path))
                             (edn-out/spit-pprint (dissoc summary :summary-file-path) summary-file-path)))))
             run-list)]
    jobs
    (runner/run-jobs! {:jobs jobs})))

(defn main
  #_(main
     [{:dataset-id "blackmountain"
       :categories ["All" "Intermittent" "Sunny" "Overcast" "Mixed"]
       :category-file "resources/datasets/blackmountain-test.csv"
       :case-study-file "resources/datasets/bm-casestudies.edn"}])
  "main method of the ns"
  [datasets]
  (runs->run-summaries! (map (fn [{:keys [dataset-id] :as dataset}]
                               (assoc dataset
                                      :base-dir (file/resolve-path [ "/results/solpred" dataset-id "auto-process"])
                                      :out-dir (file/resolve-path [ "/work/processed-runs" dataset-id])))
                             datasets))
  )

(comment
  (def config {})

  (main
   [#_{:dataset-id "blackmountain"
     :round false
     :categories ["All" "Intermittent" "Sunny" "Overcast" "Mixed"]
     :category-file "resources/datasets/blackmountain-test.csv"}
    #_{:dataset-id "blackmountain-intermittent"
     :round false
     :categories ["All" "Intermittent" "Sunny" "Overcast" "Mixed"]
     :category-file "resources/datasets/blackmountain-test.csv"}
    {:dataset-id "blackmountain-round"
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
    #_{:dataset-id "stanford"
     :round false
     :categories ["All"]
     :category-file nil}])

  (tc/info (tc/dataset "/work/processed-runs/blackmountain-round_2_blackmountain-ramp-round/wide-na/blackmountain-round_2_blackmountain-ramp-round_fully-conv_16x60s_420s_run_00_lr_3.0E-6_fold_0_wide-na.parquet"))

  (tc/info (tc/dataset "/work/processed-runs/blackmountain-round_2_blackmountain-ramp-round/wide/blackmountain-round_2_blackmountain-ramp-round_fully-conv_16x60s_420s_run_00_lr_3.0E-6_fold_0_wide.parquet"))

  (file/exists? "/work/processed-runs/blackmountain/wide/blackmountain_fully-conv-ar_16x20s_420s_run_01_lr_3.0E-6_fold_0_wide.parquet")

  )
