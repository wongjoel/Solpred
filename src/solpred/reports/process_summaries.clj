(ns solpred.reports.process-summaries
  "Processes the output of run"
  (:require
   [clojure.string :as str]
   [clojure.edn :as edn]
   [clojure.data.json :as json]
   [clojure.tools.logging :as log]
   [clojure.java.io :as io]
   [solpred.util.time :as time]
   [solpred.util.file :as file]
   [solpred.util.runner :as runner]
   [solpred.util.runtime-check :as runtime-check]
   [solpred.util.pprint-edn :as edn-out]
   [solpred.util.external :as extern]
   [solpred.reports.metrics :as metrics]
   [tablecloth.api :as tc]
   [tech.v3.dataset :as ds]
   [tech.v3.libs.parquet :as parquet]))

(defn read-run-summaries
  #_(read-run-summaries "/work/processed-runs")
  "Read run summaries (edn) from disk and return as seq of summaries"
  [base-dir]
  (->> (file/walk base-dir)
       (filter file/file?)
       (filter #(= "summary" (file/filename (file/parent %))))
       (filter #(str/ends-with? (file/filename %) ".edn"))
       (map #(edn/read-string (slurp (str %))))))

(defn widen-rename-cols
  #_(widen-rename-cols
     (ds/->dataset "/results/2021/blackmountain-round/auto-process/fully-conv/run_00/lr_3.0E-6/fold0/16x20s_420s/16x20s_420s_fully-conv_out.csv.gz") {})
  "Take the run output, makes it a wide-dataset, and renames the cols based on rename-map {old-name new-name}
  Checks that the input is in the expected format"
  [in-data {:keys [rename-col-map horizon-seconds] :as args}]
  (runtime-check/map-contains? args [:rename-col-map :horizon-seconds])
  (runtime-check/throw-on-false (= 0 (tc/row-count (tc/select-missing in-data)))
                                (str (tc/row-count (tc/select-missing in-data)) " rows with missing data found \n\n" in-data "\n\n" (tc/select-missing in-data)))
  (let [cols (set (tc/column-names in-data))]
    (runtime-check/throw-on-false (contains? cols "series") "data is not in expected format: missing series column")
    (runtime-check/throw-on-false (contains? cols "value") "data is not in expected format: missing value column"))
  (let [wide-data (tc/pivot->wider (tc/convert-types in-data "value" :float64) "series" "value")
        time-data (tc/map-columns wide-data "predicted-time" #{"time"}
                                  (fn [x] (time/datetime->string
                                           (.plusSeconds (time/string->datetime x "yyyy-MM-dd_HH-mm-ss") horizon-seconds) "yyyy-MM-dd_HH-mm-ss")))
        cols (set (tc/column-names wide-data))
        internal-rename-map (assoc rename-col-map "time" "id-time")
        keys (keys rename-col-map)]
    (runtime-check/throw-on-false
     (every? (fn [key] (contains? cols key)) keys) (str "Rename map does not match. Cols: " cols " map: " rename-col-map))
    (tc/rename-columns time-data internal-rename-map)))

(defn read-csi-from-data
  #_(read-csi-from-data "/data/blackmountain/irradiance/2015/BlackMountain_2015-01-01_cloud_projection.csv.gz")
  [path]
  (-> (ds/->dataset (str path))
      (tc/select-columns #{"Timestamp" "ClearSkyGHI"})
      (tc/map-columns "id-time" #{"Timestamp"}
                      (fn [row] (-> row
                                    (time/string->datetime "yyyy-MM-dd HH:mm:ss")
                                    (time/datetime->string "yyyy-MM-dd_HH-mm-ss"))))
      (tc/select-columns #{"id-time" "ClearSkyGHI"})
      (tc/reorder-columns "id-time")))

(defn make-nil-sample
  #_(make-nil-sample {:datetime "___" "actual" 0.1 "model-name_run-string" 0.9 "persist_run-string" 0.1}
                     (time/string->datetime "2020-01-01_09-20-10" "yyyy-MM-dd_HH-mm-ss")
                     420)
  [sample at-time horizon-seconds]
  (let [nil-sample (reduce-kv (fn [acc k v] (assoc acc k nil)) {} sample)]
    (assoc nil-sample
           :datetime at-time
           "id-time" (time/datetime->string at-time "yyyy-MM-dd_HH-mm-ss")
           "predicted-time" (time/datetime->string (.plusSeconds at-time horizon-seconds) "yyyy-MM-dd_HH-mm-ss"))))

(defn add-na-to-data
  #_(add-na-to-data
     {:min-gap 9 :max-gap 11 :nominal-gap 10 :horizon-seconds 420}
     [{:datetime (time/string->datetime "2020-01-01_09-10-10" "yyyy-MM-dd_HH-mm-ss")
       "actual" 0.1 "model-name_run-string" 0.9 "persist_run-string" 0.1}
      {:datetime (time/string->datetime "2020-01-01_09-10-20" "yyyy-MM-dd_HH-mm-ss")
       "actual" 0.1 "model-name_run-string" 0.9 "persist_run-string" 0.1}
      {:datetime (time/string->datetime "2020-01-01_09-12-11" "yyyy-MM-dd_HH-mm-ss")
       "actual" 0.1 "model-name_run-string" 0.9 "persist_run-string" 0.1}
      {:datetime (time/string->datetime "2020-01-01_09-12-20" "yyyy-MM-dd_HH-mm-ss")
       "actual" 0.1 "model-name_run-string" 0.9 "persist_run-string" 0.1}])
  "Pads data with NA values around gaps so that plotting libs know these gaps are intentional"
  [{:keys [min-gap max-gap nominal-gap horizon-seconds] :as args} data-seq]
  (runtime-check/map-contains? args [:min-gap :max-gap :nominal-gap :horizon-seconds])
  (let [new-data (reduce
                  (fn [acc sample]
                    (let [prev (:datetime (peek acc))
                          current (:datetime sample)
                          check-gap-okay? #(<= min-gap (time/seconds-between prev current) max-gap)
                          make-prev-nil-sample #(make-nil-sample sample (.minusSeconds current nominal-gap) horizon-seconds)]
                      (cond
                        (nil? prev) (conj (conj acc (make-prev-nil-sample)) sample)
                        (check-gap-okay?) (conj acc sample)
                        :else (conj (conj acc (make-prev-nil-sample)) sample))))
                  [] data-seq)
        last-sample (last new-data)]
    (conj new-data (make-nil-sample last-sample (.plusSeconds (:datetime last-sample) nominal-gap) horizon-seconds))))


(defn add-na-to-wide-dataset
  #_(add-na-to-wide-dataset "/work/processed-runs/blackmountain/wide/blackmountain_model6_4x120s_420s_run_00_crop_full_lr_3.0E-6_fold_5_wide.parquet" {:round true})
  "Outer loop for adding na to a wide dataset"
  [data {:keys [round] :as args}]
  (runtime-check/map-contains? args [:round])
  (let [gap-params (if round
                     {:min-gap 54 :max-gap 66 :nominal-gap 60}
                     {:min-gap 9 :max-gap 11 :nominal-gap 10})]
                          
    (->> data
         (ds/mapseq-reader)
         (map (fn [x] (assoc x :datetime (time/string->datetime (get x "id-time") "yyyy-MM-dd_HH-mm-ss"))))
         (sort-by :datetime)
         (add-na-to-data (merge gap-params args))
         (map (fn [x] (dissoc x :datetime)))
         (ds/->>dataset))))

(defn run-summaries->wide-datasets!
  "Convert long-datasets to wide datasets, by reading run-summaries"
  [base-dir]
  (log/debug "Making wide datasets")
  (let [runs
        (group-by #(if (file/exists? (:wide-dataset-path %)) :skip :process)
                  (read-run-summaries base-dir))
        skip-jobs
        (->> (:skip runs)
             (map (fn [{:keys [wide-dataset-path]}] #(log/debug (str "Skipping " wide-dataset-path " because it exists")))))
        proc-jobs
        (->> (:process runs)
             (map (fn [{:keys [wide-dataset-path long-dataset-path] :as run}]
                    #(do
                       (log/debug (str "Making " wide-dataset-path))
                       (file/make-dirs (file/parent wide-dataset-path))
                       (let [wide-na-ds (-> (ds/->dataset long-dataset-path)
                                            (widen-rename-cols run)
                                            (add-na-to-wide-dataset run))]
                         (parquet/ds->parquet wide-na-ds
                                              wide-dataset-path
                                              {:compression-codec :gzip}))))))
        ]
    (runner/run-jobs-without-threadpool! {:jobs (into skip-jobs proc-jobs)})))

(defn run-summaries->clear-sky-irradiance-cache!
  #_(run-summaries->clear-sky-irradiance-cache! "/work/processed-runs")
  "make the edn-metric files described in run-summaries"
  [base-dir]
  (log/debug "Making clear-sky-irradiance-cache")
  (let [group (group-by
               #(cond
                  (str/includes? (:dataset-id %) "blackmountain") :blackmountain
                  (str/includes? (:dataset-id %) "sunset") :sunset
                  :else :unknown)
               (read-run-summaries base-dir))]
    (->> (:blackmountain group)
         (run! (fn [x] (if (file/exists? (:clear-sky-irradiance-cache x))
                         (log/debug (str (:clear-sky-irradiance-cache x) " already exists, skipping"))
                         (let [cache-data
                               (->> (file/list-children "/data/blackmountain/irradiance/2015")
                                    (pmap read-csi-from-data)
                                    (reduce (fn [acc curr] (tc/concat acc curr))))]
                           (parquet/ds->parquet cache-data
                                                (:clear-sky-irradiance-cache x)
                                                {:compression-codec :gzip}))))))))

(defn run-summaries->category-metrics!
  #_(run-summaries->category-metrics! "/work/processed-runs")
  "make the category metric files described in run-summaries"
  [base-dir]
  (let [group
        (group-by #(if (file/exists? (:metric-file %)) :skip :process)
                  (->> (read-run-summaries base-dir)
                       (mapcat (fn [{:keys [category-metric-files] :as run}]
                                 (map #(merge run %) category-metric-files)))))
        skip-jobs
        (->> (:skip group)
             (map (fn [{:keys [metric-file]}] #(log/debug (str "Skipping " metric-file " because it exists")))))
        proc-jobs
        (->> (:process group)
           (map (fn [{:keys [metric-file] :as run}]
                  #(let [data (metrics/get-data-from-category run)
                        metric-dataset-path (file/resolve-path [(file/parent (file/parent metric-file)) "wide" (str/replace (file/filename metric-file) #".edn" ".parquet")])]
                    (run! (fn [path]
                            (log/debug (str "Making " path))
                            (file/make-dirs (file/parent path))) [metric-file metric-dataset-path])
                    (parquet/ds->parquet data
                                         metric-dataset-path
                                         {:compression-codec :gzip})
                    (edn-out/spit-pprint (merge run
                                                {:metric-dataset-path metric-dataset-path}
                                                (metrics/calculate-metrics run (tc/drop-missing data)))
                                         metric-file)))))]
    (runner/run-jobs! {:jobs (into skip-jobs proc-jobs)})))

(defn run-summaries->case-study-metrics!
  #_(run-summaries->case-study-metrics! "/work/processed-runs")
  "make the category metric files described in run-summaries"
  [base-dir]
  (let [group
        (group-by #(if (file/exists? (:metric-file %)) :skip :process)
                  (->> (read-run-summaries base-dir)
                       (mapcat (fn [{:keys [case-studies] :as run}]
                                 (map #(merge run %) case-studies)))))
        skip-jobs
        (->> (:skip group)
             (map (fn [{:keys [metric-file]}] #(log/debug (str "Skipping " metric-file " because it exists")))))
        proc-jobs
        (->> (:process group)
           (map (fn [{:keys [metric-file] :as run}]
                  #(let [data (metrics/get-case-study-data run)
                        metric-dataset-path (file/resolve-path [(file/parent (file/parent metric-file)) "wide" (str/replace (file/filename metric-file) #".edn" ".parquet")])]
                    (run! (fn [path]
                            (log/debug (str "Making " path))
                            (file/make-dirs (file/parent path))) [metric-file metric-dataset-path])
                    #_(log/debug (str "writing: " data))
                    (parquet/ds->parquet data
                                         metric-dataset-path
                                         {:compression-codec :gzip})
                    (edn-out/spit-pprint (merge run
                                                {:metric-dataset-path metric-dataset-path}
                                                (metrics/calculate-metrics run (tc/drop-missing data)))
                                         metric-file)))))]
    (runner/run-jobs! {:jobs (into skip-jobs proc-jobs)})))

(comment
  (parquet/ds->parquet (tc/dataset nil {:column-names [:a :b]})
                       "/work/empty.parquet"
                       {:compression-codec :gzip})
  )

(defn run-summaries->interact-plots!
  #_(run-summaries->interact-plots! "/work/processed-runs")
  "make interactive plots"
  [base-dir]
  (let [group
        (group-by #(if (file/exists? (file/resolve-path [(:plots-dir %) (str (:run-description-string %) ".html")])) :skip :process)
                  (read-run-summaries base-dir))
        skip-jobs
        (->> (:skip group)
             (map (fn [{:keys [run-description-string]}] #(log/debug (str "Skipping " run-description-string " because it exists")))))
        proc-jobs
        (->> (:process group)
             (map (fn [{:keys [plots-dir run-description-string wide-dataset-path tmp-dir] :as run}]
                    #(do
                       (runtime-check/map-contains? run [:plots-dir :run-description-string :wide-dataset-path :tmp-dir])
                       (log/debug (str "Making " plots-dir))
                       (file/make-dirs plots-dir)
                       (let [python-instr-file (file/resolve-path [tmp-dir (str run-description-string "_" (rand-int 1000) ".json")])
                             python-args
                             {:title run-description-string
                              :out-dir plots-dir
                              :path wide-dataset-path}]
                         (file/make-dirs (:tmp-dir run))
                         (spit python-instr-file (json/write-str python-args))
                         (-> (extern/make-executor ["python3" "/app/src/plotting/json-to-interact.py" python-instr-file])
                             (.exitValue (int 0))
                             (.directory (io/file "/app/src/plotting/"))
                             (.execute))
                         (file/delete python-instr-file)
                         )
                       ))))
        ]
    (runner/run-jobs! {:jobs (into skip-jobs proc-jobs)})))


(defn main
  #_(main "/work/processed-runs")
  "main method of the ns"
  [base-dir]
  (run-summaries->wide-datasets! base-dir)
  (run-summaries->clear-sky-irradiance-cache! base-dir)
  (run-summaries->category-metrics! base-dir)
  (run-summaries->case-study-metrics! base-dir)
  (run-summaries->interact-plots! base-dir))
