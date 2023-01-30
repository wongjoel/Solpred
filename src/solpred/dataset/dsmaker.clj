(ns solpred.dataset.dsmaker
  "Make a day of data into a set of independent samples.
  Takes a time series and breaks it up into individual self-contained training samples"
  (:require
   [amalloy.ring-buffer :as ring-buffer]
   [clojure.data.json :as json]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [solpred.util.external :as extern]
   [solpred.util.file :as file]
   [solpred.util.time :as time]
   [solpred.util.tar :as tar]
   [solpred.util.csi :as csi]
   [solpred.util.zip :as zip]
   [solpred.util.sun-coordinates :as sun-coords]
   [solpred.util.runtime-check :as runtime-check]
   [solpred.util.runner :as runner]
   [tablecloth.api :as tc]
   [tech.v3.dataset :as ds]
   [clojure.tools.logging :as log])
  (:import
   (java.time LocalDate LocalDateTime Duration)
   (org.apache.commons.math3.stat.descriptive DescriptiveStatistics)))

(defn calc-indexes
  #_(calc-indexes [2 3])
  "Get the indexes for a ring buffer (helper for `make-windows`)"
  [horizons]
  (let [max-horizon (apply max horizons)
        factor (- max-horizon -1)
        indexes (vec (map (fn [x] {:horizon x :index (- x factor)}) horizons))]
    {:max-horizon max-horizon :targets indexes}))

(defn get-targets
  #_(get-targets
     (:targets (calc-indexes [2 3]))
     (into (ring-buffer/ring-buffer 6) [1 2 3 4 5 6]))
  "Get the target data from the ring buffer (helper for `make-windows`)"
  [targets queue]
  (into [] (map (fn [{:keys [horizon index]}] {:horizon horizon
                                               :horizon-unit :steps
                                               :value (nth queue index)})) targets))

(defn make-windows
  #_(sequence (make-windows {:history-steps 4 :horizons-steps [2 3]}) (range 25))
  "Transducer: Made a window of historical data and a target value"
  [{:keys [history-steps horizons-steps] :as config}]
  (runtime-check/map-contains? config [:history-steps :horizons-steps])
  (fn [xf]
    (let [{:keys [max-horizon targets]} (calc-indexes horizons-steps)
          buffer-size (+ history-steps max-horizon)
          queue (volatile! (ring-buffer/ring-buffer buffer-size))]
      (fn
        ([] (xf))
        ([result] (xf result))
        ([result input]
         (let [q (vswap! queue conj input)]
           (if (= (count q) buffer-size)
             (xf result {:data (take history-steps q)
                         :targets (get-targets targets q)
                         :full-seq (seq q)})
             result)))))))


(defn make-samples-remove-gaps-std-dev
  #_(sequence (make-samples-remove-gaps-std-dev
               {:history-steps 5
                :horizons-steps [5]
                :no-gaps-detected? (fn [current prev] (< (- current prev) 2))
                :std-dev-window 5
                :std-dev-filter-satisfied? (fn [std-dev] (< 1 std-dev))})
              (range 100))
  "Transducer: Made a window of historical data and a target value
   Takes a function that returns true if no gap is found. Filters out samples with gaps"
  [{:keys [history-steps horizons-steps no-gaps-detected? std-dev-window std-dev-filter-satisfied?] :as config}]
  (fn [xf]
    (let [{:keys [max-horizon targets]} (calc-indexes horizons-steps)
          buffer-size (+ history-steps max-horizon)
          stats (DescriptiveStatistics. std-dev-window)
          queue (volatile! (ring-buffer/ring-buffer buffer-size))]
      (fn
        ([] (xf))
        ([result] (xf result))
        ([result input]
         (.addValue stats (:csi input))
         (if (empty? @queue)
           (do
             (vswap! queue conj input)
             result)
           (do
             (if (no-gaps-detected? input (last @queue))
               (do
                 (let [q (vswap! queue conj input)]
                   (if (= (count q) buffer-size)
                     (if (std-dev-filter-satisfied? (.getStandardDeviation stats))
                       (xf result {:data (take history-steps q) :targets (get-targets targets q)})
                       (do
                         (println (str "Std deviation not satisfied " (.getStandardDeviation stats)))
                         result))
                     result)))
               (do
                 (println (str "found gap " @queue))
                 (vreset! queue (conj (ring-buffer/ring-buffer buffer-size) input))
                 result))
             ))
         )))))

(defn sample->step-json
  #_(sample->step-json
     {:t0 (time/string->datetime "2015-01-01_05:52:47" "yyyy-MM-dd_HH:mm:ss")
      :data
      [{:filename "2015-01-01_05-52-17.jpg" :ghi 0.955 :timestamp (time/string->datetime "2015-01-01_05:52:17" "yyyy-MM-dd_HH:mm:ss")}
       {:filename "2015-01-01_05-52-27.jpg" :ghi 0.991 :timestamp "2015-01-01T05:52:27"}
       {:filename "2015-01-01_05-52-37.jpg" :ghi 1.102 :timestamp "2015-01-01T05:52:37"}
       {:filename "2015-01-01_05-52-47.jpg" :ghi 1.175 :timestamp (time/string->datetime "2015-01-01_05:52:47" "yyyy-MM-dd_HH:mm:ss")}]
      :targets
      [{:horizon 1 :horizon-unit :steps :value {:filename "2015-01-01_05-52-57.jpg" :ghi 1.249 :timestamp "2015-01-01T05:52:57"}}
       {:horizon 2 :horizon-unit :steps :value {:filename "2015-01-01_05-53-07.jpg" :ghi 1.249 :timestamp "2015-01-01T05:53:07"}}]})
  "Transform data into the expected JSON output format"
  [{:keys [data targets t0] :as sample}]
  (runtime-check/map-contains? sample [:data :targets :t0])
  {:id (time/datetime->string t0 "yyyy-MM-dd_HH-mm-ss")
   :inputs (->> (reverse data)
                (map #(dissoc % :original-filename :crop-params))
                (map #(assoc % :timestamp (str (:timestamp %))))
                (map-indexed (fn [index item] (assoc item :distance index)))
                )
   :targets (map #(assoc %
                        :timestamp (str (get-in % [:value :timestamp]))
                        :clearskyghi (get-in % [:value :clearskyghi])
                        :value (get-in % [:value :globalcmp11physical]))
                 targets)})

(defn sample->filename-map
  #_(sample->filename-map {:data
                        [{:filename "2015-01-01_05:52:17.jpg"}
                         {:filename "2015-01-01_05:52:27.jpg"}
                         {:filename "2015-01-01_05:52:37.jpg"}
                         {:filename "2015-01-01_05:52:47.jpg" :timestamp (time/string->datetime "2015-01-01T05:52:47" "yyyy-MM-dd'T'HH:mm:ss")}]})
  "Get the mapping from original filenames to numbered image"
  [{:keys [data] :as sample}]
  (let [id (time/datetime->string (:timestamp (last data)) "yyyy-MM-dd_HH-mm-ss")]
    (map-indexed (fn [index item]
                   {:original (:filename item)
                    :numbered (format "%s.t-%04d.webp" id index)})
                 (reverse data))))

(defn samples->folder!
  #_(samples->folder! sample-job test-samples)
  "Writes samples out to a folder."
  [{:keys [tmp-image-dir tar-assembly-dir] :as job} samples]
  (file/make-dirs tar-assembly-dir)
  (run! (fn [{:keys [json image-map] :as sample}]
          (let [json-fname (str (:id json) ".data.json")]
            (spit (file/resolve-path [tar-assembly-dir json-fname]) (json/write-str json)))
          (run! (fn [{:keys [original numbered]}]
                  (io/copy (io/file (file/resolve-path [tmp-image-dir original]))
                           (io/file (file/resolve-path [tar-assembly-dir numbered]))))
                image-map))
        samples)
  )

(defn calc-job-paths
  [date {:keys [data-dir filename-prefix filename-suffix image-dir output-dir cache-image-dir tmp-image-dir tmp-dir] :as config}]
  (runtime-check/map-contains? config [:data-dir :filename-prefix :filename-suffix :image-dir :output-dir :cache-image-dir :tmp-image-dir :tmp-dir])
  {:data-file (file/resolve-path [data-dir
                           (time/localdate->string date "yyyy")
                           (str filename-prefix (time/localdate->string date "yyyy-MM-dd") filename-suffix)])
   :image-zip (file/resolve-path [image-dir (time/localdate->string date "yyyy/yyyy-MM-dd'.zip'")])
   :output-tar (file/resolve-path [output-dir (time/localdate->string date "yyyy-MM-dd'.tar'")])
   :tar-assembly-dir (file/resolve-path [tmp-dir (time/localdate->string date "yyyy-MM-dd")])
   :tmp-image-dir (file/resolve-path [tmp-image-dir (time/localdate->string date "yyyy-MM-dd")])
   :cache-zip (file/resolve-path [cache-image-dir (time/localdate->string date "yyyy-MM-dd'.zip'")])}
  )

(defn has-no-gap?
  #_(has-no-gap? [{:timestamp (time/string->datetime "2021-01-01_12-00-00" "yyyy-MM-dd_HH-mm-ss")}
                  {:timestamp (time/string->datetime "2021-01-01_12-00-10" "yyyy-MM-dd_HH-mm-ss")}
                  {:timestamp (time/string->datetime "2021-01-01_12-00-20" "yyyy-MM-dd_HH-mm-ss")}
                  {:timestamp (time/string->datetime "2021-01-01_12-00-40" "yyyy-MM-dd_HH-mm-ss")}])
  "Takes a sorted sequence of maps containing timestamps and returns if there's a gap more than 10% away from 10 seconds"
  [full-seq]
  (runtime-check/map-contains? (first full-seq) [:timestamp])
  (boolean
   (->> full-seq
        (map :timestamp)
        (reduce (fn [acc curr]
                  (if (<= 9 (time/seconds-between acc curr) 11)
                    curr
                    (reduced false))))
        )))

(defn has-ramp-event?
  #_(has-ramp-event? [{:ghi 1000 :clear-sky-ghi 1000}
                      {:ghi 800 :clear-sky-ghi 1000}
                      {:ghi 1000 :clear-sky-ghi 1000}
                      {:ghi 1000 :clear-sky-ghi 1000}
                      {:ghi 1000 :clear-sky-ghi 1000}] 10)
  "Detects ramp events in a seq - ramp event defined when actual ramp bigger than clear sky ramp by percent"
  [full-seq percentage]
  (runtime-check/map-contains? (first full-seq) [:globalcmp11physical :clearskyghi])
  (let [ghi (map :globalcmp11physical full-seq)
        clearsky-ghi  (map :clearskyghi full-seq)
        actual-ramp (- (reduce max ghi) (reduce min ghi))
        clearsky-ramp (- (reduce max clearsky-ghi) (reduce min clearsky-ghi))
        ramp-diff (abs (- actual-ramp clearsky-ramp))
        clearsky-max (reduce max clearsky-ghi)
        threshold (* clearsky-max (/ percentage 100))]
    (> ramp-diff threshold)))

(defn has-ramp-event2?
  #_(has-ramp-event2? [{:ghi 1000 :clear-sky-ghi 1000}
                      {:ghi 800 :clear-sky-ghi 1000}
                      {:ghi 1000 :clear-sky-ghi 1000}
                      {:ghi 1000 :clear-sky-ghi 1000}
                      {:ghi 1000 :clear-sky-ghi 1000}] 10)
  "Detects ramp events in a seq - alternate definition"
  [full-seq percentage]
  (let [ghi (map :ghi full-seq)
        clearsky-ghi  (map :clear-sky-ghi full-seq)
        actual-ramp (- (reduce max ghi) (reduce min ghi))
        clearsky-max (reduce max clearsky-ghi)
        threshold (* clearsky-max (/ percentage 100))]
    (> actual-ramp threshold)))

(defn calculate-resize-crop
  #_(calculate-resize-crop {:crop-size "full" :img-width 64})
  #_(calculate-resize-crop {:crop-size 64 :img-width 64 :sunazimuth 35 :sunzenith 20 :lens-model-path "/work/calib_results-Blackmountain.txt"})
  [{:keys [crop-size img-width] :as sample}]
  (let [renamed-sample (assoc sample :azimuth (:sunazimuth sample) :zenith (:sunzenith sample))]
    (cond
      (= crop-size "full") {:left 256 :upper 0 :right 1792 :lower 1536 :width img-width :height img-width}
      :else (merge {:width img-width :height img-width}
                   (sun-coords/calculate-crop renamed-sample))))
  )

(defn calculate-filename
  #_(calculate-filename {:timestamp (time/string->datetime "2015-01-01_12-00-00" "yyyy-MM-dd_HH-mm-ss") :crop-size 64 :img-width 64 :sunazimuth 35 :sunzenith 20 :lens-model-path "/work/calib_results-Blackmountain.txt"})
  ""
  [{:keys [timestamp crop-size] :as sample}]
  (runtime-check/map-contains? sample [:timestamp :crop-size])
  {:original-filename (time/datetime->string timestamp "yyyy-MM-dd/yyyy-MM-dd_HH-mm-ss'.jpg'")
   :crop-params (calculate-resize-crop sample)
   :filename (str (time/datetime->string timestamp "yyyy-MM-dd_HH-mm-ss") "_" crop-size ".png")
   })

(defn config->jobs
  #_(config->jobs {:start-date "2015-01-01" :end-date "2015-01-10"})
  #_(config->jobs sample-config)
  "Make the job description files, filtering out files based on disk state"
  [{:keys [start-date end-date] :as config}]
  (log/debug "Making Jobs")
  (let [jobs
        (->> (iterator-seq (.iterator (.datesUntil (LocalDate/parse start-date) (LocalDate/parse end-date))))
             (map (fn [x] (assoc config :date x)))
             (map (fn [{:keys [date] :as x}]
                    (merge x (calc-job-paths date config)))))
        group
        (group-by
         (fn [x]
           (cond
             (not (file/exists? (:data-file x))) :no-data
             (not (file/exists? (:image-zip x))) :no-image
             (file/exists? (:output-tar x)) :exists
             :else :process))
         jobs)]
    (->> (:no-data group)
         (run! (fn [x] (log/debug (str "Skip, missing: " (:data-file x))))))
    (->> (:no-image group)
         (run! (fn [x] (log/debug (str "Skip, missing: " (:image-zip x))))))
    (->> (:exists group)
         (run! (fn [x] (log/debug (str "Skip, already exists: " (:output-tar x))))))
    (:process group)))

(defn datafile->samples
  #_(datafile->samples {:data-file "/data/blackmountain/irradiance/2015/BlackMountain_2015-01-01_cloud_projection.csv.gz"
                        :columns #{"Timestamp" "GlobalCMP11Physical" "ClearSkyGHI" "SunZenith" "SunAzimuth"}})
  [{:keys [data-file columns]}]
  (-> (tc/dataset data-file)
      (tc/select-columns columns)
      (tc/drop-missing)
      (ds/mapseq-reader)))

(defn job->samples
  #_(job->samples sample-job)
  [job]
  (runtime-check/map-contains? job [:round-minute? :ramps-only? :remove-high-zenith?])
  (let [round-filter (if (:round-minute? job)
                       #(let [second-of-minute (.getSecond (:t0 %))]
                          (or (< second-of-minute 5) (> second-of-minute 55)))
                       (fn [_] true))
        ramp-filter (if (:ramps-only? job)
                      #(has-ramp-event? (:full-seq %) (:ramp-threshold job))
                      (fn [_] true))
        zenith-filter (if (:remove-high-zenith? job)
                        #(> (:zenith-threshold job) (reduce max (map :sunzenith (:full-seq %))))
                        (fn [_] true))
        bounds-filter (if (:remove-out-of-bounds? job)
                        #(not (sun-coords/crop-out-of-bounds? (merge job (:crop-params %))))
                        (fn [_] true))
        ]
    (into
     []
     (comp
      (map #(reduce-kv (fn [acc k v] (assoc acc (keyword (str/lower-case k)) v)) {} %))
      (map #(assoc % :timestamp (time/string->datetime (:timestamp %) "yyyy-MM-dd HH:mm:ss")))
      (map #(merge % (calculate-filename (merge job %))))
      (map #(assoc % :clearskyindex (csi/clear-sky-index (:globalcmp11physical %) (:clearskyghi %))))
      (filter bounds-filter)
      (make-windows job)
      (map #(assoc % :t0 (:timestamp (last (:data %)))))
      (filter #(has-no-gap? (:full-seq %)))
      (map #(assoc % :data (take-nth (:history-spacing job) (:data %)))) ;; from 10-secondly to x-secondly
      (filter zenith-filter)
      (filter round-filter)
      (filter ramp-filter)
      (map #(assoc % :json (sample->step-json %)))
      (map #(assoc % :image-map (sample->filename-map %))))
     (datafile->samples job))
    )
  )

(defn job-samples->cropped-images!
  #_(job-samples->cropped-images! sample-job test-samples)
  "Make the folder of resized images"
  [{:keys [cache-zip image-zip tmp-image-dir use-cache] :as job} samples]
  (if (and (file/exists? cache-zip) use-cache)
    (do
      (log/debug (str "Using cache file " cache-zip))
      (zip/extract-zip-to-disk cache-zip tmp-image-dir))
    (let [python-instr-file (file/resolve-path [tmp-image-dir "crops.json"])
          python-data
          {:images
           (set (->> samples
                     (map #(:data %))
                     (mapcat (fn [sample]
                               (map (fn [point] (assoc (:crop-params point)
                                                       :original-filename (:original-filename point)
                                                       :filename (:filename point)))
                                    sample)))
                     ))
           :image-zip image-zip
           :tmp-image-dir tmp-image-dir
           }]
      (file/make-dirs tmp-image-dir)
      (spit python-instr-file (json/write-str python-data))
      (-> (extern/make-executor ["python3" "/app/src/image/crop_resize.py" python-instr-file])
          (.exitValue (int 0))
          (.execute))
      (file/delete python-instr-file)
      (file/make-dirs (file/parent cache-zip))
      (zip/folder->zip tmp-image-dir cache-zip)
      )))


(defn calculate-history-steps
  #_(calculate-history-steps 2 20 10)
  "Convert from number of input terms and spacing to number of history steps and spacing.
   inputs are assumed to be in the same units (e.g. seconds)
  "
  [num-input-terms input-spacing dataset-spacing]
  (let [history-spacing (/ input-spacing dataset-spacing)]
    {:history-steps (- (* num-input-terms history-spacing) (dec history-spacing))
     :history-spacing history-spacing}))

(defn calculate-horizon-steps
  #_(calculate-horizon-steps (* 7 60) 10)
  "Calculate how many steps are required to reach the desired horizon.
   `horizon` and `dataset-spacing` are assumed to be in the same units (e.g. seconds)
  "
  [horizon dataset-spacing]
  {:horizon-steps (/ horizon dataset-spacing)})

(defn job->tar!
  #_(job->tar! sample-job)
  [{:keys [tmp-image-dir tar-assembly-dir output-tar] :as job}]
  (let [samples (job->samples job)]
    (try
      (job-samples->cropped-images! job samples)
      (samples->folder! job samples)
      (file/make-dirs (file/parent output-tar))
      (tar/flat-folder->sorted-tar tar-assembly-dir output-tar)
      (finally
        (run! #(when (file/exists? %)
                 (log/debug (str "Deleting " %))
                 (file/delete-recursive %))
              [tmp-image-dir tar-assembly-dir])))))

(defn main
  ""
  [config]
  (let [jobs (config->jobs config)]
    (log/debug "processing jobs")
    (runner/run-jobs!
     {:jobs
      (->> jobs
           (map (fn [job] #(job->tar! job))))})
    ))

(comment
  (def sample-config
    {:start-date "2015-01-01"
     :end-date "2016-01-01"
     :history-steps 91
     :history-spacing 6
     :horizons-steps [42]
     :data-dir "/data/blackmountain/irradiance"
     :image-dir "/data/blackmountain/images"
     :lens-model-path "/data/blackmountain/images/calib_results-Blackmountain.txt"
     :filename-prefix "BlackMountain_"
     :filename-suffix "_cloud_projection.csv.gz"
     :img-width 128
     :crop-size 512
     :tmp-dir "/tmp/solpred"
     :tmp-image-dir "/work/tmp_images-128"
     :cache-image-dir "/work/image_cache-128"
     :output-dir "/work/blackmountain-128/bm_2_20_420_dates"
     :columns #{"Timestamp" "GlobalCMP11Physical" "ClearSkyGHI" "DirectCHP1Physical" "DiffuseCMP11Physical" "SunZenith" "SunAzimuth"}
     :round-minute? true
     :std-dev? false
     :remove-high-zenith? false
     :ramps-only? false
     :remove-out-of-bounds? false})

  (def sample-job (first (config->jobs sample-config)))

  (def test-samples (job->samples sample-job))
                   
  (main config)

  (def test-date (LocalDate/parse "2015-01-01"))

  (iterator-seq (.iterator (.datesUntil (LocalDate/parse "2015-01-01") (LocalDate/parse "2015-01-31"))))

  (def data (tc/dataset (str (:data-dir config) "/" (str (:filename-prefix config) (time/localdate->string test-date "yyyy-MM-dd") (:filename-suffix config)))))

  (take 10 (ds/mapseq-reader (tc/select-columns data (:columns config))))

  (file/list-children "/data")

  (file/list-children "/work/image_cache")

  (file/exists? "/work/image_cache/2015-01-31.zip")
  (file/exists? "/data/blackmontain/images/2015/2015-01-31.zip")

  )
