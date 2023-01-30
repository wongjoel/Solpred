(ns solpred.dataset.webdsmaker
  "Takes a flat filesystem dataset and makes it into a webdataset"
  (:require
   [solpred.util.pprint-edn :as pprint-edn]
   [solpred.dataset.dsmaker :as dsmaker]
   [solpred.dataset.shard-maker :as shard-maker]
   [solpred.dataset.split-ds :as split-ds]
   [solpred.util.file :as file]
   [clojure.tools.logging :as log]
   [clojure.edn :as edn]
   [clojure.java.io :as io]))

(defn make-samples!
  "Make dataset samples"
  [config]
  (log/info "Make samples")
  (dsmaker/main config))

(defn split-samples!
  "Split dataset samples into test and trainval"
  [config]
  (log/info "Split samples")
  (split-ds/main config))

(defn make-shards!
  "Make shards of the dataset"
  [config]
  (log/info "Make shards")
  (shard-maker/make-trainval-shards-by-position! config)
  (shard-maker/make-test-shards! config))

(defn generate-config
  #_(generate-config {:history-steps 91 :history-spacing 6 :horizons-steps [42] :base-filename "1m_7m_15m" :img-width 64 :base-dir "/work/blackmountain/" :round-minute? false})
  "Make a config structure"
  [{:keys [history-steps history-spacing horizons-steps base-filename img-width base-dir crop-size round-minute? ramps-only? remove-high-zenith? remove-out-of-bounds?]}]
  (let [start-date "2015-01-01"
        end-date "2016-01-01"
        irradiance-dir "/data/blackmountain/irradiance"
        base-out-dir (str base-dir (when ramps-only? "-ramp") (when round-minute? "-round") "-" crop-size "-" img-width)
        tmp-dir "/tmp/solpred"]
    {:dsmaker
     {:start-date start-date
      :end-date end-date
      :history-steps history-steps
      :history-spacing history-spacing
      :horizons-steps horizons-steps
      :data-dir irradiance-dir
      :tmp-dir tmp-dir
      :image-dir "/data/blackmountain/images"
      :lens-model-path "/data/blackmountain/images/calib_results-Blackmountain.txt"
      :filename-prefix "BlackMountain_"
      :filename-suffix "_cloud_projection.csv.gz"
      :img-width img-width
      :crop-size crop-size
      :tmp-image-dir (file/resolve-path [tmp-dir (str (rand-int 1000)) (str "tmp_images-"  crop-size "-" img-width)])
      :use-cache false
      :cache-image-dir (str "/work/image_cache-" crop-size "-" img-width)
      :output-dir (file/resolve-path [base-out-dir (str "bm_" base-filename "_dates")])
      :columns #{"Timestamp" "GlobalCMP11Physical" "ClearSkyGHI" "DirectCHP1Physical" "DiffuseCMP11Physical" "SunZenith" "SunAzimuth"}
      :round-minute? round-minute?
      :ramps-only? ramps-only?
      :ramp-threshold 10
      :remove-high-zenith? remove-high-zenith?
      :zenith-threshold 80
      :remove-out-of-bounds? remove-out-of-bounds?}
     :split-ds
     {:cache-file (file/resolve-path [base-out-dir "split-cache.edn"])
      :start-date start-date
      :end-date end-date
      :dir irradiance-dir
      :prefix "BlackMountain_"
      :suffix "_cloud_projection.csv.gz"
      :in-dir (file/resolve-path [base-out-dir (str "bm_" base-filename "_dates")])
      :out-dir (file/resolve-path [base-out-dir (str "bm_" base-filename "_prefixed")])}
     :shard-maker
     {:dataset-dir (file/resolve-path [base-out-dir (str "bm_" base-filename "_prefixed")])
      :num-folds 10
      :partition-size 5
      :trainval-prefix "trainval_"
      :train-prefix "train_"
      :val-prefix "val_"
      :test-prefix "test_"
      :output-dir (file/resolve-path [base-out-dir (str "shards_" base-filename)])}}))

(defn config->file!
  #_(config->file! "test.edn" {:history-steps 91 :history-spacing 6 :base-filename "2x20s_42steps"})
  "Write the config to disk"
  [outfile config-data]
  (file/make-dirs (file/parent outfile))
  (pprint-edn/spit-pprint (generate-config config-data) outfile))

(defn main
  "Execute the main function of this ns"
  [config]
  (make-samples! (:dsmaker config))
  (split-samples! (:split-ds config))
  (make-shards! (:shard-maker config)))

(defn main-local
  "Execute the main function of this ns
  Example:
  ```
  clj
  (require '[solpred.dataset.webdsmaker :as webdsmaker])
  (webdsmaker/main-local)
  ```
  "
  []
  (main (generate-config {:history-steps 2 :history-spacing 2 :horizons-steps [42] :base-filename "2x20s_420s"})))

(comment
  (main-local)

  (->> (for [terms [2] spacing [60] img-width [64] horizon [900] crop-size [#_"full" (* 64 16) #_(* 64 8)]]
         {:input-terms terms :input-spacing spacing :horizon horizon :img-width img-width :crop-size crop-size})
       (map (fn [{:keys [input-terms input-spacing horizon img-width crop-size]}]
              (assoc
               (dsmaker/calculate-history-steps input-terms input-spacing 10)
               :config-filename (str "configs/bm_" input-terms "_" input-spacing "s" "_" horizon "s" "-" crop-size "-" img-width "px" ".edn")
               :horizons-steps [(:horizon-steps (dsmaker/calculate-horizon-steps horizon 10))]
               :base-filename (str input-terms "x" input-spacing "s_" horizon "s")
               :img-width img-width
               :crop-size crop-size
               :base-dir "/work/blackmountain"
               :round-minute? false
               :ramps-only? false
               :remove-high-zenith? false
               :remove-out-of-bounds? false
               )))
       (run! (fn [x] (config->file! (:config-filename x) x))))

  (file/exists? "configs")

  (defn load-edn
  "Load edn from an io/reader source (filename or io/resource)."
  [source]
  (try
    (with-open [r (io/reader source)]
      (edn/read (java.io.PushbackReader. r)))
    (catch java.io.IOException e
      (log/error (str "Couldn't open " source ": " (.getMessage e)))
      (log/debug (str "Tried to open " (.toAbsolutePath (.toPath (io/as-file source)))))
      (throw e))
    (catch RuntimeException e
      (log/error (str "Error parsing edn file " source ": " (.getMessage e)))
      (throw e))))

  (file/exists?  "configs/bm_2_60s_120s-1024-64px.edn")
  (make-samples! (:dsmaker (load-edn "configs/bm_2_60s_120s-1024-64px.edn")))

  )
