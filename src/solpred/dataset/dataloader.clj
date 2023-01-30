(ns solpred.dataset.dataloader
  "Get data from disk and return it as either a seq or channel of maps"
  (:require
   [solpred.util.file :as file]
   [clojure.core.async :as async]
   [clojure.edn :as edn]
   [clojure.string :as str]
   [tablecloth.api :as api]
   [tech.v3.dataset :as ds])
  (:import
   (java.time LocalDate LocalDateTime Duration)))

(defn dates->paths
  "Calculates the paths from the date and config"
  [{:keys [filename-prefix filename-suffix data-dir] :as config} dates]
  (into [] (comp
            (map (fn [date] (str filename-prefix date filename-suffix)))
            (map (fn [filename] (file/resolve-path [data-dir filename]))))
        dates))

(defn date->path
  "Calculates the path from the date and config"
  #_(date->path "2020-01-01" {:filename-prefix "abc" :filename-suffix "def.csv.gz" :data-dir "/data"})
  [date {:keys [filename-prefix filename-suffix data-dir] :as config}]
  (file/resolve-path [data-dir (str filename-prefix date filename-suffix)]))

(defn path->seq-of-map
  "Reads data from path, selects columns and converts to a seq of maps
  `path`: path to data file
  `columns`: set of column names to select"
  #_(path->seq-of-map "/data/abc_2015-01-01.csv.gz" #{"col1" "col2" "col3"})
  [path columns]
  (-> path
      (api/dataset)
      (api/select-columns columns)
      (ds/mapseq-reader)))

(comment (-> (ds/->dataset [{:a 1 :b 2} {:a 3 :b 4} {:a 5 :b 6} {:a 7 :b 8} {:a 9 :b 10}])
             (ds/mapseq-reader)
             (shuffle)))

(defn path->channel
  "Reads data from path, selects columns and returns channel returning data as maps
  Will back the task populating the channel with the given threadpool"
  [path columns threadpool]
  )

(defn date->seq-of-maps
  "Takes a date and returns a seq of maps containing the requested data for that date
  `filename-prefix`: part of filename before date
  `filename-suffix`: part of filename after date
  `data-dir`: parent directory for files
  `columns`: set of columns to use
  `date`: date to get"
  [date {:keys [columns] :as config}]
  (path->seq-of-map (date->path date config) columns)
  )

(defn dates->seq-of-maps
  "Takes a seq of dates and returns a seq of maps containing the requested data for those dates
  `filename-prefix`: part of filename before date
  `filename-suffix`: part of filename after date
  `data-dir`: parent directory for files
  `columns`: set of columns to use
  `dates`: seq of dates to get"
  [{:keys [columns] :as config} dates]
  (let [paths (dates->paths config dates)]
    (into [] (mapcat (fn [path] (path->seq-of-map path columns))) paths))
  )

(defn dates->channel
  "Takes a seq of dates and returns a channel which will return the requested data for those dates"
  [{:keys [filename-prefix filename-suffix data-dir columns] :as config} threadpool dates]
  (let [paths (dates->paths config dates)]
    (into [] (mapcat (fn [path] (path->seq-of-map path columns))) paths))
  )


(comment
  (defn sender-thread1
    ""
    [channel]
    (let [data (sequence (map (fn [x] (println (str "generated " x)) x) (range 10)))]
      (run! (fn [x]
              (println (str "Put " x))
              (async/>!! channel x)) data)
      (async/close! channel)))

  (defn sender-thread2
    ""
    [channel]
    (let [data (sequence (map (fn [x] (println (str "generated " x)) x) (range 40 50)))]
      (run! (fn [x]
              (println (str "Put " x))
              (async/>!! channel x)) data)
      (async/close! channel)))

  (defn receiver-thread
    ""
    [channel]
    (loop []
      (when-let [value (async/<!! channel)]
        (println (str "Received " value))
        (recur)))
    (println "Channel Closed"))

  (def channel1 (async/chan 1 (comp
                               (map (fn [x] (println (str "channel1 " x)) x))
                               (map (fn [x] (* 10 x))))))

  (def channel2 (async/chan 1 (comp
                               (map (fn [x] (println (str "channel2 " x)) x))
                               (map (fn [x] (* 10 x))))))

  (def merged-channels (async/merge [channel1 channel2] 2))
  (def model-channel (async/chan 1 (map (fn [x] (println (str "model " x)) x))))
  (async/pipe merged-channels model-channel)

  (async/thread (sender-thread1 channel1))
  (async/thread (sender-thread2 channel2))

  (async/<!! (async/into [] model-channel))
  (async/thread (receiver-thread model-channel))
  )

(comment
  (def config {:solpred-scripts "solpredscript.js"
               :solpred-css "solpredstyles.css"
               :page-title "Single Day Report"
               :skycam-data "resources/data/data.csv.gz"
               :image-data "/data/images/2015-resized/2015-01-31.zip"
               :image-extension ".webp"
               :image-pattern "yyyy-MM-dd/yyyy-MM-dd_HH-mm-ss"})

  (def data (api/dataset (:skycam-data config)))

  (def seq-data (ds/mapseq-reader (api/select-columns data #{"Timestamp" "GlobalCMP11Physical"})))


  )
