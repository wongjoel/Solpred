(ns solpred.util.csv
  (:require
   [clojure.string :as str]
   [clojure.data.csv :as csv]
   [clojure.java.io :as io])
  (:import
   (org.apache.commons.compress.compressors.gzip GzipCompressorInputStream)
   (java.time.format DateTimeFormatter)))

(defn write-csv!
  "Writes a seq of seqs to CSV"
  #_(write-csv! [["a" "b" "c"] [1 2 3] [4 5 6]] "test.csv")
  [csv-data out-file]
  (with-open [writer (io/writer out-file)]
    (csv/write-csv writer csv-data)))

(defn replace-blanks
  "Takes a vector of strings and replaces blank strings with unnamed-<count>"
  [v]
  (:col (->> v
            (reduce (fn [x val] (if (str/blank? val)
                                  {:count (inc (:count x))
                                   :col (conj (:col x) (str "unnamed-" (:count x)))}
                                  {:count (:count x)
                                   :col (conj (:col x) val)}))
                    {:count 0 :col []})
            )))

(defn csv-data->maps [csv-data]
  (map zipmap
       (->> (first csv-data) ;; First row is the header
            replace-blanks ;; Ensure blank keys are replaced (pandas likes to add blank header bits)
            (map keyword)
            repeat)
       (rest csv-data)))

(defn read-csv
  "Read csv into maps"
  [filename]
  (with-open [reader (cond
                       (str/ends-with? filename ".csv") (-> filename
                                                   io/reader)
                       (str/ends-with? filename ".csv.gz") (-> filename
                                                      io/input-stream
                                                      GzipCompressorInputStream.
                                                      io/reader)
                       :else (throw (UnsupportedOperationException. (str "Unsupported file " filename))))]
  (doall
   (csv-data->maps (csv/read-csv reader)))))

(defn update-map
  [x f]
  (reduce-kv (fn [m k v]
               (assoc m k (f v))) {} x))

(defn parse-int-keys
  [in-map chosen-keys]
  (let [parsed-map (->> (select-keys in-map chosen-keys)
                        (reduce-kv (fn [m k v]
                                     (assoc m k (parse-long v))) {}))]
    (merge in-map parsed-map)))

(defn parse-double-keys
  [in-map chosen-keys]
  (let [parsed-map (->> (select-keys in-map chosen-keys)
                        (reduce-kv (fn [m k v]
                                     (assoc m k (parse-double v))) {}))]
    (merge in-map parsed-map)))

(defn parse-timestamp-keys
  [in-map chosen-keys time-pattern]
  (let [formatter (DateTimeFormatter/ofPattern time-pattern)
        parsed-map (->> (select-keys in-map chosen-keys)
                        (reduce-kv (fn [m k v]
                                     (assoc m k (.parse formatter v))) {}))]
    (merge in-map parsed-map)))

(parse-long "1")
(comment
  (read-csv "resources/data.csv.gz")
  (->> [{:a "1" :b "2" :c "3"}
      {:a "4" :b "5" :c "6"}]
     (map #(parse-double-keys % [:a :b])))
  )
