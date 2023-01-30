(ns solpred.dataset.split-ds
  "Split days into train/val and test sets"
  (:require
   [solpred.util.file :as file]
   [solpred.util.time :as time]
   [solpred.util.csi :as csi]
   [solpred.util.pprint-edn :as edn-out]
   [solpred.util.runtime-check :as runtime-check]
   [clojure.edn :as edn]
   [clojure.tools.logging :as log]
   [clojure.java.io :as io]
   [clojure.math :as math]
   [tablecloth.api :as tc]
   ))

(defn gen-random-split
  #_(gen-random-split 3 2 (range 100))
  "Split the input collection in the given proportion randomly"
  [trainval-proportion test-proportion coll]
  (let [num-elements (count coll)
        split-point (* (math/ceil (/ num-elements (+ trainval-proportion test-proportion))) trainval-proportion)
        splits (split-at split-point (shuffle coll))]
    {:trainval (vec (first splits))
     :test (vec (second splits))}))

(defn gen-sequential-split-xf
  #_(sequence (gen-sequential-split-xf 4 2) (range 10))
  "Helper transducer for gen-sequential-split"
  [trainval-proportion test-proportion]
  (fn [xf]
    (let [reset-point (+ trainval-proportion test-proportion)
          trainval-count (volatile! 0)
          test-count (volatile! 0)]
      (fn
        ([] (xf))
        ([result] (xf result))
        ([result input]
         (when (>= (+ @trainval-count @test-count) reset-point)
           (vreset! trainval-count 0)
           (vreset! test-count 0))
         (if (and
              (<= @test-count @trainval-count)
              (< @test-count test-proportion))
           (do
             (vswap! test-count + 1)
             (xf result {:test input}))
           (do
             (vswap! trainval-count + 1)
             (xf result {:trainval input}))))))))

(defn gen-sequential-split
  #_(gen-sequential-split 2 2 (range 10))
  "Split the input collection in the given proportion in sequence"
  [trainval-proportion test-proportion coll]
  (reduce-kv
   (fn [m k v]
     (assoc m (first k) (mapv (fn [x] ((first k) x)) v)))
   {:trainval [] :test []}
   (group-by keys (sequence (gen-sequential-split-xf trainval-proportion test-proportion) coll))))

(defn gen-csi-split
  #_(gen-csi-split 2 2 [{:id 1 :csi 1}
                        {:id 2 :csi 1}
                        {:id 3 :csi 0.4}
                        {:id 4 :csi 0.4}
                        {:id 5 :csi 0.9}
                        {:id 6 :csi 0.1}
                        {:id 7 :csi 0.2}
                        {:id 7 :csi 0.3}])
  "Split the input collection in the given proportion, first sorting by CSI"
  [trainval-proportion test-proportion coll]
  (gen-sequential-split trainval-proportion test-proportion (sort-by :csi coll)))

(defn gen-file-seq
  #_(gen-file-seq {:start-date "2015-01-01"
                   :end-date "2016-01-01"
                   :dir "/data/blackmountain/irradiance"
                   :prefix "BlackMountain_"
                   :suffix "_cloud_projection.csv.gz"})
  "Get a seq of files in a dataset"
  [{:keys [start-date end-date dir prefix suffix]}]
  (map (fn [x] {:date (str x) :file (file/resolve-path [dir (time/localdate->string x "yyyy") (str prefix x suffix)])})
       (time/gen-date-seq (time/string->localdate start-date) (time/string->localdate end-date))))

(defn make-splits!
  "Write the splits to file"
  [{:keys [in-dir out-dir trainval-ds test-ds]}]
  (runtime-check/throw-on-true (= 0 (count trainval-ds)) (str "ds is empty: " trainval-ds))
  (runtime-check/throw-on-true (= 0 (count test-ds)) (str "ds is empty: " test-ds))
  (file/make-dirs out-dir)
  (let [files (concat
               (map (fn [x] (assoc x
                                   :in-file (file/resolve-path [in-dir (str (:date x) ".tar")])
                                   :out-file (file/resolve-path [out-dir (str "trainval_" (:date x) ".tar")])))
                    trainval-ds)
               (map (fn [x] (assoc x
                                   :in-file (file/resolve-path [in-dir (str (:date x) ".tar")])
                                   :out-file (file/resolve-path [out-dir (str "test_" (:date x) ".tar")])))
                    test-ds))
        groups (group-by #(if (file/exists? (:out-file %)) :skip :process) files)]
    (->> (:skip groups)
         (run! #(log/debug (str "Skipping " (:out-file %) "because it exists"))))
    (->> (:process groups)
         (run! (fn [{:keys [in-file out-file]}]
                 (log/debug (str "Processing " out-file))
                 (io/copy (io/file in-file) (io/file out-file)))))
    ))

#_(filter (fn [x] (file/exists? (:in-file x))))
#_(run! )

(defn generate-csi-split-dataset
  "Generate csi splits"
  [config]
  (let [dataset (->> (gen-file-seq config)
                       (filter (fn [x] (file/exists? (:file x)))))
          dataset-with-csi (pmap (fn [x]
                                   (assoc x :mean-csi (csi/get-average-csi (tc/dataset (:file x))))) dataset)
        dataset-split-by-csi (gen-csi-split 4 1 dataset-with-csi)]
    {:trainval-ds (:trainval dataset-split-by-csi)
     :test-ds (:test dataset-split-by-csi)}))

(defn main
 "Main function"
  [{:keys [out-dir cache-file] :as config}]
  (let [{:keys [trainval-ds test-ds] :as result}
        (if (file/exists? cache-file)
          (edn/read-string (slurp cache-file))
          (generate-csi-split-dataset config))]
    (when-not (file/exists? cache-file) 
      (edn-out/spit-pprint
       {:trainval-ds trainval-ds :test-ds test-ds}
       cache-file))
    (make-splits! (assoc config
                         :trainval-ds trainval-ds
                         :test-ds test-ds))))

(comment
  (def config
    {:cache-file "/work/blackmountain-64/split-cache.edn",
     :start-date "2015-01-01",
     :end-date "2016-01-01",
     :dir "/data/blackmountain/irradiance",
     :prefix "BlackMountain_",
     :suffix "_cloud_projection.csv.gz",
     :in-dir "/work/blackmountain-64/bm_16x60s_420s_dates",
     :out-dir "/work/blackmountain-64/bm_16x60s_420s_prefixed"})

  (main config) 
  (generate-csi-splits config)
  (gen-file-seq config)

  (def bm-dataset
    (->> (gen-file-seq {:start-date "2015-01-01"
                        :end-date "2016-01-01"
                        :dir "/data/irradiance/BlackMountain_2014-2016_Full"
                        :prefix "BlackMountain_"
                        :suffix "_cloud_projection.csv.gz"})
         (filter (fn [x] (file/exists? (:file x))))
         ))

  (def bm-dataset-with-csi
    (pmap (fn [x]
            #_(println (str "processing " (:file x)))
            (assoc x :mean-csi (csi/get-average-csi (tc/dataset (:file x))))) bm-dataset))

  (def bm-dataset-split-by-csi
    (gen-csi-split 4 1 bm-dataset-with-csi))

  (def in-dir "/data/blackmountain/bm_1m_7m_15m_dates")
  (def out-dir "/data/blackmountain/bm_1m_7m_15m_prefixed")
  (file/make-dirs out-dir)

  (->> (:trainval bm-dataset-split-by-csi)
       (map (fn [x] (assoc x
                           :in-file (file/resolve-path [in-dir (str (:date x) ".tar")])
                           :out-file (file/resolve-path [out-dir (str "trainval_" (:date x) ".tar")]))))
       (filter (fn [x] (file/exists? (:in-file x))))
       (run! (fn [{:keys [in-file out-file]}] (io/copy (io/file in-file) (io/file out-file)))))

  (->> (:test bm-dataset-split-by-csi)
       (map (fn [x] (assoc x
                           :in-file (file/resolve-path [in-dir (str (:date x) ".tar")])
                           :out-file (file/resolve-path [out-dir (str "test_" (:date x) ".tar")]))))
       (filter (fn [x] (file/exists? (:in-file x))))
       (run! (fn [{:keys [in-file out-file]}] (io/copy (io/file in-file) (io/file out-file)))))

  #_([1 2 3 4 5] -> {:test [1 3] :train-val [2 4 5]})

  (let [in-dir "/work/blackmountain-1024-64/bm_2x60s_120s_dates"
        out-dir "/work/blackmountain-1024-64/bm_2x60s_120s_prefixed"]
    (->> (file/list-children in-dir)
         (map (fn [x] {:in-file (str x)
                       :out-file (file/resolve-path [out-dir (str "test_" (file/filename x))])}))
         (run! (fn [{:keys [in-file out-file]}] (io/copy (io/file in-file) (io/file out-file))))
         ))
  
  )


