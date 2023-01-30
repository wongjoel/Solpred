(ns solpred.dataset.shard-maker
  "Take Tar files of days and make Cross-Validation friendly shards"
  (:require
   [solpred.util.file :as file]
   [solpred.util.tar :as tar]
   [solpred.util.runner :as runner]
   [solpred.util.runtime-check :as runtime-check]
   [clojure.string :as str]
   [clojure.math :as math]
   [clojure.tools.logging :as log]
   [clojure.java.io :as io]))

(defn split-days-to-train-val-test!
  "Add trainval and test prefixes to tar files"
  [{:keys [dataset-dir test-prefix trainval-prefix output-dir] :as config}]
  (file/make-dirs output-dir)
  (->> (file/list-children dataset-dir)
       (map-indexed (fn [index item] (let [fname (file/filename item)]
                                       {:origin-path item
                                        :output-path (if (= 0 (mod index 10))
                                                       (file/resolve-path [dataset-dir (str test-prefix fname)])
                                                       (file/resolve-path [dataset-dir (str trainval-prefix fname)]))
                                        })))
       (run! (fn [{:keys [origin-path output-path]}] (file/move origin-path output-path))))
  )

(defn make-test-shards-list
  "Find all the tar files with the test prefix and make them into shards"
  #_(make-test-shards! {:dataset-dir "/data/sunset/freq2"
                       :partition-size 5
                       :test-prefix "test_data"
                        :output-dir "/data/shards/freq2"})
  [{:keys [dataset-dir partition-size test-prefix output-dir] :as config}]
  (->> (file/list-children dataset-dir)
       (filter (fn [x] (str/starts-with? (file/filename x) test-prefix)))
       (map (fn [x] (file/as-file x)))
       (partition-all partition-size)
       (map-indexed (fn [index item] {:output-path (file/resolve-path [output-dir (str test-prefix (format "%04d" index) ".tar")])
                                      :tars item}))))

(defn make-test-shards!
  "Find all the tar files with the test prefix and make them into shards"
  #_(make-test-shards! {:dataset-dir "/data/sunset/freq2"
                       :partition-size 5
                       :test-prefix "test_data"
                       :output-dir "/data/shards/freq2"})
  [{:keys [dataset-dir output-dir] :as config}]
  (runtime-check/throw-on-true (= 0 (count (file/list-children dataset-dir))) (str "dataset dir is empty: " dataset-dir))
  (file/make-dirs output-dir)
  (run! (fn [{:keys [output-path tars] :as x}]
          (if (file/exists? output-path)
            (log/debug (str "Skipping " output-path " as it already exists"))
            (tar/combine-tars tars output-path)))
        (make-test-shards-list config)))

(defn calc-trainval-shards-for-fold-by-position
  "Find all the tar files with the trainval prefix and make them into shards based on their position"
  #_(calc-trainval-shards-for-fold-by-position
     {:trainval-data (vec (map #(str "path" %) (range 0 100)))
      :partition-size 5
      :fold 4
      :output-dir "/data/shards/freq2"})
  [{:keys [trainval-data partition-size val-prefix train-prefix fold output-dir] :as config}] 
  (let [shuffled-data (shuffle trainval-data)
        val-data (subvec shuffled-data fold (+ fold (* 4 partition-size))) ;; TODO: These indicies don't do K-fold validation correctly - they actually do something more like a Monte Carlo cross-validation.
        train-data (filter (fn [x] (not (contains? (set val-data) x))) shuffled-data)
        val-shards (->> val-data
                        (map (fn [x] (file/as-file x)))
                        (partition-all partition-size)
                        (map-indexed (fn [index item]
                                       {:output-path (file/resolve-path
                                                      [output-dir
                                                       (str "fold" fold)
                                                       (str val-prefix (format "%04d" index) ".tar")])
                                        :tars item})))
        train-shards (->> train-data
                          (map (fn [x] (file/as-file x)))
                          (partition-all partition-size)
                          (map-indexed (fn [index item]
                                         {:output-path (file/resolve-path
                                                        [output-dir
                                                         (str "fold" fold)
                                                         (str train-prefix (format "%04d" index) ".tar")])
                                          :tars item})))]
    (concat val-shards train-shards)))



(defn make-trainval-shards-by-position!
  #_(make-trainval-shards-by-position!
     {:dataset-dir "/work/sunset/freq1"
      :num-folds 10
      :trainval-prefix "trainval_data"
      :train-prefix "train_data"
      :val-prefix "val"
      :output-dir "/work/sunset/shards_16x60s_420s"})
  "Find all the tar files with the trainval prefix and make them into shards based on their position"
  [{:keys [dataset-dir trainval-prefix num-folds] :as config}]
  (runtime-check/throw-on-true (= 0 (count (file/list-children dataset-dir))) (str "dataset dir is empty: " dataset-dir))
  (let [trainval-data (into [] (filter (fn [x] (str/starts-with? (file/filename x) trainval-prefix))) (file/list-children dataset-dir))
        partition-size (int (math/round (/ (count trainval-data) (* 4 num-folds))))
        tasks (->> (for [fold (range 0 num-folds)]
                    (assoc config
                           :fold fold
                           :trainval-data trainval-data
                           :partition-size partition-size))
                   (mapcat calc-trainval-shards-for-fold-by-position))
        jobs (->> tasks
                  (filter (fn [{:keys [output-path]}] (not (file/exists? output-path))))
                  (map (fn [{:keys [output-path tars] :as x}]
                              #(do
                                (file/make-dirs (file/parent output-path))
                                (tar/combine-tars tars output-path)))))]
    (runner/run-jobs!
     {:jobs jobs})))



(comment
  (make-trainval-shards-by-position!
   {:dataset-dir "/work/sunset/freq1"
    :num-folds 10
    :trainval-prefix "trainval_data"
    :train-prefix "train_"
    :val-prefix "val_"
    :output-dir "/work/sunset/shards_16x60s_420s"})

  (make-test-shards!
   {:dataset-dir "/work/sunset/freq1"
    :partition-size 5
    :test-prefix "test_"
    :output-dir "/work/sunset/shards/shards_16x60s_420s"})

  (make-trainval-shards-by-position!
   {:dataset-dir "/data/blackmountain/bm_1m_7m_15m_prefixed"
    :num-folds 10
    :trainval-prefix "trainval_"
    :train-prefix "train_"
    :val-prefix "val_"
    :output-dir "/data/blackmountain/shards_1m_7m_15m"})

  (make-test-shards!
   {:dataset-dir "/work/blackmountain-1024-64/bm_2x60s_120s_prefixed"
    :partition-size 5
    :test-prefix "test_"
    :output-dir "/work/blackmountain-1024-64/shards_2x60s_120s"})
  )
