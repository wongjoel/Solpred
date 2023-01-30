(ns solpred.side.nubots
  "This is to allow testing of report generation
  "
  (:require [solpred.util.file :as file]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.data.json :as json]
            [oz.core :as oz]
            [solpred.util.oz-util :as oz-util])
  (:import (java.time Instant LocalDate LocalDateTime ZonedDateTime Duration)
           (java.util.concurrent TimeUnit)
           (java.time.format DateTimeFormatter)))

(defn outer-vs-inner
  [data]
  (oz-util/point-line-chart {:config {:title "Outer Timestamp vs Inner Timestamp"
                                       :x-title "Outer Timestamp"
                                       :x-field "outer-timestamp"
                                       :y-title "Inner Timestamp"
                                       :y-field "inner-timestamp"}
                              :values data}))

(defn generate-hiccup
  ""
  [data]
  [:div
   (outer-vs-inner data)
   ]
  )

(defn nbs->seqmap
 ""
 [file-path]
 (with-open [reader (clojure.java.io/reader file-path)]
    (->> (line-seq reader)
         (mapv (fn [x] (json/read-str x))))))

(def parse-data-pipeline
  (comp
   (filter (fn [point] (contains? #{"custom rtf ratio" "left_knee_pitch"} (get-in point ["data" "label"]))))
   (map (fn [point] {:outer-timestamp (get point "timestamp")
                     :inner-timestamp (get-in point ["data" "timestamp"])
                     :value (first (get-in point ["data" "value"]))
                     :series (get-in point ["data" "label"])}))
   (map (fn [point] (assoc point
                           :inner-timestamp (ZonedDateTime/parse (:inner-timestamp point)
                                                                 (DateTimeFormatter/ofPattern "yyyy-MM-dd'T'HH:mm:ss.nX")))))
   (map (fn [point] (assoc point
                           :inner-timestamp (.toInstant (:inner-timestamp point)))))
   (map (fn [point] (assoc point
                           :inner-timestamp (+ (.toMicros TimeUnit/SECONDS (.getEpochSecond (:inner-timestamp point)))
                                               (.toMicros TimeUnit/NANOSECONDS (.getNano (:inner-timestamp point))))
                           )))))

(defn shift-timestamps
  ""
  [parsed-data]
  (let [first-outer-ts (reduce min (map (fn [point] (:outer-timestamp point)) parsed-data))
        first-inner-ts (reduce min (map (fn [point] (:inner-timestamp point)) parsed-data))]
    (mapv (fn [point] (assoc point
                             :outer-timestamp (- (:outer-timestamp point) first-outer-ts)
                             :inner-timestamp (- (:inner-timestamp point) first-inner-ts)))
          parsed-data))
  )

(comment
  (def experiments
    (->> [{:name "normal_3" :path "/data/nubots/regular_3_success/3_normal.nbs.filtered.json"}
          {:name "normal_10" :path "/data/nubots/regular_10_fail/10_regular.nbs.filtered.json"}
          {:name "fixed_3" :path "/data/nubots/fixed_3_success/3_fixed.nbs.filtered.json"}
          {:name "fixed_10" :path "/data/nubots/fixed_10_fail/10_fixed.nbs.filtered.json"}]
         (map (fn [exp] (assoc exp :data (shift-timestamps (into [] parse-data-pipeline (nbs->seqmap (:path exp)))))))
         ))

  (run! (fn [exp] (oz/export!
                          (generate-hiccup (:data exp))
                          (str "/output/" (:name exp) ".html")
                          {:header-extras (oz-util/generate-header-extras {:page-title (:name exp)})}))
        experiments)

  (def data (shift-timestamps (into [] parse-data-pipeline (nbs->seqmap "/data/nubots/regular_3_success/3_normal.nbs.filtered.json"))))
  (oz/export! (generate-hiccup data) "/output/nubots.html" {:header-extras (oz-util/generate-header-extras {})})
  )
