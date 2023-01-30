(ns solpred.util.stats
  (:require
   [clojure.pprint :as pprint]
   [clojure.string :as str]
   [clojure.math :as math]
   [tablecloth.api :as tc]
   [tech.v3.dataset :as ds])
  (:import
   (java.time LocalDate LocalDateTime Duration)
   (java.util.concurrent Executors)
   (org.apache.commons.math3.stat.descriptive DescriptiveStatistics)))


(defn mean
  #_(mean [1 2 3 4])
  "Calculate mean with Apache stats"
  [input-seq]
  (let [stats (DescriptiveStatistics.)]
    (run! (fn [value] (.addValue stats value)) input-seq)
    (.getMean stats)))
