(ns solpred.util.pprint-edn
  "pprint edn output"
  (:require
   [clojure.pprint :as pprint]
   [clojure.java.io :as io]
   ))

(defn spit-pprint
  #_(spit-pprint {:a-long-word "a longer string" :b (range 20) :c 3 :d 4} "dummy-file.edn")
  [object path]
  (pprint/pprint object (io/writer path)))
