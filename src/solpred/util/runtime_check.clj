(ns solpred.util.runtime-check
  "Runtime checks in a namespace
  "
  (:require
   [clojure.tools.logging :as log]))

(defn throw-ill-arg
  [message]
  (throw (IllegalArgumentException. message)))

(defn throw-on-false
  #_(throw-on-false true "error message")
  [predicate message]
  (when-not predicate (throw-ill-arg message)))

(defn throw-on-true
  #_(throw-on-true true "error message")
  [predicate message]
  (when predicate (throw-ill-arg message)))

(defn map-contains?
  #_(map-contains? {:a 1 :b 2 :c 3 :d 4} [:a :b :c])
  #_(map-contains? {:a nil} [:a :b])
  [check-map check-keys]
  (run! #(throw-on-false (contains? check-map %) (str % " not found in " check-map)) check-keys))
