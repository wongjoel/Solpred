(ns solpred.util.runner
  "Run jobs in parallel
  "
  (:require
   [clojure.tools.logging :as log])
  (:import
   (java.util.concurrent Executors)))

(defn run-jobs!
  "Run jobs in a threadpool"
  #_(run-jobs! {:jobs (map (fn [x] #(println (+ 1 x))) (range 200))})
  [{:keys [jobs num-workers] :or {num-workers (. (. Runtime getRuntime) availableProcessors)}}]
  (let [pool (Executors/newFixedThreadPool num-workers)
        job-partitions (partition-all num-workers jobs)] ; split jobs into partitions for early exceptions
    (->> job-partitions
         (run! (fn [job-group]
                 (->> job-group
                      (.invokeAll pool)
                      seq
                      (run! (fn [x] (.get x))))  ;Get the futures to ensure that exceptions are thrown
                      ))) 
    (.shutdown pool)))

(defn run-jobs-without-threadpool!
  "Run jobs without threadpool (i.e. for debugging)"
  #_(run-jobs-without-threadpool! {:jobs (map (fn [x] #(println (+ 1 x))) [1 2 3 4])})
  [{:keys [jobs]}]
  (log/debug "Running jobs without threadpool")
  (run! (fn [x] (x)) jobs))

