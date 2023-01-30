(ns solpred.dataset.verifier
  "Verify filesystem
  "
  (:require
   [clojure.tools.logging :as log]
   [solpred.util.runner :as runner]
   [solpred.util.file :as file])
  (:import
   (java.io File)
   (java.nio.file Path Paths Files FileVisitOption LinkOption OpenOption CopyOption)))

(defn calc-bm-runs
  ""
  [{:keys [dataset] :as args}]
  (log/info "Starting.")
  (->> (for [terms [2 4 8 16] spacing [20 40 60 120] horizon [420]]
         (let [base-disk "/scratch2"
               run-name (str terms "x" spacing "s_" horizon "s")
               base-dir (file/resolve-path [base-disk "won10v" "work" dataset])
               shard-dir (file/resolve-path [base-dir (str "shards_" run-name)])]
           shard-dir))
       (mapcat (fn [shard-dir]
              (concat (for [fold (range 0 10) train (range 0 37)]
                        (file/resolve-path [shard-dir (str "fold" fold) (format "train_%04d.tar" train)]))
                      (for [fold (range 0 10) val (range 0 4)]
                        (file/resolve-path [shard-dir (str "fold" fold) (format "val_%04d.tar" val)]))
                      (for [test (range 0 15)]
                        (file/resolve-path [shard-dir (format "test_%04d.tar" test)])))))
       (map (fn [path] {:path path
                        :exists? (file/exists? path)}))
       (filter (fn [{:keys [exists? size-okay?]}] (not exists?)))
       (run! (fn [{:keys [path]}] (log/info (str path " is missing")))))
  (log/info "Done."))

(defn main
  ""
  [args]
  (calc-bm-runs args))
