(ns solpred.dataset.update-sunset-tars
  "Update SUNSET tar datasets to conform to expected dataset format"
  (:require
   [solpred.util.file :as file]
   [clojure.edn :as edn]
   [clojure.string :as str]
   [tablecloth.api :as api]
   [tech.v3.dataset :as ds])
  (:import
   (java.time LocalDate LocalDateTime Duration)))

(defn calculate-output-paths
  #_(calculate-output-paths {:input-dir "/work/sunset/input"
                             :extract-dir "/work/sunset/extract"
                             :staging-dir "/work/sunset/staging"
                             :output-dir "/work/sunset/output"})
  "Make map of output paths"
  [{:keys [input-dir extract-dir staging-dir output-dir]}]
  (->> (file/list-children input-dir)
       (map (fn [child] {:path child
                         :in-name (file/filename child)}))
       (map (fn [{:keys [in-name] :as x}]
              (assoc x :extract-dir (file/resolve-path [extract-dir (str/replace in-name #".tar" "")])
                     :staging-dir (file/resolve-path [staging-dir (str/replace in-name #".tar" "")])
                     :out-tar (file/resolve-path [output-dir in-name]))))))

(comment
  (def config {:input-dir "/work/sunset/input"
               :extract-dir "/work/sunset/extract"
               :staging-dir "/work/sunset/staging"
               :output-dir "/work/sunset/output"})
  

  

  )
