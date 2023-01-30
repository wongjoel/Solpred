(ns solpred.reports.mass-reports
  (:require [solpred.util.file :as file]
            [solpred.reports.single-day :as single-day]
            [clojure.edn :as edn]
            [clojure.string :as str]
            [tablecloth.api :as api]
            [tech.v3.dataset :as ds]
            [oz.core :as oz])
  (:import (java.time LocalDate LocalDateTime Duration)))

(defn calculate-irradiance-filename
  ""
  [{:keys [filename-prefix filename-suffix] :as config} date]
  (str filename-prefix date filename-suffix))

(defn mass-reports
  ""
  [{:keys [datasets-path data-dir image-dir] :as config}]
  (->> (edn/read-string (slurp datasets-path))
       (map (fn [x] (merge config x)))
       (map (fn [x] (assoc x :date (LocalDate/parse (:date x)))))
       (map (fn [x] (assoc x :page-title (str (:date x)))))
       (map (fn [x] (assoc x :skycam-data (file/resolve-path [data-dir (calculate-irradiance-filename config (:date x))]))))
       (map (fn [x] (assoc x :image-data (file/resolve-path [image-dir (str (:date x) ".zip")]))))
       (map (fn [x] (assoc x :image-extension ".webp")))
       (map (fn [x] (assoc x :image-pattern "yyyy-MM-dd/yyyy-MM-dd_HH-mm-ss")))
       (run! (fn [x] (oz/export! (single-day/gen-report x) (str "siteout/" (:date x) ".html"))))
       ))

(comment
  (mass-reports config)

  (def config {:datasets-path "resources/datasets/datasets.edn"
               :data-dir "/media/celthana/Data/BlackMountain_2014-2016_Full"
               :image-dir "/media/celthana/Data/images/2015-resized"
               :filename-prefix "BlackMountain_"
               :filename-suffix "_cloud_projection.csv.gz"
               :solpred-scripts "solpredscript.js"
               :solpred-css "solpredstyles.css"
               :page-title "Day Sequence Report"})

  (def data (api/dataset "resources/data/data.csv.gz"))

  (api/select-columns data #{"Timestamp" "TimestampPhysical" "UnixTimestamp"})

  (mapcat identity (ds/value-reader (api/select-columns data #{"Timestamp"})))

  (oz/start-server!)
  (oz/view! (report-data-summary data))

  (oz/build! [#_{:from "src/solpred/dataset/site/"
               :to "siteout/"}
              {:from "src/solpred/dataset/site-assets/"
               :to "siteout/"
               :as-assets? true}])

  (identify-time-issues (mapcat identity (ds/value-reader (api/select-columns data #{"Timestamp"}))))
  )
