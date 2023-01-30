(ns solpred.reports.day-sequence
  (:require
   [clojure.pprint :as pprint]
   [solpred.util.file :as file]
   [solpred.util.time :as time]
   [solpred.reports.single-day :as single-day]
   [clojure.string :as str]
   [tablecloth.api :as api]
   [tech.v3.dataset :as ds]
   [solpred.util.oz-util :as oz-util]
   [oz.core :as oz])
  (:import
   (java.time LocalDate LocalDateTime Duration)))

(defn calculate-irradiance-filename
  ""
  [{:keys [filename-prefix filename-suffix] :as config} date]
  (str filename-prefix date filename-suffix))

(defn calculate-dates
  ""
  [{:keys [start-date end-date] :as config}]
  (time/gen-date-seq start-date end-date))

(defn calculate-dates-and-paths
  ""
  [{:keys [data-dir] :as config}]
  (->> (calculate-dates config)
       (map (fn [x] {:date x :path (file/resolve-path [data-dir (calculate-irradiance-filename config x)])}))))

(defn get-missing-days
  ""
  [config]
  (->> (calculate-dates-and-paths config)
       (filter (fn [x] (not (file/exists? (:path x)))))))

(defn group-days-partial-full
  ""
  [config]
  (->> (calculate-dates-and-paths config)
       (filter (fn [x] (file/exists? (:path x))))
       (pmap (fn [x] (assoc x :gap-count (count (single-day/get-time-issues (api/dataset (:path x)))))))
       (group-by (fn [x] (if (< 0 (:gap-count x))
                           :partial-days
                           :full-days)))))

(defn report-missing-days
 ""
 [config]
 (let [missing-days (get-missing-days config)
       missing-count (count missing-days)
       formatted-days (->> missing-days
                           (map #(:date %))
                           (map #(str % "\n")))]
   [:div
    [:p "Number of missing days: " missing-count]
    [:pre "Missing days:\n" formatted-days]]))

(defn report-partial-days
  ""
  [partial-days]
  (let [formatted-days (->> partial-days
                            (map #(dissoc % :path))
                            (map #(assoc % :date (str (:date %)))))
        ]
    [:div
     [:p "Number of partial days " (count partial-days)]
     [:pre "Partial days: \n" (with-out-str (pprint/pprint formatted-days))]]))

(defn report-full-days
  ""
  [full-days]
  (let [sorted-days (->> full-days
                         (pmap (fn [x] (assoc x :avg-csi (single-day/get-average-csi (api/dataset (:path x))))))
                         (sort-by :avg-csi)
                         (reverse))
        formatted-days (->> sorted-days
                            (map #(dissoc % :path))
                            (map #(dissoc % :gap-count))
                            (map #(assoc % :date (str (:date %)))))]
    [:pre "Full Days: \n" (with-out-str (pprint/pprint formatted-days))]))

(defn gen-report
  "Generate report for dataset"
  [config]
  (let [grouped-partial-full (group-days-partial-full config)]
    [:div
     [:h1 "Day Sequence Summary"]
     [:table#summarytable.solpred-table
      ]
     [:details
      [:summary "Config"]
      [:section#config.solpred-scrollbox-vertical
       [:pre (with-out-str (pprint/pprint config))]]]
     [:details
      [:summary "Missing Days"]
      [:section#colnames.solpred-scrollbox-vertical
       (report-missing-days config)]]
     [:details
      [:summary "Days with gaps in the data"]
      [:section#gaps.solpred-scrollbox
       (report-partial-days (:partial-days grouped-partial-full))]]
     [:details
      [:summary "Full days sorted by average Clear Sky Index"]
      [:section#csi.solpred-scrollbox-vertical
       (report-full-days (:full-days grouped-partial-full))
       ]]
     [:details
      [:summary "Full days sorted by variablity / intermittency"]
      [:section#intermittent.solpred-scrollbox-vertical
       ]]
     ]))

(comment
  (def config
    {:data-dir "/data/irradiance/BlackMountain_2014-2016_Full"
     :image-dir "/data/skycam-images/Canberra/2015"
     :start-date (LocalDate/of 2015 1 1)
     :end-date (LocalDate/of 2016 1 1)
     :filename-prefix "BlackMountain_"
     :filename-suffix "_cloud_projection.csv.gz"
     :page-title "Day Sequence Report"})

  (time (oz/export! (gen-report config) "/output/day-sequence.html" {:header-extras (oz-util/generate-header-extras config)}))

  (clojure.repl/doc time)
  )

;(gen-report config)
