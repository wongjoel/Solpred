(ns solpred.reports.single-day
  (:require [clojure.pprint :as pprint]
            [clojure.set :as set]
            [solpred.util.file :as file]
            [solpred.util.zip :as zip]
            [solpred.util.time :as time]
            [solpred.util.stats :as stats]
            [clojure.string :as str]
            [tablecloth.api :as api]
            [solpred.util.oz-util :as oz-util]
            [tech.v3.dataset :as ds]
            [oz.core :as oz])
  (:import (java.time LocalDate LocalDateTime Duration)
           (java.time.format DateTimeFormatter)))


(defn pair-collection
  #_(pair-collection (range 10))
  "Take a collection and return the pairs of previous and current vals"
  [coll]
  (:pairs
   (reduce (fn [{:keys [prev pairs] :as x} val]
             (if (nil? prev)
               (assoc x :prev val)
               (assoc x :prev val :pairs (conj pairs {:prev prev :curr val}))))
           {:pairs []}
           coll)))

(defn identify-time-issues
  #_(identify-time-issues [(LocalDateTime/of 2015 1 1 1 0 0)
                           (LocalDateTime/of 2015 1 1 1 0 10)
                           (LocalDateTime/of 2015 1 1 1 0 20)
                           (LocalDateTime/of 2015 1 1 1 0 40)
                           (LocalDateTime/of 2015 1 1 1 0 50)])
  "Finds instances where time difference between elements is too large
  Input: A seq of sorted LocalDateTime"
  [times]
  (->> times
       (pair-collection)
       (map (fn [{:keys [prev curr] :as x}] (assoc x :timediff (Math/abs (time/seconds-between prev curr)))))
       (filter (fn [{:keys [timediff] :as x}] (not (<= 9 timediff 11))))))

(defn get-time-issues
  #_(api/dataset "resources/data.csv")
  "Find time issues in a dataset"
  [data]
  (->> (api/select-columns data #{"Timestamp"})
       (ds/value-reader)
       (mapcat identity)
       (map (fn [x] (LocalDateTime/parse x (DateTimeFormatter/ofPattern "yyyy-MM-dd HH:mm:ss"))))
       (identify-time-issues)))


(defn get-key-column-names
  "Return column names that are important (filtering out unimportant column names)"
  [data]
  (->> data
       (api/column-names)
       (filter #(not (re-matches #"advec.*" %)))
       (filter #(not (re-matches #"ws_.*" %)))
       (filter #(not (re-matches #"RingCldFrac_.*" %)))
       (filter #(not (re-matches #".*\+\d+s.*" %)))))

(defn clear-sky-index
  "Calcuate the clear sky index with a limit to stop value from blowing up when clear sky irradiance is small
  Simple definition of clear sky index being actual GHI divided by modelled GHI"
  ([actual modelled]
   (clear-sky-index actual modelled 10))
  ([actual modelled max-val]
   (if (< 0 modelled)
     (min max-val (/ actual modelled))
     1)))

(defn get-average-csi
  [data]
  (->> (api/select-columns data #{"GlobalCMP11Physical" "ClearSkyGHI"})
       (api/drop-missing)
       (ds/mapseq-reader)
       (map (fn [x] (clear-sky-index (x "GlobalCMP11Physical") (x "ClearSkyGHI"))))
       (stats/mean)
       ))

(defn report-data-gaps
  "Format data gaps as hiccup"
  [data]
  (let [time-issues (get-time-issues data)
        issue-count (count time-issues)
        formatted-issues (->> time-issues
                              (map #(assoc % :prev (str (:prev %))))
                              (map #(assoc % :curr (str (:curr %)))))]
    [:div
     [:p (str "Number of pairs with data gaps: " issue-count)]
     [:pre "Pairs with gaps:\n"
      (with-out-str (pprint/pprint formatted-issues))]]))

(defn report-column-names
  [data]
  [:pre (str/join "\n" (get-key-column-names data))])

(defn report-sample-data
  [data]
  (let [start (range 4)
        last (dec (first (api/shape data)))
        end (range (- last 3) last)
        selection (concat start end)]
    [:pre (str (api/select-rows data selection))]))

(defn calculate-filenames
  #_(calculate-filenames config data)
  "Take timestamp column and turn it into expected filenames"
  [{:keys [image-pattern image-extension] :as config} data]
  (let [formatter (DateTimeFormatter/ofPattern image-pattern)]
    (->> (api/select-columns data #{"Timestamp"})
         (ds/value-reader)
         (mapcat identity)
         (map (fn [x] (LocalDateTime/parse x (DateTimeFormatter/ofPattern "yyyy-MM-dd HH:mm:ss"))))
         (map (fn [x] (str (.format x formatter) image-extension)))
         )))

(defn report-mismatched-data
  "Report missing images"
  [{:keys [image-data] :as config} data]
  (if (file/exists? image-data)
    (let [missing-images (zip/get-missing-entries (calculate-filenames config data) image-data)]
      [:div
       [:p "Number of missing images " (count missing-images)]
       [:pre (str/join "\n" (map #(:path %) missing-images))]])
    [:p image-data " not found."]))

(defn report-line-chart-irradiance
  [data]
  [:vega-lite
   {:data {:values
           (->> (api/select-columns data #{"Timestamp" "GlobalCMP11Physical"})
                (api/drop-missing)
                (ds/mapseq-reader)
                (map (fn [x] {:timestamp (str (x "Timestamp"))
                              :ghi (x "GlobalCMP11Physical")})))}
    :title "Irradiance Chart"
    :vconcat [{:width 1000
               :height 400
               :mark {:type "line" :tooltip true}
               :encoding {:x {:field "timestamp" :type "temporal" :scale {:domain {:param "brush" :encoding "x"}}}
                          :y {:field "ghi" :type "quantitative" :scale {:domain {:param "brush" :encoding "y"}} :axis {:title "GHI"}}}}
              {:width 1000
               :height 100
               :mark "line"
               :params [{:name "brush"
                         :select {:type "interval" :encodings ["x" "y"]}}]
               :encoding {:x {:field "timestamp" :type "temporal"}
                          :y {:field "ghi" :type "quantitative" :axis {:title "GHI"}}}}]}])

(defn report-line-chart-csi
  [data]
  [:vega-lite
   {:data {:values
           (->> (api/select-columns data #{"Timestamp" "GlobalCMP11Physical" "ClearSkyGHI"})
                (api/drop-missing)
                (ds/mapseq-reader)
                (map (fn [x] {:timestamp (str (x "Timestamp"))
                              :csi (clear-sky-index (x "GlobalCMP11Physical") (x "ClearSkyGHI"))})))}
    :title "Clear Sky Index Chart"
    :encoding {:x {:field "timestamp" :type "temporal"}
               :y {:field "csi" :type "quantitative" :axis {:title "Clear Sky Index"}}}
    :mark "line"
    :height 400
    :width 1000}])

(defn report-summary-table
  [data]
  (list
   [:tr [:td "Dataset Name:"] [:td [:code (ds/dataset-name data)]]]
   [:tr [:td "Rows:"] [:td [:code (str (api/row-count data))]]]
   [:tr [:td "Columns:"] [:td [:code (str (api/column-count data))]]]))

(defn gen-report
  "Generate report for dataset"
  [config]
  (let [data (api/dataset (:skycam-data config))]
    [:div
     [:h1 "Single Day Summary"]
     [:table#summarytable.solpred-table
      (report-summary-table data)]
     [:details
      [:summary "Config"]
      [:section#config.solpred-scrollbox-vertical
       [:pre (with-out-str (pprint/pprint config))]]]
     [:details
      [:summary "Key Column Names"]
      [:section#colnames.solpred-scrollbox-vertical
       (report-column-names data)]]
     [:details
      [:summary "Sample data"]
      [:section#sample.solpred-scrollbox
       (report-sample-data data)]]
     [:details
      [:summary "Gaps in data"]
      [:section#gaps.solpred-scrollbox-vertical
       (report-data-gaps data)]]
     [:details
      [:summary "Mismatched data"]
      [:section#mismatched.solpred-scrollbox-vertical
       (report-mismatched-data config data)]]
     [:details
      [:summary "Clear Sky Index"]
      [:section#csi.solpred-graph
       [:p "Average CSI = " (get-average-csi data)]
       (report-line-chart-csi data)]]
     [:details
      [:summary "Irradiance Plot"]
      [:section#irradiance.solpred-graph
       (report-line-chart-irradiance data)]]
     ]))

(comment
  (def config {:solpred-scripts "solpredscript.js"
               :solpred-css "solpredstyles.css"
               :page-title "Single Day Report"
               :skycam-data "resources/data/data.csv.gz"
               :image-data "/data/skycam-images/Canberra/2015/2015-01-31.zip"
               :image-extension ".jpg"
               :image-pattern "yyyy-MM-dd/yyyy-MM-dd_HH-mm-ss"})

  (time (oz/export! (gen-report config) "/output/single-day.html" {:header-extras (oz-util/generate-header-extras config)}))

  (def data (api/dataset (:skycam-data config)))

  (def dates ["2015-01-01" "2015-02-02"])

  (def dates
    (->> (file/list-children "/work/blackmountain/bm_2_20s_prefixed")
         (filter file/file?)
         (map file/filename)
         (filter (fn [x] (str/starts-with? x "test")))
         (map (fn [x] (str/replace x "test_" "")))
         (map (fn [x] (str/replace x ".tar" "")))
         ))
  
  (->> dates
       (map (fn [date] (assoc config
                           :skycam-data (str "/data/irradiance/BlackMountain_2014-2016_Full/BlackMountain_" date "_cloud_projection.csv.gz")
                           :image-data (str "/data/skycam-images/Canberra/2015/" date ".zip")
                           :page-title date
                           :date date)))
       (run! (fn [c] (oz/export!
                      (gen-report c)
                      (str "/output/inspect-test/" (:date c) ".html")
                      {:header-extras (oz-util/generate-header-extras c)}))))

  )
