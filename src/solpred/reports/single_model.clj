(ns solpred.reports.single-model
  (:require
   [clojure.pprint :as pprint]
   [clojure.set :as set]
   [clojure.data.json :as json]
   [clojure.java.io :as io]
   [solpred.reports.metrics :as metrics]
   [solpred.util.oz-util :as oz-util]
   [solpred.util.file :as file]
   [solpred.util.time :as time]
   [clojure.string :as str]
   [tablecloth.api :as api]
   [tech.v3.dataset :as ds]
   [oz.core :as oz])
  (:import
   (java.time LocalDate LocalDateTime Duration)
   (java.time.format DateTimeFormatter)))

(defn report-line-chart-irradiance
  "Spec for a line chart showing irradiance"
  [data]
  [:vega-lite
   {:data {:values
           (into []
                (map (fn [x] {:timestep (str (x "timestep"))
                              :value (x "value")
                              :series (x "series")}))
                data)}
    :title "Timeseries Chart"
    :vconcat [{:width 1000
               :height 400
               :mark {:type "line" :tooltip true}
               :encoding {:x {:field "timestep" :type "quantitative" :scale {:domain {:selection "brush" :encoding "x"}}}
                          :y {:field "value" :type "quantitative" :scale {:domain {:selection "brush" :encoding "y"}} :axis {:title "Power"}}
                          :color {:field "series" :type "nominal"}
                          :opacity {:condition {:selection "series" :value 1}
                                    :value 0.2}}
               :selection {:series {:type "multi" :fields ["series"] :bind "legend"}}}
              {:width 1000
               :height 100
               :mark "line"
               :selection {:brush {:type "interval" :encodings ["x" "y"]}}
               :encoding {:x {:field "timestep" :type "quantitative"}
                          :y {:field "value" :type "quantitative" :axis {:title "Power"}}
                          :color {:field "series" :type "nominal"}}}]}])

(defn export-data-by-day
  "Splits the input dataset into dates and exports a csv per date"
  [data]
  (let [groups (api/group-by data (fn [row] (.toLocalDate (get row "time"))) {:result-type :as-map})]
    (->> groups
         (reduce-kv (fn [m k v] (conj m {:filename (str "/output/site/" k ".csv")
                                         :dataset v}))
                    [])
         (run! (fn [{:keys [filename dataset]:as x}] (ds/write! dataset filename))))))

(defn report-summary-table
  "Generates the report summary table"
  [data]
  [:table#summarytable.solpred-table
   [:tr [:td "Dataset Name:"] [:td [:code (ds/dataset-name data)]]]
   [:tr [:td "Rows:"] [:td [:code (str (api/row-count data))]]]
   [:tr [:td "Columns:"] [:td [:code (str (api/column-count data))]]]])

(defn get-skills-for-table
  ""
  [reference target]
  {:mae-skill (metrics/get-skill (:mae reference) (:mae target))
   :mse-skill (metrics/get-skill (:mse reference) (:mse target))
   :rmse-skill (metrics/get-skill (:rmse reference) (:rmse target))})

#_(defn get-metrics-for-table
  "Gets metrics for `report-metric-table`"
  [{:keys [actual-series model-series persistence-series] :as config} data]
  (let [per (metrics/get-metrics-long-dataset data actual-series persistence-series)
        model (metrics/get-metrics-long-dataset data actual-series model-series)]
    {:persist (merge per (get-skills-for-table per per))
     :model (merge model (get-skills-for-table per model))}))

(defn make-metric-table-row
  "Generates a row for `report-metric-table`"
  [title {:keys [mae mse rmse r2 mae-skill mse-skill rmse-skill] :as metrics}]
  (let [format-string "%.5f"]
    [:tr
     [:td title]
     [:td (format format-string (double mae))]
     [:td (format format-string (double mse))]
     [:td (format format-string (double rmse))]
     [:td (format format-string (double r2))]
     [:td (format format-string (double mae-skill))]
     [:td (format format-string (double mse-skill))]
     [:td (format format-string (double rmse-skill))]]))

#_(defn report-metric-table
  "Generate the report metric table"
  [config data]
  (let [{:keys [persist model] :as metrics} (get-metrics-for-table config data)]
    [:table#summarytable.solpred-table
     [:tr [:td "model"] [:td "MAE"] [:td "MSE"] [:td "RMSE"] [:td "R2"] [:td "Skill vs. Persist MAE"] [:td "Skill vs. Persist MSE"] [:td "Skill vs. Persist RMSE"]]
     (make-metric-table-row "Model" model)
     (make-metric-table-row "Persistence" persist)]
    ))

(defn gen-single-report
  "Generate report for dataset"
  [config title data]
  [:div
   [:h1 title]
   (report-summary-table data)
   [:details
    [:summary "Config"]
    [:section#config.solpred-scrollbox-vertical
     [:pre (with-out-str (pprint/pprint config))]]]
   [:details
    [:summary "Metrics"]
    [:section#metrictable.solpred-table
     #_(report-metric-table config data)]]
   ])

(comment
  (def config {:solpred-scripts "solpredscript.js"
               :solpred-css "solpredstyles.css"
               :page-title "Test Results"
               :data-file "/data/model_out/sunset/blackmountain/1m_7m_15m/1m_7m_15m.csv.gz"
               :actual-series "actual"
               :model-series "pytorch0+15m"
               :persistence-series "persist+15m"
               })

  (def data (api/dataset (:data-file config)))

  (def model_metrics (metrics/get-metrics-long-dataset data "actual" "pytorch0+15m"))
  (def persist_metrics (metrics/get-metrics-long-dataset data "actual" "persist+15m"))

  (report-metric-table config data)

  (oz/export! (gen-single-report config "report" data) "/output/model_report.html")
  (oz/export! (report-line-chart-irradiance (ds/mapseq-reader data)) "/output/nbeats_report.html")

  (clojure.repl/doc oz/export!)
  (clojure.repl/doc ds/mapseq-reader)
  )
