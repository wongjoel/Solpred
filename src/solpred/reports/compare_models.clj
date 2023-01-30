(ns solpred.reports.compare-models
  (:require
   [clojure.pprint :as pprint]
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

(defn get-error-metrics
  "Takes a dataset, the target series, the predicted series and returns a map containing the metrics"
  [data, target-col, pred-col]
  (let [wide (api/pivot->wider data "series" "value" {:drop-missing? false})
        actual (into [] (mapcat identity) (ds/value-reader (api/select-columns wide #{target-col})))
        pred (into [] (mapcat identity) (ds/value-reader (api/select-columns wide #{pred-col})))]
    (metrics/get-standard-metrics actual pred)))

(defn report-line-chart-irradiance
  "Spec for a line chart showing irradiance"
  [data]
  [:vega-lite
   {:data {:values
           (into []
                (map (fn [x] {:timestamp (str (x "time"))
                              :value (x "value")
                              :series (x "series")}))
                data)}
    :title "Timeseries Chart"
    :vconcat [{:width 1000
               :height 400
               :mark {:type "line" :tooltip true}
               :encoding {:x {:field "timestamp" :type "temporal" :scale {:domain {:selection "brush" :encoding "x"}}}
                          :y {:field "value" :type "quantitative" :scale {:domain {:selection "brush" :encoding "y"}} :axis {:title "Power"}}
                          :color {:field "series" :type "nominal"}
                          :opacity {:condition {:selection "series" :value 1}
                                    :value 0.2}}
               :selection {:series {:type "multi" :fields ["series"] :bind "legend"}}}
              {:width 1000
               :height 100
               :mark "line"
               :selection {:brush {:type "interval" :encodings ["x" "y"]}}
               :encoding {:x {:field "timestamp" :type "temporal"}
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

(defn get-comparison-metrics-for-table
  "Gets metrics for `report-metric-table`"
  [{:keys [actual-series pytorch-series tensorflow-series persistence-series] :as config} data]
  (let [per (get-error-metrics data actual-series persistence-series)
        py (get-error-metrics data actual-series pytorch-series)
        ten (get-error-metrics data actual-series tensorflow-series)]
    {:persist (merge per (get-skills-for-table per per))
     :pytorch (merge py (get-skills-for-table per py))
     :tensorflow (merge ten (get-skills-for-table per ten))}))

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

(defn report-comparison-metric-table
  "Generated the report metric table"
  [config data]
  (let [{:keys [persist pytorch tensorflow] :as metrics} (get-comparison-metrics-for-table config data)]
    [:table#summarytable.solpred-table
     [:tr [:td "model"] [:td "MAE"] [:td "MSE"] [:td "RMSE"] [:td "R2"] [:td "Skill vs. Persist MAE"] [:td "Skill vs. Persist MSE"] [:td "Skill vs. Persist RMSE"]]
     (make-metric-table-row "Tensorflow" tensorflow)
     (make-metric-table-row "Pytorch" pytorch)
     (make-metric-table-row "Persistence" persist)]
    ))

(defn gen-comparison-report
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
     (report-comparison-metric-table config data)]]
   [:details
    [:summary "Irradiance Plot"]
    [:section#irradiance.solpred-graph
     (report-line-chart-irradiance (ds/mapseq-reader data))]]
   ])

(comment
  (def config {:solpred-scripts "solpredscript.js"
               :solpred-css "solpredstyles.css"
               :page-title "Test Results"
               :pytorch-data "/data/compare/pyt_freq2_v0.csv"
               :tensorflow-data "/data/compare/tensorflow_out_freq_2.csv"
               :actual-series "actual"
               :pytorch-series "pytorch0+15m"
               :tensorflow-series "sunset0+15min"
               :persistence-series "persist+15m"
               })

  (def config {:solpred-scripts "solpredscript.js"
               :solpred-css "solpredstyles.css"
               :page-title "Test Results"
               :data-file "/data/model_out/sunset/blackmountain/1m_15m.csv.gz"
               :actual-series "actual"
               :pytorch-series "pytorch0+15m"
               :tensorflow-series "sunset0+15min"
               :persistence-series "persist+15m"
              })

  (def pytorch-data (api/dataset (:pytorch-data config)))
  (def tensorflow-data (api/dataset (:tensorflow-data config)))
  (def combined-data (let [p-data (-> pytorch-data
                                      (api/map-columns :datetime "time"
                                                       (fn [row]
                                                         (time/string->datetime row "yyyy-MM-dd_HH-mm-ss")))
                                      (api/drop-columns "time")
                                      (api/drop-rows (fn [row] (= "actual" (get row "series"))))
                                      (api/rename-columns {:datetime "time"}))]
                       (api/union tensorflow-data p-data)))

  (report-metric-table config combined-data)
  (keys (api/group-by combined-data (fn [row] (get row "series")) {:result-type :as-map}))

  (get-error-metrics combined-data "actual" "pytorch0+15m")
  (get-error-metrics combined-data "actual" "sunset0+15min")
  (get-error-metrics combined-data "actual" "persist+15m")

  (api/group-by combined-data (fn [row] (.toLocalDate (get row "time"))) {:result-type :as-map})

  (oz/export! (gen-comparison-report config "Test Set" combined-data) "/output/full_report.html")

  (file/make-dir "/output/site")
  (file/make-dir "/output/graphs")
  (export-data-by-day combined-data)

  (->> (file/list "/output/site")
       #_(take 1)
       (map (fn [x] {:name (file/filename x)
                     :dataset (api/dataset (str x))}))
       (map (fn [{:keys [name dataset] :as x}]
              {:name (str "/output/graphs/" (str/replace name #"csv" "html"))
               :report (gen-report config name dataset)}))
       (run! (fn [{:keys [name report] :as x}]
               (oz/export! report name)))
       )

  (let [base-dir "/output/site"
        sunny-days ["2017-03-14" "2017-05-20" "2017-06-04" "2017-07-06" "2017-08-19" "2017-10-07" "2017-11-01" "2017-12-26" "2018-01-20"]
        sunny-data (->> sunny-days
                        (map (fn [x] (api/dataset (file/resolve base-dir (str x ".csv")))))
                        (apply api/union)
                        )]
    (oz/export! (gen-report config "Sunny Days" sunny-data) "/output/sunny_report.html")
    )

  (let [base-dir "/output/site"
        cloudy-days ["2017-03-15" "2017-05-24" "2017-07-05" "2017-09-06" "2017-09-22" "2017-11-04" "2017-12-29" "2018-01-07" "2018-02-01" "2018-02-18" "2018-02-28"]
        cloudy-data (->> cloudy-days
                        (map (fn [x] (api/dataset (file/resolve base-dir (str x ".csv")))))
                        (apply api/union)
                        )]
    (oz/export! (gen-report config "Cloudy Days" cloudy-data) "/output/cloudy_report.html"))

  (oz/start-server!)
  (oz/view! (gen-report config))

  (clojure.repl/doc oz/export!)
  (clojure.repl/doc ds/mapseq-reader)
  )
