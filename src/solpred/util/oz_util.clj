(ns solpred.util.oz-util
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [oz.core :as oz]))

(defn generate-header-extras
  #_(generate-header-extras {:page-title "ab"})
  "Generate the things we want to add to the <head> tag"
  [{:keys [css page-title]
    :or {css "reports/solpredstyles.css"
         page-title "my-title"}}]
  [[:style (slurp (io/resource css))]
   [:script {:type "text/javascript"} (str "document.title = \"" page-title "\";")]])

(defn temp-fix-vega-lite-version
  [path]
  (println "temp fix running")
  (spit
   path
   (str/replace
    (slurp path)
    #"https://cdn.jsdelivr.net/npm/vega-lite@4.17.0"
    "https://cdn.jsdelivr.net/npm/vega-lite@5"))
  )

(defn export!
  #_(export! hiccup "/test.html" {})
  "Wraps `oz/export!` with some niceties"
  [hiccup filepath extras]
  (oz/export! hiccup filepath {:header-extras (generate-header-extras extras)})
  #_(temp-fix-vega-lite-version filepath))

(defn map->table
  #_(map->table {:header ["a" "b" "c"]
                 :rows [["1" "2" "3"]
                        ["4" "5" "6"]]
                 :css-id "my-id"
                 :css-class "my-class"})
  "Make into a Hiccup-compatible table by adding in the HTML tags"
  [{:keys [header rows css-id css-class]}]
  [:table {:class css-class :id css-id}
   [:thhead
    (into [:tr] (map (fn [data] [:th data])) header)]
   (into [:tbody] (map (fn [row]
                         (into [:tr] (map (fn [data] [:td data])) row))) rows)])

(defn map->row
  #_(map->row [:a :b :c :d] {:a 1 :b 2 :c 3 :d 4})
  "Pull values out of map in the order of the keys passed in"
  [selectors data]
  (into [] (map (fn [x] (get data x))) selectors))

(defn map->formatted-row
  #_(map->formatted-row [:a :b :c :d] "%.4f" {:a 1 :b 2 :c 3 :d 4})
  "Pull values out of map in the order of the keys passed in"
  [selectors format-string data]
  (into [] (map (fn [x] (format format-string (double x)))) (map->row selectors data)))

(defn line-chart
  "Spec for a line chart"
  #_(line-chart {:config {:title "my-title"
                          :x-title "x-title"
                          :x-field "x-series"
                          :x-type "temporal"
                          :y-title "y-title"
                          :y-field "y-series"}
                 :values [{:x-series 1 :y-series 2 :series "a"}
                          {:x-series 2 :y-series 4 :series "a"}
                          {:x-series 3 :y-series 3 :series "a"}
                          {:x-series 1 :y-series 7 :series "b"}
                          {:x-series 2 :y-series 6 :series "b"}
                          {:x-series 3 :y-series 5 :series "b"}]})
  [{:keys [config values]}]
  (let [config (merge
                {:title "default-title"
                 :width 1200
                 :height 500
                 :mark {:type "line" :tooltip true}}
                config)]
    [:vega-lite
     {:$schema "https://vega.github.io/schema/vega-lite/v5.json"
      :data {:values values}
      :title (:title config)
      :width (:width config)
      :height (:height config)
      :mark (:mark config)
      :params [{:name "series_select"
                :select {:type "point" :fields ["series"]}
                :bind "legend"}]
      :encoding {:x {:field (:x-field config)
                     :type "quantitative"
                     :axis {:title (:x-title config)}}
                 :y {:field (:y-field config)
                     :type "quantitative"
                     :axis {:title (:y-title config)}}
                 :color {:field "series" :type "nominal"}
                 :opacity {:condition {:param "series_select" :value 1}
                           :value 0.2}}}]))


(defn point-line-chart
  "Spec for a line chart with points marked"
  [{:keys [config values]}]
  (let [new-config (merge
                    {:mark {:type "line" :point true :tooltip true}}
                    config)]
    (line-chart {:config new-config :values values})))

(defn scatter-chart
  "Spec for a scatter chart"
  [{:keys [config values]}]
  (let [new-config (merge
                    {:mark {:type "point" :tooltip true}}
                    config)]
    (line-chart {:config new-config :values values})))

(defn line-chart-with-zoom
  "Spec for a line chart"
  #_(line-chart {:config {:title "my-title"
                          :x-title "x-title"
                          :x-field "x-series"
                          :y-title "y-title"
                          :y-field "y-series"}
                 :values [{:x-series 1 :y-series 2 :series "a"}
                          {:x-series 2 :y-series 4 :series "a"}
                          {:x-series 3 :y-series 3 :series "a"}
                          {:x-series 1 :y-series 7 :series "b"}
                          {:x-series 2 :y-series 6 :series "b"}
                          {:x-series 3 :y-series 5 :series "b"}]})
  [{:keys [config values]}]
  (let [config (merge
                {:title "default-title"
                 :width 1200
                 :height 500
                 :sub-width 2000
                 :sub-height 100
                 :mark {:type "line" :tooltip true}}
                config)]
    [:vega-lite
     {:$schema "https://vega.github.io/schema/vega-lite/v5.json"
      :data {:values values}
      :title (:title config)
      :vconcat [{:width (:width config)
                 :height (:height config)
                 :mark (:mark config)
                 :encoding {:x {:field (:x-field config)
                                :type "quantitative"
                                :axis {:title (:x-title config)}
                                :scale {:domain {:param "brush" :encoding "x"}}}
                            :y {:field (:y-field config)
                                :type "quantitative"
                                :axis {:title (:y-title config)}
                                :scale {:domain {:param "brush" :encoding "y"}}}
                            :color {:field "series" :type "nominal"}}}
                {:width (:sub-width config)
                 :height (:sub-height config)
                 :mark (:mark config)
                 :params [{:name "brush"
                           :select {:type "interval" :encodings ["x" "y"]}}]
                 :encoding {:x {:field (:x-field config)
                                :type "quantitative"
                                :axis {:title (:x-title config)}}
                            :y {:field (:y-field config)
                                :type "quantitative"
                                :axis {:title (:y-title config)}}
                            :color {:field "series" :type "nominal"}}}]}]))

(defn box-plot
  "Spec for a box plot"
  #_(box-plot {:config {:title "my-title"
                          :x-title "x-title"
                          :y-title "y-title"
                          :y-field "y-series"}
                 :values [{:y-series 2 :series "a"}
                          {:y-series 4 :series "a"}
                          {:y-series 3 :series "a"}
                          {:y-series 7 :series "b"}
                          {:y-series 6 :series "b"}
                          {:y-series 5 :series "b"}]})
  [{:keys [config values]}]
  (let [config (merge
                {:title "default-title"
                 :width 1200
                 :height 500
                 :mark {:type "boxplot" :extent "min-max"}}
                config)]
    [:vega-lite
     {:$schema "https://vega.github.io/schema/vega-lite/v5.json"
      :data {:values values}
      :title (:title config)
      :width (:width config)
      :height (:height config)
      :mark (:mark config)
      :encoding {:x {:field "series"
                     :type "nominal"
                     :axis {:title (:x-title config)}}
                 :y {:field (:y-field config)
                     :type "quantitative"
                     :axis {:title (:y-title config)}
                     :scale {:zero false}}
                 :color {:field "series" :type "nominal"}}}]))

(comment
  (let [input-spec
        {:config {:title "my-title"
                  :x-title "x-title"
                  :x-field "x-series"
                  :y-title "y-title"
                  :y-field "y-series"}
         :values [{:x-series 1 :y-series 2 :series "a"}
                  {:x-series 2 :y-series 4 :series "a"}
                  {:x-series 3 :y-series 3 :series "a"}
                  {:x-series 1 :y-series 7 :series "b"}
                  {:x-series 2 :y-series 6 :series "b"}
                  {:x-series 3 :y-series 5 :series "b"}]}]

    (export! (line-chart input-spec)
             "/output/chart_utils_line.html" {})

    (export! (scatter-chart input-spec)
             "/output/chart_utils_scatter.html" {}))

  (let [input-spec
        {:config {:title "my-title"
                          :x-title "x-title"
                          :y-title "y-title"
                          :y-field "y-series"}
                 :values [{:y-series 2 :series "a"}
                          {:y-series 4 :series "a"}
                          {:y-series 3 :series "a"}
                          {:y-series 7 :series "b"}
                          {:y-series 6 :series "b"}
                          {:y-series 5 :series "b"}]}]

    (export! (box-plot input-spec)
             "/output/chart_utils_box.html" {}))
  )
