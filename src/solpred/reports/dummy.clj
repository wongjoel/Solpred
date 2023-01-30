(ns solpred.reports.dummy
  "Self contained examples of report generation"
  (:require [solpred.util.file :as file]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [oz.core :as oz]
            [solpred.util.oz-util :as oz-util])
  (:import (java.time LocalDate LocalDateTime Duration)))

(defn play-data "" [& names]
  (for [n names
        i (range 20)]
    {:time i :item n :quantity (+ (Math/pow (* i (count n)) 0.8) (rand-int (count n)))}))

(defn line-plot "" []
  [:vega-lite
   {:$schema "https://vega.github.io/schema/vega-lite/v5.json"
    :data {:values (play-data "monkey" "slipper" "broom")}
    :title "my chart"
    :encoding {:x {:field "time" :type "quantitative"}
               :y {:field "quantity" :type "quantitative"}
               :color {:field "item" :type "nominal"}}
    :mark "line"}])

(defn line-plot-with-zoom "" []
  [:vega-lite
   {:$schema "https://vega.github.io/schema/vega-lite/v5.json"
    :data {:values (play-data "monkey" "slipper" "broom")}
    :title "Sample Chart"
    :vconcat [{:width 1000
               :height 400
               :mark {:type "line" :tooltip true}
               :encoding {:x {:field "time" :type "quantitative" :scale {:domain {:param "brush" :encoding "x"}}}
                          :y {:field "quantity" :type "quantitative" :scale {:domain {:param "brush" :encoding "y"}} :axis {:title "Quantity"}}
                          :color {:field "item" :type "nominal"}}}
              {:width 1000
               :height 100
               :mark "line"
               :params [{:name "brush"
                         :select {:type "interval" :encodings ["x" "y"]}}]
               :encoding {:x {:field "time" :type "quantitative"}
                          :y {:field "quantity" :type "quantitative" :axis {:title "Quantity"}}
                          :color {:field "item" :type "nominal"}}}]}])
(defn generate-table "" []
  [:table {:class "my-class" :id "my-id"}
    [:thead [:tr [:th "a"] [:th "b"] [:th "c"]]]
    [:tbody [:tr [:td "1"] [:td "2"] [:td "3"]]]
    [:tbody [:tr [:td "4"] [:td "5"] [:td "6"]]]])

(defn generate-hiccup "" [x]
  [:div
   [:p#abc "Hello there World! 2"]
   [:p "abc"]
   [:p "123"]
   [:p "ddde"]
   (generate-table)
   (line-plot)
   (line-plot-with-zoom)]
  )

(comment
  (def config {})

  (oz/export! (generate-hiccup config) "/output/dummy.html" {:header-extras (oz-util/generate-header-extras {:page-title "abc"})})
  )
