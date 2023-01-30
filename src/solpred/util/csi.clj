(ns solpred.util.csi
  "Clear sky index calculations"
  (:require [tech.v3.dataset :as ds]
            [tablecloth.api :as api]
            [solpred.util.stats :as stats]))

(defn clear-sky-index
  #_(clear-sky-index 50 100)
  "Calcuate the clear sky index with a limit to stop value from blowing up when clear sky irradiance is small
  Simple definition of clear sky index being actual GHI divided by modelled GHI"
  ([actual modelled]
   (clear-sky-index actual modelled 10))
  ([actual modelled max-val]
   (if (< 0 modelled)
     (min max-val (/ actual modelled))
     1)))

(defn get-average-csi
  #_(get-average-csi (ds/->dataset [{"GlobalCMP11Physical" 50 "ClearSkyGHI" 100}
                                    {"GlobalCMP11Physical" 51 "ClearSkyGHI" 101}
                                    {"GlobalCMP11Physical" 52 "ClearSkyGHI" 102}
                                    {"GlobalCMP11Physical" "" "ClearSkyGHI" 102}
                                    {"GlobalCMP11Physical" 50 "ClearSkyGHI" ""}
                                    {"GlobalCMP11Physical" 53 "ClearSkyGHI" 103}]))
  "Calculate the average CSI for a dataset"
  ([data]
   (get-average-csi data #{"GlobalCMP11Physical" "ClearSkyGHI"}))
  ([data columns]
   (->> (api/select-columns data columns)
        (ds/mapseq-reader)
        (filter (fn [x] (every? number? (map (fn [y] (x y)) columns))))
        (map (fn [x] (clear-sky-index (x "GlobalCMP11Physical") (x "ClearSkyGHI"))))
        (stats/mean)
        )))
