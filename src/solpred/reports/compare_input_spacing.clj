(ns solpred.reports.compare-input-spacing
  "Script to make report comparing the output for SUNSET over different input configurations"
  (:require
   [clojure.pprint :as pprint]
   [solpred.util.oz-util :as oz-util]
   [solpred.util.file :as file]
   [clojure.edn :as edn]
   [tablecloth.api :as api]
   [tech.v3.dataset :as ds]
   [oz.core :as oz]))

(defn run->metric-table
  #_(run->metric-table {:sunset-metrics {:mae 1 :mse 2 :rmse 3 :r2 4}
                        :persist-metrics {:mae 5 :mse 6 :rmse 7 :r2 8}
                        :sunset-skills {:mae-skill 10 :mse-skill 11 :rmse-skill 12}})
  "Generate the metric table for a single run"
  [{:keys [sunset-metrics persist-metrics]}]
  (let [row-vector [:mae :mse :rmse :r2]
        format-string "%.4f"]
    (oz-util/map->table
     {:header ["Model" "MAE" "MSE" "RMSE" "r2"]
      :rows [(into ["Sunset"] (oz-util/map->formatted-row row-vector format-string sunset-metrics))
             (into ["Persist"] (oz-util/map->formatted-row row-vector format-string persist-metrics))]
      :css-id ""
      :css-class "solpred-table"})))

(defn run->skill-table
  #_(run->skill-table {:sunset-metrics {:mae 1 :mse 2 :rmse 3 :r2 4}
                        :persist-metrics {:mae 5 :mse 6 :rmse 7 :r2 8}
                        :sunset-skills {:mae-skill 10 :mse-skill 11 :rmse-skill 12}})
  "Generate the skill table for a single run"
  [{:keys [sunset-skills]}]
  (let [row-vector [:mae-skill :mse-skill :rmse-skill]
        format-string "%.4f"]
    (oz-util/map->table
     {:header ["Model" "MAE" "MSE" "RMSE"]
      :rows [(into ["Sunset Skill"] (oz-util/map->formatted-row row-vector format-string sunset-skills))]
      :css-id ""
      :css-class "solpred-table"})))

(defn run->section
  #_(run->section {:input-terms 2
                   :input-spacing 20
                   :sunset-metrics {:mae 1 :mse 2 :rmse 3 :r2 4}
                   :persist-metrics {:mae 5 :mse 6 :rmse 7 :r2 8}
                   :sunset-skills {:mae-skill 10 :mse-skill 11 :rmse-skill 12}})
  "Convert a run into a section"
  [{:keys [input-terms input-spacing] :as run}]
  [:details
   [:summary (str input-terms "x" input-spacing " seconds")]
   [:section#metrictable.solpred-table
    (run->metric-table run)
    (run->skill-table run)]]
  )

(defn gen-report
  #_(gen-report config runs)
  "Generate report for dataset"
  [config runs]
  [:div
   [:h1 (:title config)]
   [:details
    [:summary "Config"]
    [:section#config.solpred-scrollbox-vertical
     [:pre (with-out-str (pprint/pprint config))]]]
   (into [:section] (map run->section) runs)
   ])

(defn get-category-dates
  #_(get-category-dates "Intermittent" {})
  "Get the dates for a category. Returns a set."
  ([category]
   (get-category-dates category {}))
  ([category
    {:keys [mapping-file category-col date-col]
     :or {mapping-file "resources/datasets/blackmountain-test.csv"
          category-col "Category"
          date-col "Date"}}]
   (->> (api/dataset mapping-file)
        (ds/mapseq-reader)
        (filter (fn [x] (= category (get x category-col))))
        (map (fn [x] (get x date-col)))
        set)))

(defn chart-terms-vs-skill
  #_(chart-terms-vs-skill
     [{:input-spacing 1 :input-terms 2 :model-name "sunset" :sunset-skills {:rmse-skill 3 :mae-skill 4}}
      {:input-spacing 5 :input-terms 6 :model-name "sunset" :sunset-skills {:rmse-skill 7 :mae-skill 8}}
      {:input-spacing 9 :input-terms 10 :model-name "fully-conv" :fully-conv-skills {:rmse-skill 11 :mae-skill 12}}])
  "Make chart for input terms vs skill"
  [runs]
  (oz-util/point-line-chart
   {:config {:title "Skill vs Number of Input Terms"
             :x-title "Input Terms"
             :x-field "input-terms"
             :y-title "RMSE Skill"
             :y-field "rmse-skill"}
    :values (map (fn [x] {:input-terms (:input-terms x)
                          :series (str "mdl=" (:model-name x) ", in-spacing=" (:input-spacing x))
                          :rmse-skill (get-in x [(keyword (str (:model-name x) "-skills")) :rmse-skill])}) runs)}))

(defn chart-spacing-vs-skill
  #_(chart-spacing-vs-skill
     [{:input-spacing 1 :input-terms 2 :model-name "sunset" :sunset-skills {:rmse-skill 3 :mae-skill 4}}
      {:input-spacing 5 :input-terms 6 :model-name "sunset" :sunset-skills {:rmse-skill 7 :mae-skill 8}}
      {:input-spacing 9 :input-terms 10 :model-name "fully-conv" :fully-conv-skills {:rmse-skill 11 :mae-skill 12}}])
  "Make chart for input spacing vs skill"
  [runs]
  (oz-util/point-line-chart
   {:config {:title "Skill vs Spacing of Input Terms"
             :x-title "Input Spacing (seconds)"
             :x-field "input-spacing"
             :y-title "RMSE Skill"
             :y-field "rmse-skill"}
    :values (map (fn [x] {:input-spacing (:input-spacing x)
                          :series (str "mdl=" (:model-name x) ", in-terms=" (:input-terms x))
                          :rmse-skill (get-in x [(keyword (str (:model-name x) "-skills")) :rmse-skill])}) runs)}))

(defn read-run-metrics
  #_(read-run-metrics
     {:input-dir "/work/processed-runs"
      :category "Intermittent"})
  "Read run metrics from file"
  [{:keys [input-dir category]}]
  (->> (file/list-children (file/resolve-path [input-dir category "edn"]))
       (map str)
       (map (fn [child] (edn/read-string (slurp child))))))

(defn charts-model-category-skills
  #_(charts-model-category-skills
     {:input-dir "/work/processed-runs"
      :category "All"
      :model-name "sunset"})
  "Export graphs for a single model's skill, grouped by input terms and input spacing"
  [{:keys [category model-name] :as config}]
  (let [chart-runs (->> (read-run-metrics config)
                        (filter (fn [run] (= model-name (:model-name run))))
                        (map (fn [run] (assoc run :skills ((keyword (str model-name "-skills")) run)))))]
    (->> [{:hiccup (chart-terms-vs-skill chart-runs)
           :filename (str "input_terms_skill_" model-name "_"  category ".html")}
          {:hiccup (chart-spacing-vs-skill chart-runs)
           :filename  (str "input_spacing_skill_" model-name "_"  category ".html")}]
         (map (fn [chart-spec] (assoc chart-spec
                                      :model-name model-name
                                      :category category))))))

(defn charts-model-skills
  #_(charts-model-skills
     {:input-dir "/work/processed-runs"
      :categories ["All" "Intermittent" "Sunny" "Overcast" "Mixed"]
      :model-name "sunset"})
  [{:keys [categories] :as config}]
  (mapcat (fn [category] (charts-model-category-skills (assoc config :category category))) categories))

(defn charts-skills
  #_(charts-skills
     {:input-dir "/work/processed-runs"
      :categories ["All" "Intermittent" "Sunny" "Overcast" "Mixed"]
      :models ["sunset" "fully-conv"]})
  [{:keys [models] :as config}]
  (mapcat (fn [model] (charts-model-skills (assoc config :model-name model))) models))

(defn export-skills!
  #_(export-skills!
     {:input-dir "/work/processed-runs"
      :categories ["All" "Intermittent" "Sunny" "Overcast" "Mixed"]
      :models ["sunset" "fully-conv"]
      :output-dir "/output"})
  [{:keys [output-dir] :as config}]
  (->> (charts-skills config)
       (map (fn [{:keys [model-name category] :as chart-spec}]
              (assoc chart-spec
                     :output-dir (file/resolve-path [output-dir model-name category]))))
       (run! (fn [{:keys [hiccup output-dir filename]}]
               (file/make-dirs output-dir)
               (oz-util/export! hiccup (file/resolve-path [output-dir filename]) {})))))

(defn compare-category-metrics!
  #_(compare-category-metrics!
     {:input-dir "/work/processed-runs"
      :category "All"
      :output-dir "/output/compare"})
  "Export graphs for a single model's skill, grouped by input terms and input spacing"
  [{:keys [output-dir category model-name] :as config}]
  (let [chart-runs (->> (read-run-metrics config)
                        (map (fn [run] (assoc run :skills ((keyword (str model-name "-skills")) run)))))]
    (file/make-dirs output-dir)
    (oz-util/export! (chart-terms-vs-skill chart-runs)
                     (file/resolve-path [output-dir (str "input_terms_skill_compare_"  category ".html")])
                     {})
    (oz-util/export! (chart-spacing-vs-skill chart-runs)
                     (file/resolve-path [output-dir (str "input_spacing_skill_compare_"  category ".html")])
                     {})))

(defn compare-metrics!
  #_(compare-metrics!
     {:input-dir "/work/processed-runs"
      :categories ["All" "Intermittent" "Sunny" "Overcast" "Mixed"]
      :output-dir "/output/compare"})
  [{:keys [categories] :as config}]
  (run! (fn [category] (compare-category-metrics! (assoc config :category category))) categories))


(defn combine-model-metrics
  "Combine metrics for the same input spacing and terms, ensuring we don't clobber the persistence metrics"
  #_(combine-model-metrics (read-run-metrics {:input-dir "/work/processed-runs" :category "All"}))
  [metrics]
  (->> metrics
       (map (fn [x] (case (:model-name x)
                      "sunset" (assoc x :persist-metrics-sunset (:persist-metrics x))
                      "sunset-im" (assoc x :persist-metrics-sunset-im (:persist-metrics x))
                      "sunset-ar" (assoc x :persist-metrics-sunset-ar (:persist-metrics x))
                      "leaky-sunset" (assoc x :persist-metrics-leaky-sunset (:persist-metrics x))
                      "fully-conv" (assoc x :persist-metrics-fully-conv (:persist-metrics x))
                      "fully-conv-ar" (assoc x :persist-metrics-fully-conv-ar (:persist-metrics x))
                      "fully-conv-im" (assoc x :persist-metrics-fully-conv-im (:persist-metrics x))
                      "vis-transformer" (assoc x :persist-metrics-vis-transformer (:persist-metrics x))
                      (throw (java.lang.IllegalArgumentException. (str "unhandled model " x))))))
       (group-by :input-terms)
       (mapcat (fn [x] (->> (group-by :input-spacing (val x))
                            (map (fn [x] (reduce (fn [result val] (merge result val)) (val x)))))))))

(defn make-skill-table-for-metrics
  [input-dir]
  (let [metrics (read-run-metrics {:input-dir input-dir :category "All"})
        table-rows (->> (combine-model-metrics metrics)
                       (sort-by (juxt :input-terms :input-spacing))
                       (map (fn [{:keys [input-terms input-spacing category
                                         sunset-skills fully-conv-skills leaky-sunset-skills]}]
                              [input-terms input-spacing category
                               (:rmse-skill sunset-skills) (:rmse-skill fully-conv-skills) (:rmse-skill leaky-sunset-skills)])))]
    (oz-util/map->table
     {:header ["input-terms" "input-spacing" "category" "sunset-skill" "fully-conv_skill" "leaky-sunset-skills"]
      :rows table-rows
      :css-id ""
      :css-class ""})))

(defn export-skill-table!
  #_(export-skill-table! "/work/processed-runs/blackmountain" "/output/blackmountain")
  #_(export-skill-table! "/work/processed-runs/sunset" "/output/sunset")
  [input-dir output-dir]
  (oz-util/export! (make-skill-table-for-metrics input-dir) (file/resolve-path [output-dir "skill-table.html"]) {}))

(defn make-rmse-table-for-metrics
  [input-dir]
  (let [metrics (read-run-metrics {:input-dir input-dir :category "All"})
        table-rows (->> (combine-model-metrics metrics)
                       (sort-by (juxt :input-terms :input-spacing))
                       (map (fn [{:keys [input-terms input-spacing category
                                         sunset-metrics fully-conv-metrics leaky-sunset-metrics
                                         persist-metrics-sunset persist-metrics-fully-conv persist-metrics-leaky-sunset]}]
                              [input-terms input-spacing category
                               (:rmse sunset-metrics) (:rmse fully-conv-metrics) (:rmse leaky-sunset-metrics)
                               (:rmse persist-metrics-sunset) (:rmse persist-metrics-fully-conv) (:rmse persist-metrics-leaky-sunset)])))]
    (oz-util/map->table
     {:header ["input-terms" "input-spacing" "category" "sunset rmse" "fully-conv rmse" "leaky sunset rmse" "persist sunset" "persist fully-conv" "persist leaky sunset"]
      :rows table-rows
      :css-id ""
      :css-class ""})))

(defn export-rmse-table!
  #_(export-rmse-table! "/work/processed-runs/blackmountain" "/output/blackmountain")
  #_(export-rmse-table! "/work/processed-runs/sunset" "/output/sunset")
  [input-dir output-dir]
  (oz-util/export! (make-rmse-table-for-metrics input-dir) (file/resolve-path [output-dir "rmse-table.html"]) {}))

(defn main
  ""
  []
  (export-skills!
     {:input-dir "/work/processed-runs/blackmountain"
      :categories ["All" "Intermittent" "Sunny" "Overcast" "Mixed"]
      :models ["sunset" "sunset-ar" "sunset-im" "fully-conv" "fully-conv-ar" "fully-conv-im"]
      :output-dir "/output/blackmountain"})
  (compare-metrics!
     {:input-dir "/work/processed-runs/blackmountain"
      :categories ["All" "Intermittent" "Sunny" "Overcast" "Mixed"]
      :output-dir "/output/blackmountain/compare"})
  (export-rmse-table! "/work/processed-runs/blackmountain" "/output/blackmountain")
  (export-skill-table! "/work/processed-runs/blackmountain" "/output/blackmountain")

  (export-skills!
     {:input-dir "/work/processed-runs/blackmountain-intermittent"
      :categories ["All"]
      :models ["sunset" "fully-conv"]
      :output-dir "/output/blackmountain-intermittent"})
  (compare-metrics!
     {:input-dir "/work/processed-runs/blackmountain-intermittent"
      :categories ["All"]
      :output-dir "/output/blackmountain-intermittent/compare"})
  (export-rmse-table! "/work/processed-runs/blackmountain-intermittent" "/output/blackmountain-intermittent")
  (export-skill-table! "/work/processed-runs/blackmountain-intermittent" "/output/blackmountain-intermittent")
  
  (export-skills!
     {:input-dir "/work/processed-runs/sunset"
      :categories ["All"]
      :models ["sunset" "fully-conv"]
      :output-dir "/output/sunset"})
  (compare-metrics!
     {:input-dir "/work/processed-runs/sunset"
      :categories ["All"]
      :output-dir "/output/sunset/compare"})
  (export-rmse-table! "/work/processed-runs/sunset" "/output/sunset")
  (export-skill-table! "/work/processed-runs/sunset" "/output/sunset")
 )

(comment
  (def config {})

  (main)

  (def metrics (read-run-metrics {:input-dir "/work/processed-runs" :category "All"}))
  (def table-data (->> (combine-model-metrics metrics)
                       (sort-by (juxt :input-terms :input-spacing))
                       (map (fn [{:keys [input-terms input-spacing category sunset-skills fully-conv-skills]}]
                              [input-terms input-spacing category (:rmse-skill sunset-skills) (:rmse-skill fully-conv-skills)]))))
  
  )
