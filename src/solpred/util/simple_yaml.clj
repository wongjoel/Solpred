(ns solpred.util.simple-yaml
  "Parses the yaml files used to store hparams in runs"
  (:require
   [clojure.string :as str]
   [solpred.util.runtime-check :as runtime-check])
  )

(defn parse-file
  #_(parse-file "/results/solpred/blackmountain-round/auto-process/fully-conv/run_01/lr_3.0E-6/fold0/16x60s_420s/lightning_logs/version_1430166/hparams.yaml")
  "Parses the hparams.yaml file into a map"
  [path]
  (let [line (map str/trim (str/split-lines (slurp path)))]
    (runtime-check/throw-on-false (= (first line) "args: !!python/object:argparse.Namespace") (str "Unexpected first line: " (first line)))
    (->> (drop 1 line)
         (map #(str/split % #": " 2))
         (map (fn [split]
                (runtime-check/throw-on-false (= (count split) 2) (str "bad-split " split))
                split))
         (reduce (fn [acc [k v]]
                   (assoc acc
                          (keyword k)
                          (cond
                            (= v "null") nil
                            (= v "false") false
                            (= v "true") true
                            (parse-long v) (parse-long v)
                            (parse-double v) (parse-double v)
                            :else v))
                   ) {})))
  )


