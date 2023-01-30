(ns solpred.core
  (:require
   [solpred.dataset.webdsmaker :as webdsmaker]
   [solpred.launcher.launcher :as launcher]
   [solpred.util.cleaner :as cleaner]
   [solpred.dataset.verifier :as verifier]
   [solpred.dataset.dsmaker :as dsmaker]
   [cli-matic.core :as cli]
   [clojure.tools.logging :as log]
   [clojure.edn :as edn]
   [clojure.java.io :as io])
  (:gen-class))

(defn load-edn
  "Load edn from an io/reader source (filename or io/resource)."
  [source]
  (try
    (with-open [r (io/reader source)]
      (edn/read (java.io.PushbackReader. r)))
    (catch java.io.IOException e
      (log/error (str "Couldn't open " source ": " (.getMessage e)))
      (log/debug (str "Tried to open " (.toAbsolutePath (.toPath (io/as-file source)))))
      (throw e))
    (catch RuntimeException e
      (log/error (str "Error parsing edn file " source ": " (.getMessage e)))
      (throw e))))

(defn dsmaker-operation
  #_(dsmaker-operation {:config-path "configs/bm_16_60s_420s-64px.edn"})
  "Runs the dsmaker operation"
  [{:keys [config-path] :as args}]
  (log/info (str "Launching with args " args))
  (log/info (str "config-path " config-path))
  (let [config (load-edn config-path)]
    (println config)
    (dsmaker/main (:dsmaker config))))

(defn dataset-operation
  #_(dataset-operation {:config-path "configs/bm_16_60s_420s-64px.edn"})
  "Runs the dataset operation"
  [{:keys [config-path] :as args}]
  (log/info (str "Launching with args " args))
  (log/info (str "config-path " config-path))
  (let [config (load-edn config-path)]
    (println config)
    (webdsmaker/main config)
    ))

(defn launch-operation
  "Run a model on the given dataset"
  [args]
  (log/info (str "Launching with args " args))
  (launcher/main args))

(defn clean-operation
  "Clean up filesystem"
  [args]
  (log/info (str "Launching with args " args))
  (cleaner/main args)
  )

(defn verify-operation
  "verify filesystem"
  [args]
  (log/info (str "Launching with args " args))
  (verifier/main args)
  )

(def CONFIGURATION
  {:command "solpred"
   :description "manages solpred experiments"
   :version "2021.07.19"
   :subcommands [{:command "dsmaker"
                  :runs dsmaker-operation
                  :opts [{:option "config-path" :as "The path to the config file" :type :string}]
                  }
                 {:command "dataset"
                  :runs dataset-operation
                  :opts [{:option "config-path" :as "The path to the config file" :type :string}]
                  }
                 {:command "launch"
                  :runs launch-operation
                  :opts [{:option "dataset" :as "The dataset to run" :type :string}
                         {:option "dry-run" :as "Don't actually launch anything" :type :with-flag :default false}
                         {:option "model-name" :as "Model display name" :type :string}
                         {:option "script-name" :as "Name of the script to call" :type :string}
                         {:option "run-dir" :as "the subdir to place runs" :type :string}
                         {:option "launch-dir" :as "the subdir to place launchers" :type :string}
                         {:option "engine" :as "the engine to use" :type :string :default "slurm"}
                         {:option "folds" :as "number of folds to run" :type :int :default 3}
                         {:option "horizon-list" :as "horizon seconds without the s" :type :edn :default :present}
                         {:option "term-list" :as "input-term list" :type :edn :default :present}
                         {:option "spacing-list" :as "Spacing list" :type :edn :default :present}
                         {:option "img-width" :as "image width" :type :int :default 64}
                         {:option "crop-size" :as "crop size" :type :string :default "full"}
                         {:option "lr-list" :as "Learning rate list" :type :edn :default :present}
                         {:option "num-workers" :as "Number of workers" :type :int :default 5}
                         ]
                  }
                 {:command "clean"
                  :runs clean-operation
                  :opts [{:option "dry-run" :as "Don't actually delete anything" :type :with-flag :default false}
                         {:option "skip-files" :as "Don't delete files" :type :with-flag :default false}
                         {:option "skip-dirs" :as "Don't delete dirs" :type :with-flag :default false}
                         {:option "base-dir" :as "the dir to clean" :type :string}
                         {:option "threshold-mb" :as "the minimum file size" :type :float}
                         ]
                  }
                 {:command "verify"
                  :runs verify-operation
                  :opts [{:option "dataset" :as "The dataset to check" :type :string}]
                  }
                 ]})

(defn -main
  "CLI Entry point"
  [& args]
  #_(println args)
  (cli/run-cmd args CONFIGURATION))
