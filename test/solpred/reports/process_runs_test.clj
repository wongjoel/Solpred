(ns solpred.reports.process-runs-test
  (:require [clojure.test :as test]
            [solpred.reports.process-runs :as process-runs]))


(test/deftest test-name-parse-gen-roundtrip
  (test/testing "Name parse generation roundtrip"
                ;; How do I generate these instead of hardcoding them?
    (test/is (every? boolean (let [dataset-id "BMR" 
                                   model-id "fully-conv"
                                   input-terms 16
                                   input-spacing 60
                                   horizon 420
                                   run-id 0
                                   fold-id 1
                                   gen-name (process-runs/make-run-description-string
                                             {:dataset-id dataset-id :model-id model-id :input-terms input-terms :input-spacing input-spacing :horizon horizon :run-id run-id :fold-id fold-id})
                                   parsed (process-runs/parse-name gen-name)]
                               [(= (parsed :dataset-id) dataset-id)
                                (= (parsed :model-id) model-id)
                                (= (parsed :input-terms) input-terms)
                                (= (parsed :input-spacing) input-spacing)
                                (= (parsed :horizon) horizon)
                                (= (parsed :run-id) run-id)
                                (= (parsed :fold-id) fold-id)])))))



