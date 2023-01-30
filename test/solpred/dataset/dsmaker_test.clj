(ns solpred.dataset.dsmaker-test
  (:require [clojure.test :as test]
            [solpred.dataset.dsmaker :as dsmaker]
            [solpred.util.time :as time]))


(defn my-test-fixure [f]
  (println "My setup function here")
  (f)
  (println "My teardown function here")
  (println))

(test/use-fixtures :each my-test-fixure)

(test/deftest test-sample-making
  (test/testing "Sample making"
    (test/is (=
              [{:data [0 1 2 3], :targets [{:horizon 2, :horizon-unit :steps, :value 5} {:horizon 3, :horizon-unit :steps, :value 6}]}
               {:data [1 2 3 4], :targets [{:horizon 2, :horizon-unit :steps, :value 6} {:horizon 3, :horizon-unit :steps, :value 7}]}
               {:data [2 3 4 5], :targets [{:horizon 2, :horizon-unit :steps, :value 7} {:horizon 3, :horizon-unit :steps, :value 8}]}
               {:data [3 4 5 6], :targets [{:horizon 2, :horizon-unit :steps, :value 8} {:horizon 3, :horizon-unit :steps, :value 9}]}]
              (sequence (dsmaker/make-samples {:history-steps 4 :horizons-steps [2 3]}) (range 10))))))

(test/deftest test-gap-detection
  (test/testing "Gap detection"
    (test/is ((dsmaker/has-no-gap? [{:timestamp (time/string->datetime "2021-01-01_12-00-00" "yyyy-MM-dd_HH-mm-ss")}
                                    {:timestamp (time/string->datetime "2021-01-01_12-00-10" "yyyy-MM-dd_HH-mm-ss")}
                                    {:timestamp (time/string->datetime "2021-01-01_12-00-20" "yyyy-MM-dd_HH-mm-ss")}
                                    {:timestamp (time/string->datetime "2021-01-01_12-00-40" "yyyy-MM-dd_HH-mm-ss")}])))))



