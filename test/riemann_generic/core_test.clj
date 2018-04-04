(ns riemann-generic.core-test
  (:require [clojure.test :refer :all]
            [riemann.time.controlled :refer :all]
            [riemann.time :refer :all]
            [riemann.test :refer [run-stream run-stream-intervals test-stream
                                  with-test-stream test-stream-intervals]]
            [riemann-generic.core :refer :all]))

(use-fixtures :once control-time!)
(use-fixtures :each reset-time!)

(deftest threshold-test
  (testing "with warning key"
    (test-stream (threshold {:warning 30 :critical 70})
      [{:metric 29}
       {:metric 30}
       {:metric 40}
       {:metric 70}
       {:metric 90}]
      [{:metric 30 :state "warning"}
       {:metric 40 :state "warning"}
       {:metric 70 :state "critical"}
       {:metric 90 :state "critical"}]))
  (testing "without warning key"
    (test-stream (threshold {:critical 70})
      [{:metric 29}
       {:metric 30}
       {:metric 40}
       {:metric 70}
       {:metric 90}]
      [{:metric 70 :state "critical"}
       {:metric 90 :state "critical"}])))

(deftest condition-test
  (testing "condition"
    (test-stream (condition {:condition-fn #(and (>= (:metric %) 30)
                                                 (< (:metric %) 70))
                             :state "warning"})
      [{:metric 29}
       {:metric 30}
       {:metric 40}
       {:metric 70}
       {:metric 90}]
      [{:metric 30 :state "warning"}
       {:metric 40 :state "warning"}])))

(deftest condition-during-test
  (testing "should find event over 9000 during 10s "
    (test-stream (condition-during {:condition-fn #(and 
                                                     (> (:metric %) 9000) 
                                                     (compare (:service %) "power"))
                                       :duration 10
                                       :state "disaster"})
               [{:metric 10   :time 0  :service "power"}
                {:metric 9001 :time 1  :service "power"}
                {:metric 9001 :time 2  :service "bar"}
                {:metric 9002 :time 12 :service "power"}
                {:metric 9500 :time 13 :service "power"}
                {:metric 8000 :time 14 :service "power"}
                {:metric 9999 :time 15 :service "bar"}]
               [{:metric 9002 :state "disaster" :time 12 :service "power"}
                {:metric 9500 :state "disaster" :time 13 :service "power"}]))
  (testing "should not find event over 9000 during 10s with instabilized metrics "
    (test-stream (condition-during {:condition-fn #(and 
                                                     (> (:metric %) 9000) 
                                                     (compare (:service %) "power"))
                                       :duration 10
                                       :state "disaster"})
               [{:metric 10   :time 0  :service "power"}
                {:metric 9001 :time 1  :service "power"}
                {:metric 9001 :time 2  :service "bar"}
                {:metric 7000 :time 6  :service "power"}
                {:metric 9002 :time 12 :service "power"}
                {:metric 9500 :time 13 :service "power"}
                {:metric 8000 :time 14 :service "power"}
                {:metric 9999 :time 15 :service "bar"}]
               [])))

(deftest above-test
  (test-stream (above {:threshold 70 :duration 10})
               [{:metric 40 :time 0}
                {:metric 80 :time 1}
                {:metric 80 :time 12}
                {:metric 81 :time 13}
                {:metric 10 :time 14}]
               [{:metric 80 :state "critical" :time 12}
                {:metric 81 :state "critical" :time 13}])
  (test-stream (above {:threshold 70 :duration 10})
               [{:metric 80 :time 1}
                {:metric 40 :time 12}
                {:metric 90 :time 13}]
               []))




(deftest below-test
  (test-stream (below {:threshold 70 :duration 10})
               [{:metric 80 :time 0}
                {:metric 40 :time 1}
                {:metric 40 :time 12}
                {:metric 41 :time 13}
                {:metric 90 :time 14}]
               [{:metric 40 :state "critical" :time 12}
                {:metric 41 :state "critical" :time 13}]))

(deftest between-test
  (test-stream (between {:min-threshold 70
                         :max-threshold 90
                         :duration 10})
               [{:metric 100 :time 0}
                {:metric 80 :time 1}
                {:metric 80 :time 12}
                {:metric 81 :time 13}
                {:metric 10 :time 14}]
               [{:metric 80 :state "critical" :time 12}
                {:metric 81 :state "critical" :time 13}]))

(deftest outside-test
  (test-stream (outside {:min-threshold 70
                         :max-threshold 90
                         :duration 10})
               [{:metric 80 :time 0}
                {:metric 100 :time 1}
                {:metric 101 :time 12}
                {:metric 1 :time 13}
                {:metric 70 :time 14}]
               [{:metric 101 :state "critical" :time 12}
                {:metric 1 :state "critical" :time 13}]))

(deftest critical-test
  (test-stream (critical {:duration 10})
               [{:time 0 :state "critical"}
                {:time 1 :state "critical"}
                {:time 12 :state "critical"}
                {:time 13 :state "critical"}
                {:time 14 :state "ok"}]
               [{:state "critical" :time 12}
                {:state "critical" :time 13}]))

(deftest generate-streams-test
  (let [out (atom [])
        child #(swap! out conj %)
        s (generate-streams {:condition [{:condition-fn #(and (>= (:metric %) 30)
                                                    (< (:metric %) 70))
                                          :state "warning"
                                          :children [child]}]})]
    (s {:metric 29})
    (s {:metric 30})
    (s {:metric 40})
    (s {:metric 70})
    (s {:metric 90})
    (is (= [{:metric 30 :state "warning"}
            {:metric 40 :state "warning"}]
           @out)))

  (let [out (atom [])
        child #(swap! out conj %)
        s (generate-streams {:condition-during [{:condition-fn #(and 
                                                                  (> (:metric %) 9000) 
                                                                  (compare (:service %) "power"))
                                          :duration 10
                                          :state "disaster"
                                          :children [child]}]})]
    (s {:metric 10   :time 0  :service "power"})
    (s {:metric 9001 :time 1  :service "power"})
    (s {:metric 9001 :time 2  :service "bar"})
    (s {:metric 9002 :time 12 :service "power"})
    (s {:metric 9500 :time 13 :service "power"})
    (s {:metric 8000 :time 14 :service "power"})
    (s {:metric 9999 :time 15 :service "bar"})
    (is (= [{:metric 9002 :state "disaster" :time 12 :service "power"}
            {:metric 9500 :state "disaster" :time 13 :service "power"}]
           @out)))


  (let [out (atom [])
        child #(swap! out conj %)
        s (generate-streams {:critical [{:duration 10
                                         :children [child]}]})]
    (s {:time 0 :service "baz" :state "critical"})
    (s {:time 1 :service "bar" :state "critical"})
    (s {:time 12 :service "bar" :state "critical"})
    (s {:time 13 :service "lol" :state "critical"})
    (s {:time 14  :service "bar" :state "ok"})
    (is (= @out [{:state "critical" :time 12 :service "bar"}
                 {:state "critical" :time 13 :service "lol"}])))

  (let [out (atom [])
        child #(swap! out conj %)
        s (generate-streams {:regex-dt [{:pattern ".*(?i)error.*"
                                         :duration 20
                                         :children [child]}]})]
    (s {:time 0 :metric "foo"})
    (s {:time 10 :metric "bar"})
    (s {:time 30 :metric "error"})
    (s {:time 51 :metric "ggwp ERROR"})
    (is (= @out [{:time 51 :metric "ggwp ERROR" :state "critical"}])))

  (let [out (atom [])
        child #(swap! out conj %)
        s (generate-streams {:critical [{:where #(= (:service %) "bar")
                                         :duration 10
                                         :children [child]}]})]
    (s {:time 0 :service "bar" :state "critical"})
    (s {:time 1 :service "bar" :state "critical"})
    (s {:time 12 :service "bar" :state "critical"})
    (s {:time 13 :service "bar" :state "critical"})
    (s {:time 14  :service "bar" :state "ok"})
    (is (= @out [{:state "critical" :time 12 :service "bar"}
                 {:state "critical" :time 13 :service "bar"}])))
  (let [out (atom [])
        child #(swap! out conj %)
        s (generate-streams {:critical [{:where #(= (:service %) "bar")
                                         :duration 10
                                         :children [child]}]
                             :threshold [{:where #(= (:service %) "foo")
                                          :warning 30
                                          :critical 70
                                          :children [child]}]})]
    (s {:time 0 :service "bar" :state "critical"})
    (s {:time 1 :service "bar" :state "critical"})
    (s {:time 12 :service "bar" :state "critical"})
    (s {:time 13 :service "bar" :state "critical"})
    (s {:time 14  :service "bar" :state "ok"})
    (s {:service "foo" :metric 29})
    (s {:service "foo" :metric 30})
    (s {:service "foo" :metric 40})
    (s {:service "foo" :metric 70})
    (s {:service "bar" :metric 70})
    (s {:service "foo" :metric 90})
    (is (= @out [{:state "critical" :time 12 :service "bar"}
                 {:state "critical" :time 13 :service "bar"}
                 {:service "foo" :metric 30 :state "warning"}
                 {:service "foo" :metric 40 :state "warning"}
                 {:service "foo" :metric 70 :state "critical"}
                 {:service "foo" :metric 90 :state "critical"}]))))


(deftest percentile-crit-test
  (test-stream (percentile-crit {:service "api req"
                                 :critical-fn #(> (:metric %) 100)
                                 :point 0.99})
    [{:time 0 :metric 110 :service "api req 0.99"}
     {:time 0 :metric 110 :service "api req 0.95"}
     {:time 0 :metric 90 :service "api req 0.99"}]
    [{:time 0 :metric 110 :state "critical" :service "api req 0.99"}]))

(deftest percentiles-crit-test
  (let [out1 (atom [])
        child1 #(swap! out1 conj %)
        out2 (atom [])
        child2 #(swap! out2 conj %)
        s (percentiles-crit {:service "api req"
                             :duration 20
                             :points {1 {:critical-fn #(> (:metric %) 100)
                                         :warning-fn #(> (:metric %) 100)}
                                      0.50 {:critical-fn #(> (:metric %) 500)}
                                      0 {:critical-fn #(> (:metric %) 1000)
                                         :critical 1000}}}
            child1
            child2)]
    (s {:time 0 :service "api req" :metric 0})
    (s {:time 0 :service "api req" :metric 100})
    (s {:time 1 :service "api req" :metric 200})
    (advance! 22)
    (s {:time 23 :service "api req" :metric 30})
    (s {:time 23 :service "api req" :metric 40})
    (s {:time 23 :service "api req" :metric 501})
    (s {:time 23 :service "api req" :metric 503})
    (s {:time 24 :service "api req" :metric 600})
    (s {:time 24 :service "api req" :metric 600})
    (s {:time 25 :service "api req" :metric 601})
    (advance! 41)
    (is (= @out1
          [{:time 1 :service "api req 1" :metric 200 :state "warning"}
           {:time 1 :service "api req 1" :metric 200 :state "critical"}
           {:time 25 :service "api req 1" :metric 601 :state "warning"}
           {:time 25 :service "api req 1" :metric 601 :state "critical"}
           {:time 23 :service "api req 0.5" :metric 503 :state "critical"}]))
    (is (= @out2
          [{:time 1 :service "api req 1" :metric 200}
           {:time 0 :service "api req 0.5" :metric 100}
           {:time 0 :service "api req 0" :metric 0}
           {:time 25 :service "api req 1" :metric 601}
           {:time 23 :service "api req 0.5" :metric 503}
           {:time 23 :service "api req 0" :metric 30}]))))

(deftest scount-test
  (test-stream (scount {:duration 20})
    [{:time 1}
     {:time 19}
     {:time 30}
     {:time 31}
     {:time 35}
     {:time 61}]
    [{:time 1 :metric 2}
     {:time 30 :metric 3}
     ;; no event during the time window
     (riemann.common/event {:metric 0 :time 61})]))

(deftest scount-crit-test
  (test-stream (scount-crit {:service "foo"
                             :duration 20
                             :critical-fn #(> (:metric %) 5)})
    [{:time 1}
     {:time 2}
     {:time 3}
     {:time 4}
     {:time 5}
     {:time 19}
     {:time 30}
     {:time 31}
     {:time 35}
     {:time 61}]
    [{:time 1 :metric 6 :state "critical"}]))

(deftest regex-dt-test
  (test-stream (regex-dt {:pattern ".*(?i)error.*" 
                          :duration 20})
    [{:time 0  :metric "foo"}
     {:time 10 :metric "bar"}
     {:time 30 :metric "error"}
     {:time 51 :metric "ggwp Error"}
     ]
    [{:time 51 :metric "ggwp Error" :state "critical"}]))

(def kafka-output #(println % " => event"))
