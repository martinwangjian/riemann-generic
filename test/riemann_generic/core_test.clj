(ns riemann-generic.core-test
  (:require [clojure.test :refer :all]
            [riemann.time.controlled :refer :all]
            [riemann.time :refer :all]
            [riemann.test :refer [run-stream run-stream-intervals test-stream
                                  with-test-stream test-stream-intervals]]
            [riemann-generic.core :refer :all]))

(use-fixtures :once control-time!)
(use-fixtures :each reset-time!)

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
  (test-stream (above {:threshold 70 :state "critical"})
    [{:metric 29}
     {:metric 30}
     {:metric 40}
     {:metric 70}
     {:metric 90}]
    [{:metric 90 :state "critical"}]))

(deftest above-during-test
  (test-stream (above-during {:threshold 70 :duration 10 :state "critical"})
               [{:metric 40 :time 0}
                {:metric 80 :time 1}
                {:metric 80 :time 12}
                {:metric 81 :time 13}
                {:metric 10 :time 14}]
               [{:metric 80 :state "critical" :time 12}
                {:metric 81 :state "critical" :time 13}])
  (test-stream (above-during {:threshold 70 :duration 10 :state "warning"})
               [{:metric 80 :time 1}
                {:metric 40 :time 12}
                {:metric 90 :time 13}]
               []))

(deftest below-test
  (test-stream (below {:threshold 9000 :state "critical"})
    [{:metric 8000}
     {:metric 9000}
     {:metric 9001}
     {:metric 8999}
     {:metric 9999}]
    [{:metric 8000 :state "critical"}
     {:metric 8999 :state "critical"}]))

(deftest below-during-test
  (test-stream (below-during {:threshold 70 :duration 10 :state "critical"})
               [{:metric 80 :time 0}
                {:metric 40 :time 1}
                {:metric 40 :time 12}
                {:metric 41 :time 13}
                {:metric 90 :time 14}]
               [{:metric 40 :state "critical" :time 12}
                {:metric 41 :state "critical" :time 13}]))

(deftest outside-test
  (test-stream (outside {:min-threshold 70
                         :max-threshold 90
                         :state "critical"})
               [{:metric 80  :time 0}
                {:metric 100 :time 1}
                {:metric 101 :time 12}
                {:metric 1   :time 13}
                {:metric 70  :time 14}]
               [{:metric 100 :state "critical" :time 1}
                {:metric 101 :state "critical" :time 12}
                {:metric 1   :state "critical" :time 13}]))

(deftest outside-during-test
  (test-stream (outside-during {:min-threshold 70
                                :max-threshold 90
                                :duration 10
                                :state "critical"})
               [{:metric 80  :time 0}
                {:metric 100 :time 1}
                {:metric 101 :time 12}
                {:metric 1   :time 13}
                {:metric 70  :time 14}]
               [{:metric 101 :state "critical" :time 12}
                {:metric 1   :state "critical" :time 13}]))

(deftest between-test
  (test-stream (between {:min-threshold 70
                         :max-threshold 90
                         :state "critical"})
               [{:metric 99 :time 0}
                {:metric 80 :time 1}
                {:metric 80 :time 12}
                {:metric 81 :time 13}
                {:metric 10 :time 14}]
               [{:metric 80 :state "critical" :time 1}
                {:metric 80 :state "critical" :time 12}
                {:metric 81 :state "critical" :time 13}]))

(deftest between-during-test
  (test-stream (between-during {:min-threshold 70
                                :max-threshold 90
                                :duration 10
                                :state "critical"})
               [{:metric 99 :time 0}
                {:metric 80 :time 1}
                {:metric 80 :time 12}
                {:metric 81 :time 13}
                {:metric 10 :time 14}]
               [{:metric 80 :state "critical" :time 12}
                {:metric 81 :state "critical" :time 13}]))

(deftest regex-test
  (test-stream (regex {:pattern ".*(?i)error.*" :state "critical"})
    [{:time 0  :metric "foo"}
     {:time 10 :metric "bar"}
     {:time 30 :metric "error"}
     {:time 51 :metric "ggwp Error"}]
    [{:time 30 :metric "error" :state "critical"}
     {:time 51 :metric "ggwp Error" :state "critical"}]))

(deftest regex-during-test
  (test-stream (regex-during {:pattern ".*(?i)error.*" 
                              :duration 20
                              :state "critical"})
    [{:time 0  :metric "foo"}
     {:time 10 :metric "bar"}
     {:time 30 :metric "error"}
     {:time 51 :metric "ggwp Error"}]
    [{:time 51 :metric "ggwp Error" :state "critical"}]))

(deftest ddt-above-test
  (test-stream (ddt-above {:dt 2
                           :threshold 5
                           :state "disaster" 
                           :new-service "ddt-foo"})
    [{:time 0  :metric 0}
     {:time 1  :metric 0}
     {:time 2  :metric -1};ignored
     {:time 3  :metric 2}
     {:time 4  :metric -3};ignored
     {:time 5  :metric 14} 
     {:time 6  :metric 20};ignored
     {:time 7  :metric 15} 
     {:time 8  :metric 20};ignored
     {:time 9  :metric -1}
     {:time 10 :metric 0}];ignored
    [{:time 6  :metric 6 :state "disaster" :service "ddt-foo"}])
  (test-stream (ddt-above {:dt 3
                           :threshold 5
                           :state "warning" 
                           :new-service "ddt-bar"})
    [{:time 0  :metric 0}
     {:time 1  :metric 0};ignored
     {:time 2  :metric -1}
     {:time 3  :metric -1};ignored
     {:time 4  :metric 2} ;ignored
     {:time 5  :metric -3}
     {:time 6  :metric -3};ignored
     {:time 7  :metric 20};ignored
     {:time 8  :metric 21}
     {:time 9  :metric 7};ignored
     {:time 10 :metric 21};ignored
     {:time 11 :metric 42}
     {:time 12 :metric 42};ignored
     {:time 13 :metric -1};ignored
     {:time 14 :metric 0}
     {:time 15 :metric 0};ignored
     {:time 16 :metric 0}];ignored
    [{:time 9  :metric 8 :state "warning" :service "ddt-bar"}
     {:time 12 :metric 7 :state "warning" :service "ddt-bar"}]))

(deftest downsample-test
  (test-stream (downsample {:by [:host :service]
                            :duration 3
                            :ttl 300
                            :new-service-fn #(str "received:" (:service %) "_from:" (:host %))})
    [{:time 0  :metric 0 :service "foo" :host "a" }
     {:time 1  :metric 1 :service "foo" :host "b" }
     {:time 1  :metric 1 :service "bar" :host "b" }
     {:time 2  :metric 2 :service "foo" :host "b" }
     {:time 3  :metric 3 :service "foo" :host "a" }
     {:time 3  :metric 3 :service "bar" :host "a" }
     {:time 4  :metric 4 :service "foo" :host "a" }
     {:time 5  :metric 5 :service "foo" :host "b" }
     {:time 6  :metric 6 :service "foo" :host "b" }
     {:time 6  :metric 6 :service "bar" :host "b" }
     {:time 7  :metric 7 :service "foo" :host "a" }]
    [{:time 0, :metric 0, :service "received:foo_from:a", :host "a", :ttl 300} 
     {:time 1, :metric 1, :service "received:foo_from:b", :host "b", :ttl 300}
     {:time 1, :metric 1, :service "received:bar_from:b", :host "b", :ttl 300} 
     {:time 3, :metric 3, :service "received:foo_from:a", :host "a", :ttl 300}
     {:time 3, :metric 3, :service "received:bar_from:a", :host "a", :ttl 300}
     {:time 5, :metric 5, :service "received:foo_from:b", :host "b", :ttl 300} 
     {:time 6, :metric 6, :service "received:bar_from:b", :host "b", :ttl 300}
     {:time 7, :metric 7, :service "received:foo_from:a", :host "a", :ttl 300}
    ]))

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
        s (generate-streams {:regex-during [{:pattern ".*(?i)error.*"
                                             :duration 20
                                             :state "critical"
                                             :children [child]}]})]
    (s {:time 0 :metric "foo"})
    (s {:time 10 :metric "bar"})
    (s {:time 30 :metric "error"})
    (s {:time 51 :metric "ggwp ERROR"})
    (is (= @out [{:time 51 :metric "ggwp ERROR" :state "critical"}])))

  (let [out (atom [])
        child #(swap! out conj %)
        s (generate-streams {:above [{:where #(= (:service %) "foo")
                                          :threshold 30
                                          :state "critical"
                                          :children [child]}]})]
    (s {:service "foo" :metric 29})
    (s {:service "foo" :metric 30})
    (s {:service "foo" :metric 40})
    (s {:service "foo" :metric 70})
    (s {:service "bar" :metric 70})
    (s {:service "foo" :metric 90})
    (is (= @out [{:service "foo" :metric 40 :state "critical"}
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
  (test-stream (scount {:duration 30})
    [{:time 1}
     {:time 19}
     {:time 30}
     {:time 31}
     {:time 35}
     {:time 61}]
    [{:time 1  :metric 3}
     {:time 31 :metric 2}
     ;; no event during the time window
     ]))

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

(def kafka-output #(println % " => event"))
