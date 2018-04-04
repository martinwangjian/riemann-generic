(ns riemann-generic.core
  (:require [riemann.streams :refer :all]
            [riemann.config :refer :all]
            [riemann.test :refer :all]
            [riemann-cond-dt.core :as dt]
            [clojure.tools.logging :refer :all]))

;; (setq clojure-defun-style-default-indent t)

(defn threshold
  "Compare the `:metric` event value with the values of `:warning` in `:critical`
  in `opts` and update the event state accordely, and forward to children.

  `opts` keys:
  - `:critical` : A number, the event `:state` will be set to `critical` if the
  event metric is >= to the value. (optional)
  - `:warning`  : A number, the event `:state` will be set to `warning` if the
  event metric is < to `:critical` and >= to `:warning` (optional)

  Example:

  (threshold {:warning 30 :critical 70} email)"
  [opts & children]
  (let [child-streams (remove nil?
                        [(when (:warning opts)
                           (where (and (< (:metric event) (:critical opts))
                                    (>= (:metric event) (:warning opts)))
                             (with :state "warning"
                               (fn [event]
                                 (call-rescue event children)))))
                         (when (:critical opts)
                           (where (>= (:metric event) (:critical opts))
                             (with :state "critical"
                               (fn [event]
                                 (call-rescue event children)))))])]
    (apply sdo child-streams)))

(defn condition
  "Use the `:condition-fn` value (which should be function accepting an event) to set the event `:state` accordely. Forward events to children

  `opts` keys:
  - `:condition-fn` : A function accepting an event and returning a boolean 
  - `:state`        : The state of event forwarded to children.

  Example:

  (condition {:condition-fn #(and (>= (:metric %) 30)
                                  (< (:metric %) 70))
              :state \"warning\"})

  In this example, event :state will be \"warning\" if `:metric` is >= 30 and < 70" 
  [opts & children]
  (let [child-stream (remove nil?
                        [(when (:condition-fn opts)
                          (where ((:condition-fn opts) event)
                            (with :state (:state opts)
                              (fn [event]
                                (call-rescue event children)))))])]
    (apply sdo child-stream)))

(defn condition-during
  "if the condition `condition-fn`(which should be function accepting an event) is valid for all events received during at least the period `dt`, valid events received after the `dt` period will be passed on until an invalid event arrives. Forward to children.
  `:metric` should not be nil (it will produce exceptions).

  `opts` keys:
  - `:condition-fn` : A function accepting an event and returning a boolean.
  - `:duration`     : The time period in seconds.
  - `:state`        : The state of event forwarded to children.

  Example:

  (condition-during {:condition-fn #(and 
                                      (> (:metric %) 42) 
                                      (compare (:service %) \"foo\"))
                     :duration 10
                     :state \"critical\"})

  Set `:state` to \"critical\" if events `:metric` is > to 42 and `:metric` is \"foo\" during 10 sec or more."
  [opts & children]
  (dt/cond-dt (:condition-fn opts) (:duration opts) 
    (with :state (:state opts) 
      (fn [event]
        (call-rescue event children)))))

(defn above-during
  "If the condition `(> (:metric event) threshold)` is valid for all events received during at least the period `dt`, valid events received after the `dt` period will be passed on until an invalid event arrives. Forward to children.
  `:metric` should not be nil (it will produce exceptions).

  `opts` keys:
  - `:threshold` : The threshold used by the above stream
  - `:duration`  : The time period in seconds.
  - `:state`     : The state of event forwarded to children.

  Example:

  (above-during {:threshold 70 :duration 10 :state \"critical\"} email)

  Set `:state` to \"critical\" if events `:metric` is > to 70 during 10 sec or more."
  [opts & children]
  (dt/above (:threshold opts) (:duration opts)
    (with :state (:state opts)
      (fn [event]
        (call-rescue event children)))))

(defn below-during
  "If the condition `(< (:metric event) threshold)` is valid for all events received during at least the period `dt`, valid events received after the `dt` period will be passed on until an invalid event arrives. Forward to children.
  `:metric` should not be nil (it will produce exceptions).

  `opts` keys:
  - `:threshold` : The threshold used by the above stream
  - `:duration`  : The time period in seconds.
  - `:state`     : The state of event forwarded to children.

  Example:

  (below-during {:threshold 70 :duration 10 :state \"critical\"} email)

  Set `:state` to \"critical\" if events `:metric` is < to 70 during 10 sec or more."
  [opts & children]
  (dt/below (:threshold opts) (:duration opts)
    (with :state (:state opts)
      (fn [event]
        (call-rescue event children)))))

(defn outside-during
  "If the condition `(or (< (:metric event) low) (> (:metric event) high))` is valid for all events received during at least the period `dt`, valid events received after the `dt` period will be passed on until an invalid event arrives.

  `opts` keys:
  - `:min-threshold` : The min threshold
  - `:max-threshold` : The max threshold
  - `:duration`      : The time period in seconds.
  - `:state`         : The state of event forwarded to children.

  Example:

  (outside-during {:min-threshold 70
                   :max-threshold 90
                   :duration 10
                   :state \"critical\"})

  Set `:state` to \"critical\" if events `:metric` is < to 70 or > 90 during 10 sec or more."
  [opts & children]
  (dt/outside (:min-threshold opts) (:max-threshold opts) (:duration opts)
    (with :state (:state opts)
      (fn [event]
        (call-rescue event children)))))

(defn between-during
  "If the condition `(and (> (:metric event) low) (< (:metric event) high))` is valid for all events received during at least the period `dt`, valid events received after the `dt` period will be passed on until an invalid event arrives.

  `:metric` should not be nil (it will produce exceptions).
  `opts` keys:
  - `:min-threshold` : The min threshold
  - `:max-threshold` : The max threshold
  - `:duration`      : The time period in seconds.
  - `:state`         : The state of event forwarded to children.

  Example:

  (between-during {:min-threshold 70
                   :max-threshold 90
                   :duration 10
                   :service \"bar\"
                   :state \"critical\"})

  Set `:state` to \"critical\" if events `:metric` is > to 70 and < 90 during 10 sec or more."
  [opts & children]
  (dt/between (:min-threshold opts) (:max-threshold opts) (:duration opts)
    (with :state (:state opts)
      (fn [event]
        (call-rescue event children)))))

(defn regex-during
  "if regex `:pattern` matched all events received during at least the period `dt`, matched events received after the `dt` period will be passed on until an invalid event arrives.  The matched event `:state` will be set to `critical` and forward to children.
  `:metric` should not be nil (it will produce exceptions).


  `opts` keys:
  - `:pattern`  : A string regex
  - `:duration` : The time period in seconds.
  - `:state`    : The state of event forwarded to children.

  Example:

  (regex-dt {:pattern '.*(?i)error.*' :duration 10 :state \"critical\"} 
            children)

  Set `:state` to \"critical\" if metric of events contain \"error\" during 10 sec or more."
  [opts & children]

  (apply condition-during {:condition-fn #(re-matches (re-pattern (:pattern opts)) (:metric %)) 
                           :duration (:duration opts) 
                           :state (:state opts)} 
                          children))

(defn percentile-crit
  [opts & children]
  (let [child-streams (remove nil?
                  [(when-let [warning-fn (:warning-fn opts)]
                     (where (warning-fn event)
                       (with :state "warning"
                         (fn [event]
                           (call-rescue event children)))))
                   (when-let [critical-fn (:critical-fn opts)]
                     (where (critical-fn event)
                       (with :state "critical"
                         (fn [event]
                           (call-rescue event children)))))])]
    (where (service (str (:service opts) " " (:point opts)))
      (apply sdo child-streams))))

(defn percentiles-crit
  "Calculates percentiles and alert on it.

  `opts` keys:
  - `:service`   : Filter all events using `(service (:service opts))`
  - `:duration`  : The time period in seconds.
  - `:points`    : A map, the keys are the percentiles points.
  The value should be a map with these keys:
  - `:critical-fn` a function accepting an event and returning a boolean (optional).
  - `:warning-fn` a function accepting an event and returning a boolean (optional).
  For each point, if the event match `:warning-fn` and `:critical-fn`, the event `:state` will be \"warning\" or \"critical\"

Example:

(percentiles-crit {:service \"api req\"
                   :duration 20
                   :points {1 {:critical-fn #(> (:metric %) 100)
                               :warning-fn #(> (:metric %) 100)}
                            0.50 {:critical-fn #(> (:metric %) 500)}
                            0 {:critical-fn #(> (:metric %) 1000)}}}"
  [opts & children]
  (let [points (mapv first (:points opts))
        percentiles-streams (mapv (fn [[point conf]]
                                    (percentile-crit
                                     (assoc conf :service (:service opts)
                                                 :point point)
                                      (first children)))
                              (:points opts))
        children (conj percentiles-streams (second children))]
    (where (service (:service opts))
      (percentiles (:duration opts) points
        (fn [event]
          (call-rescue event children))))))

(defn scount
  "Takes a time period in seconds `:duration`.

  Lazily count the number of events in `:duration` seconds time windows.
  Forward the result to children

  `opts` keys:
  - `:duration`   : The time period in seconds.

  Example:

  (scount {:duration 20} children)

  Will count the number of events in 20 seconds time windows and forward the result to children."
  [opts & children]
  (fixed-time-window (:duration opts)
    (smap riemann.folds/count
      (fn [event]
        (call-rescue event children)))))

(defn scount-crit
  "Takes a time period in seconds `:duration`.

  Lazily count the number of events in `:duration` seconds time windows.
  Use the `:warning-fn` and `:critical-fn` values (which should be function
  accepting an event) to set the event `:state` accordely

  Forward the result to children

  `opts` keys:
  - `:duration`   : The time period in seconds.
  - `:critical-fn` : A function accepting an event and returning a boolean (optional).
  - `:warning-fn`  : A function accepting an event and returning a boolean (optional).
  Example:

  (scount-crit {:duration 20 :critical-fn #(> (:metric %) 5)} children)

  Will count the number of events in 20 seconds time windows. If the count result
  is > to 5, set `:state` to \"critical\" and forward and forward the result to
  children."
  [opts & children]
  (let [child-streams (remove nil? [(when-let [critical-fn (:critical-fn opts)]
                                      (where  (critical-fn event)
                                        (with :state "critical"
                                          (fn [event]
                                            (call-rescue event children)))))
                                    (when-let [warning-fn (:warning-fn opts)]
                                      (where (warning-fn event)
                                        (with :state "warning"
                                          (fn [event]
                                            (call-rescue event children)))))])]
    (scount opts
      (apply sdo child-streams))))

(defn expired-host
  [opts & children]
  (sdo
    (where (not (expired? event))
      (with {:service "host up"
             :ttl (:ttl opts)}
        (by :host
          (throttle 1 (:throttle opts)
            (index)))))
    (expired
      (where (service "host up")
        (with :description "host stopped sending events to Riemann"
          (fn [event]
            (call-rescue event children)))))))

(defn generate-stream
  [[stream-key streams-config]]
  (let [s (condp = stream-key
            :threshold threshold
            :condition condition
            :condition-during condition-during
            :above-during above-during
            :below-during below-during
            :outside-during outside-during
            :between-during between-during
            :regex-during regex-during
            :scount scount
            :scount-crit scount-crit
            :percentiles-crit percentiles-crit)
        streams (mapv (fn [config]
                        (let [children (:children config)
                              stream (apply (partial s
                                              (dissoc config :children :match))
                                       children)]
                          (if-let [match-clause (:where config)]
                            (where (match-clause event)
                              stream)
                            stream)))
                     streams-config)]
     (apply sdo streams)))

(defn generate-streams
  [config]
  (let [children (mapv generate-stream config)]
    (fn [event]
      (call-rescue event children))))
