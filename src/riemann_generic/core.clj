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

(defn threshold-fn
  "Use the `:warning-fn` and `:critical-fn` values (which should be function
  accepting an event) to set the event `:state` accordely. Forward events to
  children

  `opts` keys:
  - `:critical-fn` : A function accepting an event and returning a boolean (optional).
  - `:warning-fn`  : A function accepting an event and returning a boolean (optional).

  Example:

  (threshold-fn {:warning-fn #(and (>= (:metric %) 30)
                                   (< (:metric %) 70))
                 :critical-fn #(>= (:metric %) 70)})

  In this example, event :state will be \"warning\" if `:metric` is >= 30 and < 70
  and \"critical\" if `:metric` is >= 70"
  [opts & children]
  (let [child-streams (remove nil?
                        [(when (:warning-fn opts)
                           (where ((:warning-fn opts) event)
                                  (with :state "warning"
                                        (fn [event]
                                          (call-rescue event children)))))
                         (when (:critical-fn opts)
                           (where ((:critical-fn opts) event)
                                  (with :state "critical"
                                        (fn [event]
                                          (call-rescue event children)))))])]
    (apply sdo child-streams)))

(defn above
  "If the condition `(> (:metric event) threshold)` is valid for all events
  received during at least the period `dt`, valid events received after the `dt`
  period will be passed on until an invalid event arrives. Forward to children.
  `:metric` should not be nil (it will produce exceptions).

  `opts` keys:
  - `:threshold` : The threshold used by the above stream
  - `:duration`   : The time period in seconds.

  Example:

  (above {:threshold 70 :duration 10} email)

  Set `:state` to \"critical\" if events `:metric` is > to 70 during 10 sec or more."
  [opts & children]
  (dt/above (:threshold opts) (:duration opts)
    (with :state "critical"
      (fn [event]
        (call-rescue event children)))))

(defn threshold-during-fn
  "if the condition `threshold-fn`(which should be function
  accepting an event)is valid for all events
  received during at least the period `dt`, valid events received after the `dt`
  period will be passed on until an invalid event arrives. Forward to children.
  `:metric` should not be nil (it will produce exceptions).

  `opts` keys:
  - `:threshold-fn` : A function accepting an event and returning a boolean.
  - `:duration`     : The time period in seconds.
  - `:state`        : The state of event forwarded to children.

  Example:

  (threshold-during-fn {:threshold-fn #(and 
                                           (> (:metric %) 42) 
                                           (compare (:service %) \"foo\"))
                        :duration 10
                        :state \"critical\"})

  Set `:state` to \"critical\" if events `:metric` is > to 42 and `:metric` is \"foo\" during 10 sec or more."
  [opts & children]
  (dt/cond-dt (:threshold-fn opts) (:duration opts) 
    (with :state (:state opts) 
      (fn [event]
        (call-rescue event children)))))


(defn below
  "If the condition `(< (:metric event) threshold)` is valid for all events
  received during at least the period `dt`, valid events received after the `dt`
  period will be passed on until an invalid event arrives. Forward to children.
  `:metric` should not be nil (it will produce exceptions).

  `opts` keys:
  - `:threshold` : The threshold used by the above stream
  - `:duration`   : The time period in seconds.

  Example:

  (below {:threshold 70 :duration 10} email)

  Set `:state` to \"critical\" if events `:metric` is < to 70 during 10 sec or more."
  [opts & children]
  (dt/below (:threshold opts) (:duration opts)
    (with :state "critical"
      (fn [event]
        (call-rescue event children)))))

(defn outside
  "If the condition `(or (< (:metric event) low) (> (:metric event) high))` is valid for all events received during at least the period `dt`, valid events received after the `dt` period will be passed on until an invalid event arrives.

  `opts` keys:
  - `:min-threshold` : The min threshold
  - `:max-threshold` : The max threshold
  - `:duration`   : The time period in seconds.

  Example:

  (outside {:min-threshold 70
            :max-threshold 90
            :duration 10})

  Set `:state` to \"critical\" if events `:metric` is < to 70 or > 90 during 10 sec or more."
  [opts & children]
  (dt/outside (:min-threshold opts) (:max-threshold opts) (:duration opts)
    (with :state "critical"
      (fn [event]
        (call-rescue event children)))))

(defn between
  "If the condition `(and (> (:metric event) low) (< (:metric event) high))` is valid for all events received during at least the period `dt`, valid events received after the `dt` period will be passed on until an invalid event arrives.

  `:metric` should not be nil (it will produce exceptions).
  `opts` keys:
  - `:min-threshold` : The min threshold
  - `:max-threshold` : The max threshold
  - `:duration`   : The time period in seconds.

  Example:

  (between {:min-threshold 70
            :max-threshold 90
            :duration 10
            :service \"bar\"})

  Set `:state` to \"critical\" if events `:metric` is > to 70 and < 90 during 10 sec or more."
  [opts & children]
  (dt/between (:min-threshold opts) (:max-threshold opts) (:duration opts)
    (with :state "critical"
      (fn [event]
        (call-rescue event children)))))

(defn critical
  "Takes a time period in seconds `durationt`.
  If all events received during at least the period `durationt` have `:state` critical, new critical events received after the `durationt` period will be passed on until an invalid event arrives.

  `opts` keys:
  - `:duration`   : The time period in seconds.

  Example:

  (critical {:duration \"10\"} email)

  Set `:state` to \"critical\" if events `:state` is critical during 10 sec or more."
  [opts & children]
  (dt/critical (:duration opts)
    (fn [event]
      (call-rescue event children))))

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
  (fixed-time-window 20
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

  (scount {:duration 20 :critical-fn #(> (:metric %) 5)} children)

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
            :threshold-fn threshold-fn
            :threshold-during-fn threshold-during-fn
            :above above
            :below below
            :scount scount
            :scount-crit scount-crit
            :outside outside
            :percentiles-crit percentiles-crit
            :between between
            :critical critical)
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
