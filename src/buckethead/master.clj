(ns buckethead.master
  (:require [clojure.tools.logging :as log]
            [die.roboter :as roboter]
            [aws.sdk.s3 :refer [list-objects]]
            [buckethead.config :as config]))

(def max-keys 5)
(def ^{:dynamic true} *max-queue-length* 4)
(def queue (java.util.concurrent.LinkedBlockingQueue. *max-queue-length*))
(def output (ref ()))

(defn consumer
  []
  (future
    (loop [object-key (.take queue)]
      (log/info "consumer" object-key)
      (let [result (roboter/send-back `(buckethead.worker/work ~object-key))]
        (log/info "result" @result))
      (log/info "consumer done")
      (dosync (alter output conj object-key))
      (when-let [k (.poll queue 60 java.util.concurrent.TimeUnit/SECONDS)]
        (log/info "found another element")
        (recur k)))))

(defn fetch-objects
  []
  (loop [marker nil]
    (let [options {:marker marker :max-keys max-keys}
          result (list-objects (config/get :aws) (config/get :bucket) options)]
      (doseq [object (:objects result)]
        (.put queue (:key object))
        (log/info "put" (:key object) "into queue"))
      ;; TODO wait for the queue to empty when next marker nil
      (when-let [next-marker (:next-marker result)]
        (log/info "next-marker" next-marker)
        (recur next-marker)))))

(defn start
  []
  (dorun (repeatedly 4 consumer))
  (fetch-objects))
