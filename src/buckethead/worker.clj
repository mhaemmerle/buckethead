(ns buckethead.worker
  (:require [clojure.tools.logging :as log]
            [die.roboter :as roboter]
            [aws.sdk.s3 :refer [get-object]]
            [cheshire.core :refer [parse-string]]
            [buckethead.config :as config]))

(defn work
  [object-key]
  (log/info "received work" object-key)
  (let [result (get-object (config/get :aws) (config/get :bucket) object-key)
        content (parse-string (slurp (:content result)) true)
        response (count (:battle_log content))]
    (log/info "done working" response)
    response))

(defn start
  []
  (roboter/work))
