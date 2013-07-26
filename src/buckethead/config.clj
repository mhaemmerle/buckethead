(ns buckethead.config
  (:refer-clojure :exclude [get]))

(def configuration {:bucket ""
                    :aws {:access-key ""
                          :secret-key ""}})

(defn get
  [key]
  (clojure.core/get configuration key))
