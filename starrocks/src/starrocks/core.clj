(ns starrocks.core
  (:gen-class)
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [cli :as cli]
                    [checker :as checker]
                    [core :as jepsen]
                    [generator :as gen]
                    [os :as os]
                    [tests :as tests]
                    [util :as util]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [starrocks [db :as db]
                       [tables :as tables]]))

(defn starrocks-test
  [opts]
  (merge tests/noop-test
         opts
         {:name "starrocks"
          :os   debian/os
          :db   (db/db)}
         (tables/workload opts)))

(defn -main
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn starrocks-test}))
            args))