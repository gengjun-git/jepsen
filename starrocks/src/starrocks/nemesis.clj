(ns starrocks.nemesis
  (:require [jepsen
             [client :as client]
             [control :as c]
             [nemesis :as nemesis]
             [net :as net]
             [generator :as gen]
             [util :as util :refer [letr]]]
    [jepsen.control.util :as cu]
    [jepsen.nemesis.time :as nt]
    [clojure.set :as set]
    [clojure.string :as str]
    [clojure.pprint :refer [pprint]]
    [starrocks.db :as db]
    [clojure.tools.logging :refer :all]
    [slingshot.slingshot :refer [try+ throw+]]))

(defn kill-gen
  []
  (->> (cycle [(gen/sleep 5)
               {:type :info, :f :start-fe}
               (gen/sleep 5)
               {:type :info, :f :stop-fe}])
       (gen/seq)))

(defn kill-gen2
  []
  {:type :info, :f :stop-fe})

(defn process-nemesis
  []
  (reify nemesis/Nemesis
    (setup! [this test] this)

    (invoke! [this test op]
      (let [nodes (:nodes test)
            nodes (case (:f op)
                    ; When resuming, resume all nodes
                    (:start-fe) nodes

                    (util/random-nonempty-subset nodes))
            ; If the op wants to give us nodes, that's great
            nodes (or (:value op) nodes)]
        (info "nodes to nemesis" nodes)
        (assoc op :value
                  (c/on-nodes test nodes
                              (fn [test node]
                                (case (:f op)
                                  :start-fe  (db/start-fe! test node)
                                  :stop-fe   (db/stop-fe! test node)))))))

    (teardown! [this test])))
