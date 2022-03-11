(ns starrocks.tables
  (:refer-clojure :exclude [test])
  (:require [clojure.string :as str]
            [jepsen [client :as client]
             [checker :as checker]
             [generator :as gen]]
            [knossos.op :as op]
            [clojure.tools.logging :refer [info warn]]
            [starrocks.sql :as c :refer :all]))

(defrecord SetClient [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open node test)))

  (setup! [this test]
    (info "do nothing in setup!"))

  (invoke! [this test op]
    (case (:f op)
      :create  (do (c/execute! conn [(str "create table t" (:value op))])
                   (assoc op :type :ok))

      :show (->> (c/query conn ["show tables"])
                 (mapv :Tables_in_test)
                 (assoc op :type :ok, :value))))

  (teardown! [_ test])

  (close! [_ test]
    (c/close! conn)))

(defn creates
  []
  (->> (range)
       (map (fn [x] {:type :invoke, :f :create, :value x}))
       (gen/seq)))

(defn shows
  []
  {:type :invoke, :f :show, :value nil})

(defn workload
  [opts]
  (let [c (:concurrency opts)]
    (info "concurrency is " c)
    {:client (SetClient. nil)
     :generator (->> (gen/reserve (/ c 2) (creates) (shows))
                     (gen/stagger 1/10))
     :checker (checker/set-full)}))
