(ns starrocks.db
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [cli :as cli]
                    [control :as c]
                    [checker :as checker]
                    [core :as jepsen]
                    [generator :as gen]
                    [db :as db]
                    [util :as util]]
            [slingshot.slingshot :refer [try+ throw+]]
            [jepsen.control.util :as cu]
            [starrocks.sql :as sql]))

(def starrocs-home "/root")
(def fe-start-bin  (str starrocs-home "/fe/bin/start_fe.sh"))
(def fe-http-port 8030)

(defn http-url
  [node port url]
  (str "http://" node ":" port url))

(defn page-ready?
  "Fetches a status page URL on the local node, and returns true iff the page
  was available."
  [url]
  (try+
   (info "try to call url" url)
   (c/exec :curl :--fail url)
   (catch [:type :jepsen.control/nonzero-exit] _ false)))

(defn wait-to-ready
  [url]
  (loop [ready false]
    (Thread/sleep 1000)

    (let [ready (page-ready? url)]
      (if ready
        ; done
        ready
        ; still starting
        (recur ready)))))

(defn stop-fe!
  [test node]
  (info "stop-fe! called" node)
  (c/su
    (cu/grepkill! :java)))

(defn start-fe!
  [test node]
  (info "start-fe! called" node)
  (c/su
    (c/exec fe-start-bin :--daemon)))

(defn db
  []
  (reify db/DB
    (setup! [_ test node]
      (c/su
       (info node "setup starrocks")

       (c/exec fe-start-bin :--daemon)

       (wait-to-ready (http-url node fe-http-port "/api/health"))

       ; sleep 10s to wait for the new master log
       (util/sleep 10000)

       (let [conn (sql/open node {})]
         (sql/execute! conn ["drop database if exists test"]))))

    (teardown! [_ test node]
      (c/su
        (info node "teardown starrocks")
        (cu/grepkill! :java)))))
