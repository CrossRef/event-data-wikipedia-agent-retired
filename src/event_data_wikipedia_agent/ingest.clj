(ns event-data-wikipedia-agent.ingest
    (:require [baleen.queue :as baleen-queue])
    (:import [wikipedia RCStreamLegacyClient]
             [java.util.logging Logger Level]
             [java.util UUID])
    (:require [clj-time.core :as clj-time]
              [clj-time.format :as clj-time-format]
              [clj-time.coerce :as clj-time-coerce]
              [config.core :refer [env]])
    (:require [clojure.tools.logging :refer [error info debug]]
              [clojure.data.json :as json]))

(defn- callback [context type-name args]
  (let [arg (first args)
        arg-str (.toString arg)
        parsed (json/read-str arg-str)
        timestamp (clj-time-coerce/from-long (* 1000 (get parsed "timestamp")))]
    ; Don't log the inputs, the volume would be too large to be worth it (GBs per day).
    (baleen-queue/enqueue-with-time context "input" timestamp arg-str false)))

(defn- error-callback []
  ; Just panic. Whole process will be restarted.
  ; EX_IOERR
  (System/exit 74))

(defn- new-client [context]
  (info "Subscribe" (env :recent-changes-subscribe-filter))
  (let [the-client (new RCStreamLegacyClient context callback (env :recent-changes-subscribe-filter) error-callback)]
    (.run the-client)))
 
(defn run
  "Run the ingestion, blocking. If the RCStreamLegacyClient dies, exit."
  [context]
    ; The logger is mega-chatty (~50 messages per second at INFO). We have alternative ways of seeing what's going on.
    (info "Start RC Stream...")
    (.setLevel (Logger/getLogger "io.socket") Level/OFF)
    
    ; Crash the process if there's a failure.  
    (new-client context)

    (info "RC Stream running."))
