(ns event-data-wikipedia-agent.process
  "Process edit events in Wikipedia."

  (:require [baleen.queue :as baleen-queue]
            [baleen.web :as baleen-web]
            [baleen.util :as baleen-util])
  (:require [clojure.data.json :as json]
            [clojure.tools.logging :as l]
            [clojure.set :refer [difference]])
  (:require [org.httpkit.client :as http]
            [clj-time.coerce :as clj-time-coerce])
  (:import [java.net URLEncoder])
  )


(defn build-restbase-url
  "Build a URL that can be retrieved from RESTBase"
  [server-name title revision]
  (let [encoded-title (URLEncoder/encode (.replaceAll title " " "_"))]
    (str "https://" server-name "/api/rest_v1/page/html/" encoded-title "/" revision)))

(defn build-wikipedia-url
  "Build a URL that can be used to fetch the page via the normal HTML website."
  [server-name title]
  (str "https://" server-name "/w/index.php?" (#'http/query-string {:title title})))


(defn process-bodies
  "Process a diff between two HTML documents.
   Return [added-dois removed-dois]."
  [{old-revision :old-revision old-body :old-body
    new-revision :new-revision new-body :new-body
    title :title
    server-name :server-name
    input-event-id :input-event-id}]

  (let [old-dois (baleen-web/extract-dois-from-body old-body)
        new-dois (baleen-web/extract-dois-from-body new-body)
        added-dois (difference new-dois old-dois)
        removed-dois (difference old-dois new-dois)]

    [added-dois removed-dois]))

(defn process
  "Process a new input event by looking up old and new revisions.
  Return map in audit log format, with :input, :events and :meta."
  [data]
  
  (let [server-name (get data "server_name")
        server-url (get data "server_url")
        title (get data "title")
        old-revision (get-in data ["revision" "old"])
        new-revision (get-in data ["revision" "new"])
        old-restbase-url (build-restbase-url server-name title old-revision)
        new-restbase-url (build-restbase-url server-name title new-revision)
        
        {old-status :status old-body :body} @(http/get old-restbase-url)
        {new-status :status new-body :body} @(http/get new-restbase-url)

        timestamp (clj-time-coerce/from-long (* 1000 (get data "timestamp")))

        [added-dois removed-dois]  (when (and (= 200 old-status) (= 200 new-status))
                                    (process-bodies {:old-revision old-revision :old-body old-body
                                                     :new-revision new-revision :new-body new-body
                                                     :title title
                                                     :server-name server-name
                                                     :timestamp timestamp}))

        canonical-url (baleen-web/fetch-canonical-url (build-wikipedia-url server-name title))

        added-events (map (fn [doi] {:doi doi :event-id (baleen-util/new-uuid)}) added-dois)
        removed-events (map (fn [doi] {:doi doi :event-id (baleen-util/new-uuid)}) removed-dois)
        ]

        (when-not (= 200 old-status)
          (l/error "Failed to fetch" old-restbase-url))

        (when-not (= 200 new-status)
          (l/error "Failed to fetch" new-restbase-url))

    {:input data
     :events {:added added-events
              :removed removed-events}
     :meta {:url canonical-url}}))


(defn process-f
  [context change-event]
  (let [parsed-event (json/read-str change-event)]
    ; Only interested in 'edit' type events (not 'comment', 'categorize' etc).
    (if-not (= (get parsed-event "type") "edit")
      true
      (let [result (process parsed-event)
            result-str (json/write-str result)]
            
        (if (or (not-empty (-> result :events :added))
                (not-empty (-> result :events :removed)))
          (do
            (l/info "Event with data: " result)

            (baleen-queue/enqueue context "matched" result-str true)
            true)

          (do 
            ; If not matched, don't save them, the signal-to-noise ratio is too small.
            true))))))

(defn run
  "Run processing. Blocks forever."
  [context]
  (l/info "Run process.")
  (baleen-queue/process-queue context "input" (partial process-f context)))
