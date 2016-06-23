(ns event-data-wikipedia-agent.push
  "Push events to Lagotto."
  (:require [baleen.queue :as baleen-queue]
            [baleen.web :as baleen-web]
            [baleen.util :as baleen-util]
            [baleen.lagotto :as baleen-lagotto])
  (:require [clojure.data.json :as json]
            [clojure.tools.logging :as l]
            [clojure.set :refer [difference]])
  (:require [clj-time.coerce :as clj-time-coerce]))

(defn process-f [context json-blob]
  (let [item-parsed (json/read-str json-blob)
        input (get item-parsed "input")
        additions (get-in item-parsed ["events" "added"])
        removals (get-in item-parsed ["events" "removed"])

        timestamp (str (clj-time-coerce/from-long
                        (* 1000 (get-in item-parsed ["input" "timestamp"]))))

        title (get-in item-parsed ["input" "title"])
        author-name (when-let [author-name (get-in item-parsed ["input" "user"])]
                      (str "https://meta.wikimedia.org/wiki/User:" author-name))

        url (get-in item-parsed ["meta" "url"])
          
        add-results (doall (map (fn [{doi "doi" event-id "event-id"}]
                                  (baleen-lagotto/send-deposit
                                    context
                                    :subj-title title
                                    :subj-url url
                                    :subj-work-type "entry-encyclopedia"
                                    :subj-author author-name
                                    :obj-doi doi
                                    :action "add"
                                    :event-id event-id
                                    :date-str timestamp
                                    :source-id "wikipedia"
                                    :relation-type "references")) additions))

        remove-results (doall (map (fn [{doi "doi" event-id "event-id"}]
                                (baleen-lagotto/send-deposit
                                      context
                                      :subj-title title
                                      :subj-url url
                                      :subj-author author-name
                                      :subj-work-type "entry-encyclopedia"
                                      :obj-doi doi
                                      :action "remove"
                                      :event-id event-id
                                      :date-str timestamp
                                      :source-id "wikipedia"
                                      :relation-type "references")) additions))]
      ; Return success.
      (and (every? true? add-results)
           (every? true? remove-results))))

(defn run
  "Run processing. Blocks forever."
  [context]
  (baleen-queue/process-queue context "matched" (partial process-f context) :keep-done true))

