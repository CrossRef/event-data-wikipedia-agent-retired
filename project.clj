(defproject event-data-wikipedia-agent "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
           [org.clojure/tools.logging "0.3.1"]
           [org.slf4j/slf4j-simple "1.7.21"]
                  [org.clojure/data.json "0.2.6"]
                  [gottox/socketio "0.1"]
                  [crossref/baleen "0.1"]]
  :plugins [[lein-localrepo "0.5.3"]]
  :java-source-paths ["src-java"]
  :main ^:skip-aot event-data-wikipedia-agent.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}
             :prod {:resource-paths ["config/prod"]}
             :dev  {:resource-paths ["config/dev"]}})
