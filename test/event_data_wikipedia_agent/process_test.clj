(ns event-data-wikipedia-agent.process-test
  (:require [clojure.test :refer :all]
            [event-data-wikipedia-agent.process :as process]))

(deftest build-urls
  (testing "RESTBase URLs constructed correctly."
    (is (= "https://en.wikipedia.com/api/rest_v1/page/html/Fish/1234"
            (process/build-restbase-url "en.wikipedia.com" "Fish" 1234)))

    (is (= "https://en.wikipedia.com/api/rest_v1/page/html/Fish_and_Chips/1234"
            (process/build-restbase-url "en.wikipedia.com" "Fish and Chips" 1234))))

  (testing "Wikipedia URLs constructed correctly."
    (is (= "https://en.wikipedia.com/w/index.php?title=Fish"
           (process/build-wikipedia-url "en.wikipedia.com" "Fish")))))
