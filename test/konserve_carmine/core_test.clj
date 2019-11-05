(ns konserve-carmine.core-test
  (:require [clojure.test :refer :all]
            [konserve.core :as k]
            [konserve-carmine.core :refer :all]
            [clojure.core.async :refer [<!!]]))

(deftest carmine-store-test
  (testing "Test the carmine store functionality."
    (let [store (<!! (new-carmine-store))]
      (<!! (k/assoc-in store [:foo] nil))
      (is (= (<!! (k/get-in store [:foo]))
             nil))
      (<!! (k/assoc-in store [:foo] :bar))
      (is (= (<!! (k/get-in store [:foo]))
             :bar))
      (<!! (k/assoc-in store [:foo] {:bar 0}))
      (<!! (k/update-in store [:foo :bar] inc))
      (is (= (<!! (k/get-in store [:foo :bar]))
             1))
      (<!! (k/dissoc store :foo))
      (is (= (<!! (k/get-in store [:foo]))
             nil))
      (<!! (k/bassoc store :binbar (byte-array (range 10))))
      (<!! (k/bget store :binbar (fn [{:keys [input-stream]}]
                                 (is (= (map byte (slurp input-stream))
                                        (range 10)))))))))

(comment
  (run-tests))
