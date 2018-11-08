(ns metabase.driver.generic-sql.connection-test
  (:require [expectations :refer :all]
            [metabase.driver.util :as driver.u]
            [metabase.test.data :refer :all]
            [metabase.test.util.log :as tu.log]))

;; ## TESTS FOR CAN-CONNECT?

;; Check that we can connect to the Test DB
(expect
  true
  (driver.u/can-connect-with-details? :h2 (:details (db))))

;; Lie and say Test DB is Postgres. CAN-CONNECT? should fail
(expect
  false
  (tu.log/suppress-output
    (driver.u/can-connect-with-details? :postgres (:details (db)))))

;; Random made-up DBs should fail
(expect
  false
  (tu.log/suppress-output
    (driver.u/can-connect-with-details? :postgres {:host "localhost", :port 5432, :dbname "ABCDEFGHIJKLMNOP", :user "rasta"})))

;; Things that you can connect to, but are not DBs, should fail
(expect
  false
  (tu.log/suppress-output
    (driver.u/can-connect-with-details? :postgres {:host "google.com", :port 80})))
