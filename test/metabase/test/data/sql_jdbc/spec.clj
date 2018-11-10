(ns metabase.test.data.sql-jdbc.spec
  (:require [metabase.driver :as driver]
            [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
            [metabase.test.data.interface :as tx]))

(defmulti database->spec
  "Return a JDBC spec that should be used to connect to `dbdef`. Uses `jdbc-sql.conn/connection-details->spec` by
  default."
  {:arglists '([driver contxt dbdef])}
  tx/dispatch-on-driver-with-test-extensions
  :hierarchy #'driver/hierarchy)

(defmethod database->spec :sql-jdbc/test-extensions [driver context dbdef]
  (sql-jdbc.conn/connection-details->spec driver (tx/database->connection-details driver context dbdef)))
