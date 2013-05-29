(ns korma.db
  "Functions for creating and managing database specifications."
  (comment (:require [clojure.java.jdbc :as jdbc]
                     [clojure.java.jdbc.internal :as ijdbc]
                     [korma.config :as conf])
           (:import com.mchange.v2.c3p0.ComboPooledDataSource))
  (:require [korma.config :as conf]
            [clojure.string :as str])
  (:import [System.Data IDbCommand]))

(defonce _default (atom nil))

(defn ->strategy [{:keys [keys fields]}]
  {:keyword keys
   :identifier fields})

(def ^:dynamic *db* {:connection nil :level 0})

(def ^:dynamic *transaction* nil)

(defn get-connection [] (:connection *db*))

(defn default-connection
    "Set the database connection that Korma should use by default when no 
  alternative is specified."
    [conn]
    (conf/merge-defaults (:options conn))
    (reset! _default conn))

(comment
  (defmacro transaction
    "Execute all queries within the body in a single transaction."
    [& body]
    `(jdbc/with-connection (get-connection @_default)
       (jdbc/transaction
        ~@body)))

  (defn rollback
    "Tell this current transaction to rollback."
    []
    (jdbc/set-rollback-only))

  (defn is-rollback?
    "Returns true if the current transaction will be rolled back"
    []
    (jdbc/is-rollback-only))


  (defn- exec-sql [query]
    (let [results? (:results query)
          sql (:sql-str query)
          params (:params query)]
      (try
        (condp = results?
          :results (jdbc/with-query-results rs (apply vector sql params)
        a            (vec rs))
          :keys (ijdbc/do-prepared-return-keys* sql params)
          (jdbc/do-prepared sql params))
        (catch Exception e (handle-exception e sql params)))))

  (defn do-query [query]
    (let [conn (when-let[db (:db query)]
                 (get-connection db))
          cur (or conn (get-connection @_default))
          prev-conn (jdbc/find-connection)
          opts (or (:options query) @conf/options)]
      (jdbc/with-naming-strategy (->strategy (:naming opts))
        (if-not prev-conn
          (jdbc/with-connection cur
            (exec-sql query))
          (exec-sql query))))))

(declare exec-scalar)

(defn default-convert-value [v#]
  (cond
   (instance? System.DBNull v#)
   nil

   :else
   v#))

(defn- mysql-get-last-insert-id [rows-affected]
  (let [last-insert-id (exec-scalar "SELECT LAST_INSERT_ID()" [])]
    (for [i (range rows-affected)] {:id (when (= 0 i) last-insert-id)})))

(defn- mysql-subprotocol []
  {:get-last-insert-id
   mysql-get-last-insert-id
   :convert-value
   (eval `(fn mysql-convert-value [v#]
            (cond
             (instance? MySql.Data.Types.MySqlDateTime v#)
             (when (.IsValidDateTime v#)
               (.GetDateTime v#))

             (instance? System.DBNull v#)
             nil

             :else
             v#)))})

(defn- extract-subprotocol [{:keys [subprotocol]}]
  (case subprotocol
    "mysql" (mysql-subprotocol)
    nil))

(defn create-db
    "Create a db connection object manually instead of using defdb. This is often useful for
  creating connections dynamically, and probably should be followed up with:

  (default-connection my-new-conn)"
    [spec]
    {:spec spec
     :subprotocol (extract-subprotocol spec)
     :options (conf/extract-options spec)})

(defmacro defdb
    "Define a database specification. The last evaluated defdb will be used by default
  for all queries where no database is specified by the entity."
    [db-name spec]
    `(let [spec# ~spec]
       (defonce ~db-name (create-db spec#))
       (default-connection ~db-name)))

(defn load-type [typename]
  (let [asms (seq (.GetAssemblies AppDomain/CurrentDomain))]
    (loop [asm (first asms)
           more (rest asms)]
      (if asm
        (if-let [type (.GetType asm typename)]
          type
          (recur (first more) (rest more)))
          nil))))

(defn create-connection [db]
  (let [{:keys [assembly-name classname connection-string]} (:spec db)]
    (when assembly-name
      (try (assembly-load assembly-name)
           (catch Object ex)))
    (let [cls (load-type classname)
          conn (if cls
                 (Activator/CreateInstance cls)
                 (throw (Exception. (str "Unable to load database class " classname))))]
      (if conn
        (do
          (.set_ConnectionString conn connection-string)
          conn)
        (throw (Exception. (str "Unable to connect to database of type " cls)))))))

(defn- make-default-connection-string [opts]
  (let [opts (dissoc opts :naming :subprotocol :delimiters)]
    (str/join ";" (map #(str (name (first %)) "=" (second %)) opts))))

(defn mysql
    "Create a database specification for a mysql database. Opts should include keys
  for :Database, :Uid, and :Pwd. You can also optionally set :Host and :Port.
  Delimiters are automatically set to \"`\"."
    [opts]
    (merge {:assembly-name "Mysql.Data"
            :classname "MySql.Data.MySqlClient.MySqlConnection" 
            :subprotocol "mysql"
            :delimiters "`"
            :connection-string (make-default-connection-string opts)}
           opts))

(defn postgres
  "Create a database specification for a postgres database. Opts should include keys
  for \"Database\", \"User Id\", and \"Password\". You can also optionally set \"Server\" and \"Port\"."
  [opts]
  (merge {:assembly-name "Npgsql"
          :classname "Npgsql.NpgsqlConnection" 
          :subprotocol "sqlite3"
          :connection-string (make-default-connection-string opts)}
           opts))

(defn sqlite3
  "Create a database specification for a SQLite3 database. Opts should include a key
  for \"Data Source\" which is the path to the database file."
  [opts]
  (merge {:assembly-name "System.Data.SQLite"
          :classname "System.Data.SQLite.SQLiteConnection" 
          :subprotocol "sqlite3"
          :connection-string (make-default-connection-string opts)}
           opts))

(defn with-connection* [db func]
  (if-let [conn (korma.db/create-connection db)]
    (binding [*db* (assoc *db* :connection conn :level 0 :rollback (atom false) :db db)]
     (try (.Open conn)
          (func)
          (finally (.Close conn))))
    (throw (Exception. (str "Unable to connect to database " db)))))

(defmacro with-connection [db & body]
  `(korma.db/with-connection* ~db (fn [] ~@body)))

(defn rollback "Tell this current transaction to rollback." []
  (reset! (:rollback *db*) true))

(defn is-rollback? "Returns true if the current transaction will be rolled back" []
  (deref (:rollback *db*)))

(defn transaction* [func]
  (binding [*db* (update-in *db* [:level] inc)]
    (if (= (:level *db*) 1)
      (let [transaction (.BeginTransaction (get-connection))]
        (try
          (let [res (func)]
            (if (is-rollback?)
              (do (.Rollback transaction) nil)
              (do (.Commit transaction) res)))
          (catch Exception e
            (.Rollback transaction)
            (throw e))))
      (func))))

(defmacro transaction
  "Execute all queries within the body in a single transaction."
  [& body]
  `(with-connection @_default
     (transaction* (fn [] ~@body))))


(defn set-params [command params]
  (doall (map-indexed
          (fn [idx param] (.AddWithValue (.get_Parameters command) (str "@p" idx) param)) params)))

(defn create-command [conn sql params]
  (let [cmd (.CreateCommand conn)]
    (.set_CommandText cmd sql)
    (set-params cmd params)
    cmd))

(defn- read-result [reader convert-value]
  (let [n (.FieldCount reader)]
    (apply hash-map (mapcat (fn [i] [(keyword (.GetName reader i)) (convert-value (.GetValue reader i))]) (range n)))))

(defn- read-results [reader {:keys [convert-value] :or {convert-value default-convert-value}}]
  (try (loop [res []]
         (if (.Read reader)
           (recur (conj res (read-result reader convert-value)))
           res))
       (finally (.Close reader))))

(defn exec-scalar
  ([conn sql params]
      (let [cmd (create-command conn sql params)]
        (.ExecuteScalar cmd)))
  ([sql params] (exec-scalar (get-connection) sql params)))

(defn exec-non-query
  ([conn sql params]
      (let [cmd (create-command conn sql params)]
        (.ExecuteNonQuery cmd)))
  ([sql params] (exec-non-query (get-connection) sql params)))

(defn exec-reader-query
  ([conn sql params subprotocol]
      (let [cmd (create-command conn sql params)]
        (read-results (.ExecuteReader cmd) subprotocol)))
  ([sql params subprotocol] (exec-reader-query (get-connection) sql params subprotocol)))

(defn- not-impl []
  (println "Not implemented yet"))

(defn handle-exception [e sql params]
    (println "Failure to execute query with SQL:")
    (println sql " :: " params)
    (comment (cond
              (instance? java.sql.SQLException e) (jdbc/print-sql-exception e)
              :else (.printStackTrace e)))
    (throw e))

(defn- get-last-insert-id [rows-affected]
  (when-let [get-last-insert-id-fn (get-in *db* [:db :subprotocol :get-last-insert-id])]
    (get-last-insert-id-fn rows-affected)))

(defn- exec-sql [query]
    (let [results? (:results query)
          sql (:sql-str query)
          params (:params query)
          subprotocol (get-in query [:db :subprotocol])]
      (try
        (condp = results?
          :results (exec-reader-query sql params subprotocol)
          :keys (let [rows-affected (exec-non-query sql params)]
                  (if-let [last-insert-id (get-last-insert-id rows-affected)]
                    last-insert-id
                    {:rows-affected rows-affected}))
          (not-impl))
        (catch Exception e (handle-exception e sql params)))))


(defn do-query [query]
  (let [db (:db query)
        db (or db @_default)
        prev-conn (get-connection)
        opts (or (:options query) @conf/options)]
    (if-not prev-conn
      (with-connection db
        (exec-sql query))
      (exec-sql query))))
