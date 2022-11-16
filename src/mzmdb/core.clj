(ns mzmdb.core
  (:gen-class))

(def tables (atom {})) 
(def table-schemas (atom {}))
(def table-column-positions (atom {}))

;; utility function to check if a query is rightly formatted
;; on success: returns true
;; on failure: returns false
(defn validate-query [query]
  (if (and
    (not= nil (:where query))
    (= 3 (count (:where query)))
    (or
      (= := (nth (:where query) 0))
      (= :< (nth (:where query) 0))
      (= :<= (nth (:where query) 0))
      (= :> (nth (:where query) 0))
      (= :>= (nth (:where query) 0))))
    true
    false))

;; create new table with the given schema 
;; on success: returns table name
(defn create-table [table-name table-schema]
  (if (= nil (get @table-schemas table-name))
    (do
      (reset! table-schemas 
        (assoc @table-schemas table-name 
          (zipmap 
            (keys table-schema) 
            (doall (map-indexed vector (vals table-schema))))))
      (reset! tables 
        (assoc @tables table-name []))
      table-name)
    (println "create-table: already exists"))) 

;; insert a record into an existing table
;; on success: returns the newly inserted record
(defn insert [table-name record]
  (if-let [schema (get @table-schemas table-name)]
    (loop [[column & t] record] 
      (if-let [column-type (second (get schema (key column)))]
        (if (case column-type
              :number (number? (val column))
              :string (string? (val column))
              :else false)
          (if t 
            (recur t)
            (do
              (reset! tables 
                (assoc @tables 
                  table-name
                  (conj (get @tables table-name) (vec (vals record)))))
              (vec (vals record))))
          (println (str "insert: wrong data type for " (key column))))
        (println (str "insert: column " (key column) " not found"))))
    (println "insert: table not found")))

;; query the database to return table records with given constraints
;; on success: returns the vector of records
(defn select [table-name query]
  (if-let [schema (get @table-schemas table-name)]
    (if (validate-query query)
      (let [query (:where query)
            op (first query)
            col (second query)
            val (last query)
            table (get @tables table-name)]
        (if-let [sch (get schema col)]
          (if (case (last sch)
                :number (number? val)
                :string (string? val)
                :else false)
            (vec (filter 
              (fn [record]
                (let [rec-val (nth record (first sch))]
                  (case op
                    := (= rec-val val)
                    :> (> rec-val val)
                    :>= (>= rec-val val)
                    :< (< rec-val val)
                    :<= (<= rec-val val)
                    :else false))) table))
            (println (str "select: wrong datatype for " col)))
          (println (str "select: column " col " not found"))))
      (println "select: invalid query"))
    (println "select: table not found")))


;; delete record(s) from a table with given constraints
;; on success: returns true
(defn delete [table-name query]
  (if-let [schema (get @table-schemas table-name)]
    (if (validate-query query)
      (let [query (:where query)
            op (first query)
            col (second query)
            val (last query)
            table (get @tables table-name)]
        (if-let [sch (get schema col)]
          (if (case (last sch)
                :number (number? val)
                :string (string? val)
                :else false)
            (let [new-table-records 
              (vec (filter 
                (fn [record]
                  (let [rec-val (nth record (first sch))]
                    (case op
                      := (not= rec-val val)
                      :> (<= rec-val val)
                      :>= (< rec-val val)
                      :< (>= rec-val val)
                      :<= (> rec-val val)
                      :else true))) table))]
              (reset! tables 
                (assoc @tables 
                  table-name
                  new-table-records))
              true)
            (println (str "delete: wrong datatype for " col)))
          (println (str "delete: column " col " not found"))))
      (println "delete: invalid query"))
    (println "delete: table not found")))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))