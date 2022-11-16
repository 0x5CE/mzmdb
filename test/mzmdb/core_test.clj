(ns mzmdb.core-test
  (:require [clojure.test :refer :all]
            [mzmdb.core :refer :all]))

(deftest validate-query-test
  (testing "Validation test"
    (is (validate-query {:where [:= :id 1]}))
    (is (not (validate-query {:where []})))
    (is (not (validate-query {:wheree [:= :id 1 5]})))))

(deftest create-table-test
  (testing "db create-table test"
    (is (= :employee 
      (create-table :employee 
        {:id :number :first-name :string :last-name :string :salary :number})))
    (is (= nil (create-table :employee nil)))))

(deftest insert-test
  (testing "db insert test"
    (create-table :employee-2
      {:id :number :first-name :string :last-name :string :salary :number})
    (is (= 4 (count (insert :employee-2
      {:id 1 :first-name "Alan" :last-name "Kay" :salary 105000.0}))))
    (is (not (= 4 (count (insert :employee-3 nil)))))))

(deftest select-test
  (testing "db insert test"
    (create-table :employee-3
      {:id :number :first-name :string :last-name :string :salary :number})
    (insert :employee-3
      {:id 1 :first-name "Alan" :last-name "Kay" :salary 105000.0})
    (insert :employee-3
      {:id 2 :first-name "Mike" :last-name "X" :salary 50000.0})
    (is (= 2 (count (select :employee-3 {:where [:>= :salary 10000]}))))
    (is (= 0 (count (select :employee-3 {:where [:= :salary 10000]}))))
    (is (= nil (select :employee-3 {:where [:= :salary ""]})))))

(deftest delete-test
  (testing "db delete test"
    (create-table :employee-4
      {:id :number :first-name :string :last-name :string :salary :number})
    (insert :employee-4
      {:id 1 :first-name "Alan" :last-name "Kay" :salary 105000.0})
    (insert :employee-4
      {:id 2 :first-name "Mike" :last-name "X" :salary 50000.0})
    (is (= true (delete :employee-4 {:where [:= :id 1]})))
    (is (= nil (delete :employee-4 {:where [:= :salaryy 100]})))
    (is (= nil (delete :employee-4 {:where [:= :id ""]})))))