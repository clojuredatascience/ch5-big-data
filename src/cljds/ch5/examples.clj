(ns cljds.ch5.examples
  (:require [abracad.avro :as avro]
            [cljds.ch5.data :refer :all]
            [cljds.ch5.hadoop :refer [hadoop-gradient-descent]]
            [cljds.ch5.tesser :refer :all]
            [cljds.ch5.parkour :refer [hadoop-sgd hadoop-extract-features]]
            [clojure.core.reducers :as r]
            [cljds.ch5.util :refer :all]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [incanter.charts :as c]
            [incanter.core :as i]
            [iota]
            [parkour.conf :as conf]
            [parkour.io.text :as text]
            [tesser.core :as t]
            [tesser.hadoop :as h]
            [tesser.math :as m]
            [incanter.svg :as svg]))

(defn ex-5-1 []
  (-> (slurp "data/soi.csv")
      (str/split #"\n")
      (first)))

(defn ex-5-2 []
  (-> (io/reader "data/soi.csv")
      (line-seq)
      (first)))

(defn ex-5-3 []
  (-> (io/reader "data/soi.csv")
      (line-seq)
      (count)))

(defn ex-5-4 []
  (->> (io/reader "data/soi.csv")
       (line-seq)
       (reduce (fn [i x]
                 (inc i)) 0)))

(defn ex-5-5 []
  (->> (io/reader "data/soi.csv")
       (line-seq)
       (r/fold + (fn [i x]
                   (inc i)))))

(defn ex-5-6 []
  (+))

(defn ex-5-7 []
  (->> (iota/seq "data/soi.csv")
       (r/fold + (fn [i x]
                   (inc i)))))

(defn ex-5-8 []
   (->> (iota/seq "data/soi.csv")
        (r/drop 1)
        (r/map parse-line)
        (r/take 1)
        (into [])))

(defn ex-5-9 []
  (let [data (iota/seq "data/soi.csv")
        column-names (parse-columns (first data))]
    (->> (r/drop 1 data)
         (r/map parse-line)
         (r/map (fn [fields]
                  (zipmap column-names fields)))
         (r/take 1)
         (into []))))

(defn ex-5-10 []
  (let [data (iota/seq "data/soi.csv")
        column-names (parse-columns (first data))]
    (->> (r/drop 1 data)
         (r/map parse-line)
         (r/map (fn [fields]
                  (zipmap column-names fields)))
         (r/remove (fn [record]
                     (zero? (:zipcode record))))
         (r/take 1)
         (into []))))

(defn ex-5-11 []
  (let [data (load-data "data/soi.csv")
        xs (into [] (r/map :N1 data))]
    (/ (reduce + xs)
       (count xs))))

(defn mean
  ([] 0)
  ([x y] (/ (+ x y) 2)))

(defn ex-5-12 []
  (->> (load-data "data/soi.csv")
       (r/map :N1)
       (r/fold mean)))

(defn mean-combiner
  ([] {:count 0 :sum 0})
  ([a b] (merge-with + a b)))

(defn mean-reducer [acc x]
  (-> acc
      (update-in [:count] inc)
      (update-in [:sum] + x)))

(defn ex-5-13 []
  (->> (load-data "data/soi.csv")
       (r/map :N1)
       (r/fold mean-combiner
               mean-reducer)))

(defn mean-post-combiner [{:keys [count sum]}]
  (if (zero? count) 0 (/ sum count)))

(defn ex-5-14 []
  (->> (load-data "data/soi.csv")
       (r/map :N1)
       (r/fold mean-combiner
               mean-reducer)
       (mean-post-combiner)))

(defn ex-5-15 []
   (let [data (->> (load-data "data/soi.csv")
                   (r/map :N1))
         mean-x (->> data
                     (r/fold mean-combiner
                             mean-reducer)
                     (mean-post-combiner))
         sq-diff (fn [x] (i/pow (- x mean-x) 2))]
     (->> data
          (r/map sq-diff)
          (r/fold mean-combiner
                  mean-reducer)
          (mean-post-combiner))))

(defn variance-combiner
  ([] {:count 0 :mean 0 :sum-of-squares 0})
  ([a b]
   (let [count (+ (:count a) (:count b))]
     {:count count
      :mean (/ (+ (* (:count a) (:mean a))
                  (* (:count b) (:mean b))) count)
      :sum-of-squares (+ (:sum-of-squares a)
                         (:sum-of-squares b)
                         (/ (* (- (:mean b)
                                  (:mean a))
                               (- (:mean b)
                                  (:mean a))
                               (:count a)
                               (:count b))
                            count))})))

(defn variance-reducer [{:keys [count mean sum-of-squares]} x]
  (let [count' (inc count)
        mean'  (+ mean (/ (- x mean) count'))]
    {:count count'
     :mean mean'
     :sum-of-squares (+ sum-of-squares
                        (* (- x mean') (- x mean)))}))

(defn variance-post-combiner [{:keys [count mean sum-of-squares]}]
   (if (zero? count) 0 (/ sum-of-squares count)))

(defn ex-5-16 []
  (->> (load-data "data/soi.csv")
       (r/map :N1)
       (r/fold variance-combiner
               variance-reducer)
       (variance-post-combiner)))

(defn sd-post-combiner [result]
  (i/sqrt (variance-post-combiner result)))

(defn ex-5-17 []
  (let [data (into [] (load-data "data/soi.csv"))]
    (->> (m/covariance :A02300 :A00200)
         (t/tesser (t/chunk 512 data )))))

(defn ex-5-18 []
  (let [data (iota/seq "data/soi.csv")]
    (->> (prepare-data)
         (m/covariance :A02300 :A00200)
         (t/tesser (chunks data)))))

(defn ex-5-19 []
  (let [data (iota/seq "data/soi.csv")]
    (->> (prepare-data)
         (m/correlation :A02300 :A00200)
         (t/tesser (chunks data)))))

(defn ex-5-20 []
  (let [data (iota/seq "data/soi.csv")]
    (->> (prepare-data)
         (t/map :A00200)
         (t/fuse {:A00200-mean (m/mean)
                  :A00200-sd   (m/standard-deviation)})
         (t/tesser (chunks data)))))

(defn ex-5-21 []
  (let [data (iota/seq "data/soi.csv")]
    (->> (prepare-data)
         (t/map #(select-keys % [:A00200 :A02300]))
         (t/facet)
         (m/mean)
         (t/tesser (chunks data)))))

(defn ex-5-22 []
  (let [data (iota/seq "data/soi.csv")
        fx :A00200
        fy :A02300]
    (->> (prepare-data)
         (t/fuse {:covariance (m/covariance fx fy)
                  :variance-x (m/variance (t/map fx))
                  :mean-x (m/mean (t/map fx))
                  :mean-y (m/mean (t/map fx))})
         (t/post-combine calculate-coefficients)
         (t/tesser (chunks data)))))

(defn ex-5-23 []
  (let [data (iota/seq "data/soi.csv")
        attributes {:unemployment-compensation :A02300
                    :salary-amount             :A00200
                    :gross-income              :AGI_STUB
                    :joint-submissions         :MARS2
                    :dependents                :NUMDEP}]
    (->> (prepare-data)
         (m/correlation-matrix attributes)
         (t/tesser (chunks data)))))

(defn ex-5-24 []
  (let [data (iota/seq "data/soi.csv")
        features [:A02300 :A00200 :AGI_STUB :NUMDEP :MARS2]]
    (->> (feature-scales features)
         (t/tesser (chunks data)))))

(defn ex-5-25 []
  (let [data     (iota/seq "data/soi.csv")
        features [:A02300 :A00200 :AGI_STUB :NUMDEP :MARS2]
        factors (->> (feature-scales features)
                     (t/tesser (chunks data)))]
    (->> (load-data "data/soi.csv")
         (r/map #(select-keys % features ))
         (r/map (scale-features factors))
         (into [])
         (first))))

(defn ex-5-26 []
  (let [data     (iota/seq "data/soi.csv")
        features [:A02300 :A00200 :AGI_STUB :NUMDEP :MARS2]
        factors (->> (feature-scales features)
                     (t/tesser (chunks data)))]
    (->> (load-data "data/soi.csv")
         (r/map (scale-features factors))
         (r/map (extract-features :A02300 features))
         (into [])
         (first))))

(defn ex-5-27 []
   (let [columns [:A02300 :A00200 :AGI_STUB :NUMDEP :MARS2]
         data    (iota/seq "data/soi.csv")]
     (->> (prepare-data)
          (t/map (extract-features :A02300 columns))
          (t/map :xs)
          (t/fold (matrix-sum (inc (count columns)) 1))
          (t/tesser (chunks data)))))

(defn ex-5-28 []
  (let [columns [:A02300 :A00200 :AGI_STUB :NUMDEP :MARS2]
        fcount  (inc (count columns))
        coefs   (vec (replicate fcount 0))
        data    (iota/seq "data/soi.csv")]
    (->> (prepare-data)
         (t/map (extract-features :A02300 columns))
         (t/map (calculate-error (i/trans coefs)))
         (t/fold (matrix-sum fcount 1))
         (t/tesser (chunks data)))))

(defn ex-5-29 []
  (let [columns [:A02300 :A00200 :AGI_STUB :NUMDEP :MARS2]
        fcount  (inc (count columns))
        coefs   (vec (replicate fcount 0))
        data    (iota/seq "data/soi.csv")]
    (->> (prepare-data)
         (t/map (extract-features :A02300 columns))
         (t/map (calculate-error (i/trans coefs)))
         (t/fuse {:sum   (t/fold (matrix-sum fcount 1))
                  :count (t/count)})
         (t/post-combine (fn [{:keys [sum count]}]
                           (i/div sum count)))
         (t/tesser (chunks data)))))

(defn ex-5-30 []
  (let [features [:A02300 :A00200 :AGI_STUB :NUMDEP :MARS2]
        fcount   (inc (count features))
        coefs    (vec (replicate fcount 0))
        data     (iota/seq "data/soi.csv")]
    (->> (prepare-data)
         (t/map (extract-features :A02300 features))
         (t/map (calculate-error (i/trans coefs)))
         (t/fold (matrix-mean fcount 1))
         (t/tesser (chunks data)))))

(defn ex-5-31 []
  (let [features [:A00200 :AGI_STUB :NUMDEP :MARS2]
        fcount   (inc (count features))
        coefs    (vec (replicate fcount 0))
        data     (chunks (iota/seq "data/soi.csv"))
        factors  (->> (feature-scales features)
                      (t/tesser data))
        options {:fy :A02300 :features features
                 :factors factors :coefs coefs :alpha 0.1}]
    (->> (gradient-descent-fold options)
         (t/tesser data))))

(defn descend [options data]
  (fn [coefs]
    (->> (gradient-descent-fold (assoc options :coefs coefs))
         (t/tesser data))))

(defn ex-5-32 []
  (let [features [:A00200 :AGI_STUB :NUMDEP :MARS2]
        fcount   (inc (count features))
        coefs    (vec (replicate fcount 0))
        data     (chunks (iota/seq "data/soi-sample.csv"))
        factors  (->> (feature-scales features)
                      (t/tesser data))
        options  {:fy :A02300 :features features
                  :factors factors :coefs coefs :alpha 0.1}
        iterations 100
        xs (range iterations)
        ys (->> (iterate (descend options data) coefs)
                (take iterations))]
    (-> (c/xy-plot xs (map first ys)
                   :x-label "Iterations"
                   :y-label "Coefficient")
        (c/add-lines xs (map second ys))
        (c/add-lines xs (map #(nth % 2) ys))
        (c/add-lines xs (map #(nth % 3) ys))
        (c/add-lines xs (map #(nth % 4) ys))
        (i/view))))

(defn ex-5-33 []
  (->> (text/dseq "data/soi.csv")
       (r/take 2)
       (into [])))

(defn ex-5-34 []
  (let [conf     (conf/ig)
        input    (text/dseq "data/soi.csv")
        workdir  (rand-file "tmp")
        features [:A00200 :AGI_STUB :NUMDEP :MARS2]]
    (h/fold conf input workdir #'feature-scales features)))

(defn ex-5-35 []
  (let [workdir  "tmp"
        out-file (rand-file workdir)]
    (hadoop-gradient-descent (conf/ig) "data/soi.csv" workdir)))

(defn ex-5-36 []
  (let [features [:A00200 :AGI_STUB :NUMDEP :MARS2]
        fcount   (inc (count features))
        coefs    (vec (replicate fcount 0))
        data     (chunks (iota/seq "data/soi.csv"))
        factors  (->> (feature-scales features)
                      (t/tesser data))
        options  {:fy :A02300 :features features
                  :factors factors :coefs coefs :alpha 1e-3}
        ys       (stochastic-gradient-descent options data)
        xs       (range (count ys))]
    (-> (c/xy-plot xs (map first ys)
                   :x-label "Iterations"
                   :y-label "Coefficient")
        (c/add-lines xs (map #(nth % 1) ys))
        (c/add-lines xs (map #(nth % 2) ys))
        (c/add-lines xs (map #(nth % 3) ys))
        (c/add-lines xs (map #(nth % 4) ys))
        (i/view))))

(defn ex-5-37 []
  (let [workdir  "tmp"
        out-file (rand-file workdir)]
    (hadoop-extract-features (conf/ig) "tmp"
                             "data/soi.csv" out-file)
    (str out-file)))

(defn ex-5-38 []
  (let [workdir  "tmp"
        out-file (rand-file workdir)]
    (hadoop-sgd (conf/ig) "tmp" "data/soi.csv" out-file)
    (str out-file)))
