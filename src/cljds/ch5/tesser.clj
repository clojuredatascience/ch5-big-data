(ns cljds.ch5.tesser
  (:require [iota :as iota]
            [incanter.core :as i]
            [tesser.math :as m]
            [tesser.core :as t]
            [clojure.java.io :as io]
            [clojure.string :refer [blank? split]]
            [cljds.ch5.data :refer :all]
            [incanter.core :as c]
            [incanter.stats :as s]))

(defn variance [fx coll]
  (->> (t/map fx)
       (m/variance)
       (t/tesser (chunks coll))))

(defn covariance [fx fy coll]
  (->> (m/covariance fx fy)
       (t/tesser (chunks coll))))

(defn standard-deviation [fx coll]
  (->> (t/map fx)
       (m/standard-deviation)
       (t/tesser (chunks coll))))

(defn calculate-coefficients [{:keys [covariance variance-x
                                      mean-x mean-y]}]
  (let [slope     (/ covariance variance-x)
        intercept (- mean-y (* mean-x slope))]
    [intercept slope]))

(defn linear-regression [fx fy coll]
  (->> (t/fuse {:covariance (m/covariance fx fy)
                :variance-x (m/variance (t/map fx))
                :mean-x (m/mean (t/map fx))
                :mean-y (m/mean (t/map fx))})
       (t/post-combine calculate-coefficients)
       (t/tesser (chunks coll))))

;; Gradient Descent

(defn feature-scales [features]
  (->> (prepare-data)
       (t/map #(select-keys % features))
       (t/facet)
       (t/fuse {:mean (m/mean)
                :sd   (m/standard-deviation)})))

(defn feature-scales-fold [{:keys [column-names features]}]
  (->> (prepare-data column-names)
       (t/map #(select-keys % features))
       (t/facet)
       (t/fuse {:mean (m/mean)
                :sd   (m/standard-deviation)})))

(defn feature-means [features fold]
  (->> fold
       (t/map #(select-keys % features))
       (t/facet)
       (m/mean)))

(defn matrix-sum [nrows ncols]
  (let [zeros-matrix (i/matrix 0 nrows ncols)]
    {:reducer-identity (constantly zeros-matrix)
     :reducer i/plus
     :combiner-identity (constantly zeros-matrix)
     :combiner i/plus}))

(defn matrix-mean [nrows ncols]
  (let [zeros-matrix (i/matrix 0 nrows ncols)]
    {:reducer-identity  (constantly [zeros-matrix 0])
     :reducer           (fn [[sum counter] x]
                          [(i/plus sum x) (inc counter)])
     :combiner-identity (constantly [zeros-matrix 0])
     :combiner          (fn [[sum-a count-a] [sum-b count-b]]
                          [(i/plus sum-a sum-b)
                           (+ count-a count-b)])
     :post-combiner     (fn [[sum count]]
                          (i/div sum count))}))

#_(defn gradient-descent-fold [fy columns coefs alpha factors]
  (let [zeros-matrix (i/matrix 0 (count columns) 1)]
    (->> (t/map (scale-features factors))
         (t/map (extract-features fy columns))
         (t/map (calculate-error coefs))
         (t/fold (matrix-mean (inc (count columns)) 1))
         (t/post-combine (update-coefficients coefs alpha)))))

(defn gradient-descent-hadoop-fold [{:keys [column-names fy features
                                            coefs alpha factors]}]
  (let [zeros-matrix (i/matrix 0 (count features) 1)
        coefs (i/matrix coefs)]
    (->> (prepare-data column-names)
         (t/map (scale-features factors))
         (t/map (extract-features fy features))
         (t/map (calculate-error coefs))
         (t/fold (matrix-mean (inc (count features)) 1))
         (t/post-combine (update-coefficients coefs alpha)))))


(defn gradient-descent-fold [{:keys [fy features factors
                                     coefs alpha]}]
  (let [zeros-matrix (i/matrix 0 (count features) 1)]
    (->> (prepare-data)
         (t/map (scale-features factors))
         (t/map (extract-features fy features))
         (t/map (calculate-error (i/trans coefs)))
         (t/fold (matrix-mean (inc (count features)) 1))
         (t/post-combine (update-coefficients coefs alpha)))))

(defn stochastic-gradient-descent [options data]
  (let [batches (->> (into [] data)
                     (shuffle)
                     (partition 250))
        descend (fn [coefs batch]
                  (->> (gradient-descent-fold
                        (assoc options :coefs coefs))
                       (t/tesser (chunks batch))))]
    (reductions descend (:coefs options) batches)))
