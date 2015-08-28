(ns cljds.ch5.data
  (:require [clojure.core.reducers :as r]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [incanter.core :as i]
            [tesser.core :as t]
            [iota]))

(def column-names
  [:STATEFIPS :STATE :zipcode :AGI_STUB :N1 :MARS1 :MARS2 :MARS4 :PREP :N2 :NUMDEP :A00100 :N00200 :A00200 :N00300 :A00300 :N00600 :A00600 :N00650 :A00650 :N00900 :A00900 :SCHF :N01000 :A01000 :N01400 :A01400 :N01700 :A01700 :N02300 :A02300 :N02500 :A02500 :N03300 :A03300 :N00101 :A00101 :N04470 :A04470 :N18425 :A18425 :N18450 :A18450 :N18500 :A18500 :N18300 :A18300 :N19300 :A19300 :N19700 :A19700 :N04800 :A04800 :N07100 :A07100 :N07220 :A07220 :N07180 :A07180 :N07260 :A07260 :N59660 :A59660 :N59720 :A59720 :N11070 :A11070 :N09600 :A09600 :N06500 :A06500 :N10300 :A10300 :N11901 :A11901 :N11902 :A11902])

(def skip-header
  (r/remove #(.startsWith % "STATEFIPS")))

(defn parse-double [x]
  (Double/parseDouble x))

(defn parse-line [line]
  (let [[text-fields double-fields] (->> (str/split line #",")
                                         (split-at 2))]
    (concat text-fields
            (map parse-double double-fields))))

(defn format-record [column-names line]
  (zipmap column-names line))

(def line-formatter
  (r/map parse-line))

(defn record-formatter [column-names]
  (r/map (fn [fields]
           (zipmap column-names fields))))

(def remove-zero-zip
  (r/remove (fn [record]
              (zero? (:zipcode record)))))

(defn chunks [coll]
  (->> (into [] coll)
       (t/chunk 1024)))

(defn parse-columns [line]
  (->> (str/split line #",")
       (map keyword)))

(defn load-data [file]
  (let [data (iota/seq file)
        col-names  (parse-columns (first data))
        parse-file (comp remove-zero-zip
                      (record-formatter col-names)
                      line-formatter)]
    (parse-file (rest data))))

(defn prepare-data []
  (->> (t/remove #(.startsWith % "STATEFIPS"))
       (t/map parse-line)
       (t/map (partial format-record column-names))
       (t/remove  #(zero? (:zipcode %)))))

(defn feature-matrix [record features]
  (let [xs (map #(get record %) features)]
    (i/matrix (cons 1 xs))))

(defn extract-features [fy features]
  (fn [record]
    {:y  (fy record)
     :xs (feature-matrix record features)}))

(defn scale-features [factors]
  (let [f (fn [x {:keys [mean sd]}]
            (/ (- x mean) sd))]
    (fn [x]
      (merge-with f x factors))))

(defn unscale-features [factors]
  (let [f (fn [x {:keys [mean sd]}]
            (+ (* x sd) mean))]
    (fn [x]
      (merge-with f x factors))))

(defn calculate-error [coefs-t]
  (fn [{:keys [y xs]}]
    (let [y-hat (first (i/mmult coefs-t xs))
          error (- y-hat y)]
      (i/mult xs error))))

(defn update-coefficients [coefs alpha]
  (fn [cost]
    (->> (i/mult cost alpha)
         (i/minus coefs))))
