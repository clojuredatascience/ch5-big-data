(ns cljds.ch5.hadoop
  (:gen-class)
  (:require [cljds.ch5.data :as ch5d]
            [cljds.ch5.tesser :refer [feature-scales
                                      gradient-descent-hadoop-fold
                                      gradient-descent-fold]]
            [cljds.ch5.data :refer :all]
            [cljds.ch5.util :refer :all]
            [clojure.java.io :as io]
            [clojure.pprint :refer [pprint]]
            [incanter.core :as i]
            [parkour.io [text :as text]]
            [parkour [tool :as tool] [conf :as conf]]
            [tesser [core :as t]
                    [hadoop :as h]]))

(defn hadoop-gradient-descent [conf input-file workdir]
  (let [features [:A00200 :AGI_STUB :NUMDEP :MARS2]
        fcount  (inc (count features))
        coefs   (vec (replicate fcount 0))
        input   (text/dseq input-file)
        options {:column-names column-names
                 :features features
                 :coefs coefs
                 :fy :A02300
                 :alpha 1e-3}
        factors (h/fold conf input (rand-file workdir)
                        #'feature-scales
                        features)
        descend (fn [coefs]
                  (h/fold conf input (rand-file workdir)
                          #'gradient-descent-fold
                          (merge options {:coefs coefs
                                          :factors factors})))]
    (take 5 (iterate descend coefs))))

(defn -main [& args]
  (tool/run hadoop-gradient-descent args))
