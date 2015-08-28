(ns cljds.ch5.parkour
  (:gen-class)
  (:require [cljds.ch5.data :refer :all]
            [cljds.ch5.tesser :refer [feature-scales]]
            [cljds.ch5.tesser :refer [matrix-mean]]
            [cljds.ch5.util :refer :all]
            [clojure.core.reducers :as r]
            [clojure.string :as str]
            [parkour (conf :as conf) (fs :as fs) (mapreduce :as mr)
             ,       (graph :as pg) (reducers :as pr) [tool :as tool]]
            [parkour.io (seqf :as seqf) (avro :as mra) (dux :as dux)
             ,          (sample :as sample) [text :as text]]
            [tesser.core :as t]
            [tesser.hadoop :as h]
            tesser.hadoop.serialization
            [transduce.reducers :as tr]
            [incanter.core :as i])
  (:import [org.apache.hadoop.io NullWritable]
           [tesser.hadoop_support FressianWritable]))

(defn parse-m
  {::mr/source-as :vals
   ::mr/sink-as   :vals}
  [fy features factors lines]
  (->> (skip-header lines)
       (r/map parse-line)
       (r/map (partial format-record column-names))
       (r/map (scale-features factors))
       (r/map (extract-features fy features))
       (into [])
       (shuffle)
       (partition 250)))

(defn sum-r
  {::mr/source-as :vals
   ::mr/sink-as   :vals}
  [fcount alpha batches]
  (let [initial-coefs (vec (replicate fcount 0))
        descend-batch (fn [coefs batch]
                        (->> (t/map (calculate-error
                                     (i/trans coefs)))
                             (t/fold (matrix-mean fcount 1))
                             (t/post-combine
                              (update-coefficients coefs alpha))
                             (t/tesser (chunks batch))))]
    (r/reduce descend-batch initial-coefs batches)))

(defn hadoop-extract-features [conf workdir input output]
  (let [fy       :A02300
        features [:A00200 :AGI_STUB :NUMDEP :MARS2]
        fcount   (inc (count features))
        input   (text/dseq input)
        factors (h/fold conf input (rand-file workdir)
                        #'feature-scales
                        features)
        conf (conf/ig)]
    (-> (pg/input input)
        (pg/map #'parse-m fy features factors)
        (pg/output (text/dsink output))
        (pg/execute conf "extract-features-job"))))

(defn hadoop-sgd [conf workdir input-file output]
  (let [kv-classes [NullWritable FressianWritable]
        fy       :A02300
        features [:A00200 :AGI_STUB :NUMDEP :MARS2]
        fcount   (inc (count features))
        input   (text/dseq input-file)
        factors (h/fold conf input (rand-file workdir)
                        #'feature-scales
                        features)
        conf (conf/assoc! conf "mapred.reduce.tasks" 1)]
    (-> (pg/input input)
        (pg/map #'parse-m fy features factors)
        (pg/partition kv-classes)
        (pg/reduce #'sum-r fcount 1e-8)
        (pg/output (text/dsink output))
        (pg/execute conf "sgd-job"))))

(defn -main [& args]
  (tool/run hadoop-sgd args))
