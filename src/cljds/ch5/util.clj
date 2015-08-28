(ns cljds.ch5.util
  (:require [clojure.java.io :as io]))

(defn rand-file [path]
  (io/file path (str (long (rand 0x100000000)))))
