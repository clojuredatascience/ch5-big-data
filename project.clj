(defproject cljds/ch5 "0.1.0"
  :description "Example code for the book Clojure for Data Science"
  :url "https://github.com/clojuredatascience/ch5-big-data"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [iota "1.1.2"]
                 [incanter/incanter "1.5.5"]
                 [tesser.core "1.0.0"]
                 [tesser.math "1.0.0"]
                 [tesser.hadoop "1.0.1"]
                 [org.clojure/tools.cli "0.3.1"]

                 [transduce "0.1.0"]
                 [org.apache.avro/avro "1.7.5"]
                 [org.apache.avro/avro-mapred "1.7.5"
                  :classifier "hadoop2"]
                 [com.damballa/parkour "0.5.4"]]
  
  :resource-paths ["data"]
  
  :profiles {:dev {:resource-paths ["dev-resources"]
                   :repl-options {:init-ns cljds.ch5.examples}}
             :uberjar {:main cljds.ch5.hadoop
                       :aot [cljds.ch5.hadoop]
                       :uberjar-exclusions [#".*LICENSE.*" #".*license.*"]}
             :provided {:dependencies
                        [[org.apache.hadoop/hadoop-client "2.4.1"]
                         [org.apache.hadoop/hadoop-common "2.4.1"]
                         [org.slf4j/slf4j-api "1.6.1"]
                         [org.slf4j/slf4j-log4j12 "1.6.1"]
                         [log4j "1.2.17"]]}}

  :aot [cljds.ch5.core]
  :main cljds.ch5.core)
