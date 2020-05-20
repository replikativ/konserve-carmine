(defproject io.replikativ/konserve-carmine "0.1.4-SNAPSHOT"
  :description "A redis backend with carmine for konserve."
  :url "http://github.com/replikativ/konserve-carmine"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0" :scope "provided"]
                 [io.replikativ/konserve "0.6.0-20200512.093105-1"]
                 [com.taoensso/carmine "2.19.1"]]
  :profiles { :dev {:dependencies [[metosin/malli "0.0.1-20200404.091302-14"]]}})
