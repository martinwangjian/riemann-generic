(defproject riemann-generic "0.2.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[riemann-cond-dt "1.0.3"]]
  :main ^:skip-aot riemann-generic.core
  :target-path "target/%s"
  :profiles {:dev {:dependencies [[riemann "0.3.0"]
                                  [org.clojure/clojure "1.9.0"]]}})
