# konserve-carmine

A redis backend for [konserve](https://github.com/replikativ/konserve) implemented with [carmine](https://github.com/ptaoussanis/carmine). 


# Status

![Clojure CI](https://github.com/alekcz/konserve-carmine/workflows/Clojure%20CI/badge.svg?branch=master) [![codecov](https://codecov.io/gh/alekcz/konserve-carmine/branch/master/graph/badge.svg)](https://codecov.io/gh/alekcz/konserve-carmine) 

## Usage

[![Clojars Project](https://img.shields.io/clojars/v/io.replikativ/konserve-carmine.svg)](http://clojars.org/io.replikativ/konserve-carmine)

`[alekcz/konserve-carmine "0.1.4-SNAPSHOT"]`

The purpose of konserve is to have a unified associative key-value interface for
edn datastructures and binary blobs. Use the standard interface functions of konserve.

You can provide the carmine redis connection specification map to the
`new-carmine-store` constructor as an argument. We do not require additional
settings beyond the konserve serialization protocol for the store, so you can
still access the store through carmine directly wherever you need.

```clojure
(require '[konserve-carmine.core :refer :all]
         '[clojure.core.async :refer [<!!] :as async]
         '[konserve.core :as k])
  
  (def carmine-store (<!! (new-carmine-store {:pool {} :spec {:uri "redis://localhost:6379/"}})))

  (<!! (k/exists? carmine-store  "cecilia"))
  (<!! (k/get-in carmine-store ["cecilia"]))
  (<!! (k/assoc-in carmine-store ["cecilia"] 28))
  (<!! (k/update-in carmine-store ["cecilia"] inc))
  (<!! (k/get-in carmine-store ["cecilia"]))

  (defrecord Test [a])
  (<!! (k/assoc-in carmine-store ["agatha"] (Test. 35)))
  (<!! (k/get-in carmine-store ["agatha"]))
```




## License

Copyright © 2016-2020 Christian Weilbach and contributors

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
