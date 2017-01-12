# konserve-carmine

A redis backend for [konserve](https://github.com/replikativ/konserve) implemented with [carmine](https://github.com/ptaoussanis/carmine). 

## Usage

Add to your leiningen dependencies:
[![Clojars Project](http://clojars.org/io.replikativ/konserve-carmine/latest-version.svg)](http://clojars.org/io.replikativ/konserve-carmine)

The purpose of konserve is to have a unified associative key-value interface for
edn datastructures and binary blobs. Use the standard interface functions of konserve.

You can provide the carmine redis connection specification map to the
`new-carmine-store` constructor as an argument. We do not require additional
settings beyond the konserve serialization protocol for the store, so you can
still access the store through carmine directly wherever you need.

~~~clojure
  (require '[konserve-carmine.core :refer :all]
           '[konserve.core :as k)
  (def carmine-store (<!! (new-carmine-store)))

  (<!! (k/exists? carmine-store  "john"))
  (<!! (k/get-in carmine-store ["john"]))
  (<!! (k/assoc-in carmine-store ["john"] 42))
  (<!! (k/update-in carmine-store ["john"] inc))
  (<!! (k/get-in carmine-store ["john"]))
~~~




## License

Copyright © 2016 Christian Weilbach

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
