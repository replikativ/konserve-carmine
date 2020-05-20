(ns konserve-carmine.core
  "Address globally aggregated immutable key-value conn(s)."
  (:require [clojure.core.async :as async]
            [konserve.serializers :as ser]
            [hasch.core :as hasch]
            [taoensso.carmine :as car]
            [konserve.protocols :refer [PEDNAsyncKeyValueStore
                                        -exists? -get -get-meta
                                        -update-in -assoc-in -dissoc
                                        PBinaryAsyncKeyValueStore
                                        -bassoc -bget
                                        -serialize -deserialize
                                        PKeyIterable
                                        -keys]])
  (:import  [java.io ByteArrayInputStream ByteArrayOutputStream]
            [java.nio ByteBuffer]))

(set! *warn-on-reflection* 1)
      
(defn it-exists? 
  [conn id]
  (= (car/wcar conn (car/exists id)) 1)) 
  
(defn get-it 
  [conn id]
  (car/wcar conn (car/parse-raw (car/get id))))

(defn update-it 
  [conn id data]
  (car/wcar conn (car/set id (car/raw data))))

(defn delete-it 
  [conn id]
  (car/wcar conn (car/del id))) 

(defn get-keys 
  [conn]
  (car/wcar conn (car/keys "*")))

(defn str-uuid 
  [key] 
  (str (hasch/uuid key))) 

(defn prep-ex 
  [^String message ^Exception e]
  (ex-info message {:error (.getMessage e) :cause (.getCause e) :trace (.getStackTrace e)}))

(defn prep-stream 
  [bytes]
  { :input-stream  (ByteArrayInputStream. bytes) 
    :size (count bytes)})

(defrecord CarmineStore [conn serializer read-handlers write-handlers locks]
  PEDNAsyncKeyValueStore
  (-exists? 
    [this key] 
      (let [res-ch (async/chan 1)]
        (async/thread
          (try
            (async/put! res-ch (it-exists? conn (str-uuid key)))
            (catch Exception e (async/put! res-ch (prep-ex "Failed to determine if item exists" e)))))
        res-ch))

  (-get 
    [this key] 
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [res (get-it conn (str-uuid key))]
            (if (some? res) 
              (let [bais (ByteArrayInputStream. res)
                    data (-deserialize serializer read-handlers bais)]
                (async/put! res-ch (second data)))
              (async/close! res-ch)))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to retrieve value from store" e)))))
      res-ch))

  (-get-meta 
    [this key] 
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [res (get-it conn (str-uuid key))]
            (if (some? res) 
              (let [bais (ByteArrayInputStream. res)
                    data (-deserialize serializer read-handlers bais)] 
                (async/put! res-ch (first data)))
              (async/close! res-ch)))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to retrieve value metadata from store" e)))))
      res-ch))

  (-update-in 
    [this key-vec meta-up-fn up-fn args]
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [[fkey & rkey] key-vec
                old-val' (get-it conn (str-uuid fkey))
                old-val (when old-val'
                          (let [bais (ByteArrayInputStream. old-val')]
                            (-deserialize serializer read-handlers bais)))
                new-val [(meta-up-fn (first old-val)) 
                         (if rkey (apply update-in (second old-val) rkey up-fn args) (apply up-fn (second old-val) args))]
                ^ByteArrayOutputStream baos (ByteArrayOutputStream.)]
            (-serialize serializer baos write-handlers new-val)
            (update-it conn (str-uuid fkey) (.toByteArray baos))
            (async/put! res-ch [(second old-val) (second new-val)]))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to update/write value in store" e)))))
        res-ch))

  (-assoc-in [this key-vec meta val] (-update-in this key-vec meta (fn [_] val) []))

  (-dissoc 
    [this key] 
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (delete-it conn (str-uuid key))
          (async/close! res-ch)
          (catch Exception e (async/put! res-ch (prep-ex "Failed to delete key-value pair from store" e)))))
        res-ch))

  PBinaryAsyncKeyValueStore
  (-bget 
    [this key locked-cb]
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [res (get-it conn (str-uuid key))]
            (if (some? res) 
              (let [res-vec (vec res)
                    meta-len (-> res-vec (subvec 0 7) byte-array ByteBuffer/wrap (.getInt 0))
                    data (byte-array (subvec res-vec (+ 8 meta-len)))]
                (async/put! res-ch (locked-cb (prep-stream data))))
              (async/close! res-ch)))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to retrieve binary value from store" e)))))
      res-ch))

  (-bassoc 
    [this key meta-up-fn input]
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [old-val' (get-it conn (str-uuid key))
                old-val (when old-val'
                          (let [old-vec (vec old-val')
                                meta-len (-> old-vec (subvec 0 7) byte-array ByteBuffer/wrap (.getInt 0))
                                meta (subvec old-vec 8 (+ 8 meta-len))
                                bais (ByteArrayInputStream. (byte-array meta))]
                            [(-deserialize serializer read-handlers bais) 
                             (byte-array (subvec old-vec (+ 8 meta-len)))]))
                new-meta (meta-up-fn (first old-val))
                ^ByteArrayOutputStream baos (ByteArrayOutputStream.)
                _ (-serialize serializer baos write-handlers new-meta)
                meta-as-bytes (.toByteArray baos)
                meta-size (.putInt (ByteBuffer/allocate 8) (count meta-as-bytes))
                combined-byte-array (byte-array 
                                      (into [] 
                                        (concat (.array meta-size) meta-as-bytes input)))]
            (update-it conn (str-uuid key) combined-byte-array)
            (async/put! res-ch [(second old-val) input]))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to update/write binary value in store" e)))))
        res-ch))

  PKeyIterable
  (-keys 
    [_]
    (let [res-ch (async/chan)]
      (async/thread
        (try
          (let [key-stream (get-keys conn)
                keys' (when key-stream
                        (for [k key-stream]
                          (let [bais (ByteArrayInputStream. (get-it conn k))]
                            (first (-deserialize serializer read-handlers bais)))))
                keys (map :key keys')]
            (doall
              (map #(async/put! res-ch %) keys)))
          (async/close! res-ch) 
          (catch Exception e (async/put! res-ch (prep-ex "Failed to retrieve keys from store" e)))))
        res-ch)))


(defn new-carmine-store
  ([]
   (new-carmine-store {:pool {} :spec {}}))
  ([carmine-conn & {:keys [serializer read-handlers write-handlers]
                    :or {serializer (ser/fressian-serializer)
                         read-handlers (atom {})
                         write-handlers (atom {})}}]
    (let [res-ch (async/chan 1)]                      
      (async/thread 
        (try
          (async/put! res-ch
            (map->CarmineStore {:conn carmine-conn
                                :read-handlers read-handlers
                                :write-handlers write-handlers
                                :serializer serializer
                                :locks (atom {})}))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to connect to store" e)))))
      res-ch)))

(defn delete-store [store]
  (let [res-ch (async/chan 1)]
    (async/thread
      (try
        (car/wcar (:conn store) (car/flushall))
        (async/close! res-ch)
        (catch Exception e (async/put! res-ch (prep-ex "Failed to delete store" e)))))          
    res-ch))