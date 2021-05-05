(ns jepsen.ocons
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [cli :as cli]
             [client :as client]
             [control :as c]
             [db :as db]
             [tests :as tests]
             [generator :as gen]
             ]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [knossos.model :as model]
            [cheshire.core :as json]
            ))

(def dir    "/opt/ocons/")
(def binary "ocons-main.exe")
(def client-bin "ocons-client.exe")
(def logfile (str dir "ocons.log"))
(def pidfile (str dir "ocons.pid"))

(defn node-url
  "A HTTP url for connecting on a particular port."
  [node port]
  (str node ":" port))

(def peer-port 2380)
(defn peer-url ""
  [node]
  (node-url node peer-port))

(def client-port 2379)
(defn client-url ""
  [node]
  (node-url node client-port))

(defn initial-cluster
  "Constructs the initial cluster string for a test: 1:node-1:2380,2:node-2:2380"
  [test]
  (->> (:nodes test)
       (map-indexed (fn [idx, node]
              (str idx ":" (peer-url node))))
       (str/join ",")))

(defn client-server-addrs
  "Gets the comma separated list of server addresses for the client"
  [test]
  (->> (:nodes test)
       (map (fn [node]
              (client-url node)))
       (str/join ",")))

(defn to-file [path] (clojure.java.io/file path))

(defn db
  "ocons DB"
  [variation]
  (reify db/DB
    (setup! [_ test node]
      (info node "Installing ocons" variation)
      (c/exec :mkdir dir)
      (c/upload (to-file (str variation ".exe")) (str dir binary))
      (c/upload (to-file (str "ocons-client.exe")) (str dir client-bin))
      (c/exec :chmod :+x (str dir binary))
      (c/exec :chmod :+x (str dir client-bin))
      (cu/start-daemon!
        {:logfile logfile
         :pidfile pidfile
         :chdir dir}
        binary
        (.indexOf (:nodes test) node)
        (initial-cluster test)
        (str dir "data")
        client-port peer-port
        5 0.1 
        :-s 500
      )
      (Thread/sleep 10000)
      )
   (teardown! [_ test node]
      (info node "tearing down ocons")
      (cu/stop-daemon! binary pidfile)
      (c/su (c/exec :rm :-rf dir))
      )
   db/LogFiles
   (log-files [_ test node] [logfile (str dir "logger")])
   ))

(defn r [_ _] {:type :invoke, :f :read, :value nil})
(defn w [_ _] {:type :invoke, :f :write, :value (rand-int 5)})

(defn ocons-get!
  "Get a value for a key"
  [node test key]
  (c/on node
        (c/su
          (c/exec (str dir client-bin) :get (client-server-addrs test) (c/lit key))
          )
        )
  )

(defn ocons-set!
  "Stores a value for a key"
  [node test key value]
  (c/on node
        (c/su
          (c/exec (str dir client-bin) :put (client-server-addrs test) (c/lit key) (c/lit value))
          )
        )
  )

(defrecord Client [k node]
  client/Client

  (open! [this test node]
    (assoc this :node node)
    )

  (close! [_ _] )

  (setup! [this test]
    (do
      (ocons-set! (:client this) test k (json/generate-string nil))
      ))

  (invoke! [client test op]
    (let [node (:node client)]
      (case (:f op)
        :read  (let [resp (ocons-get! node test k)]
                 (assoc op :type :ok :value resp)
                 )
        :write  (do (->> (:value op)
                         json/generate-string
                         (ocons-set! node test k))
                    (assoc op :type :ok))
        )
      )
    )

  (teardown! [_ test])
  )

(defn ocons-test ""
  [opts]
  (merge tests/noop-test 
         opts
         {:name "ocons"
          :os debian/os
          :db (db "paxos")
          :pure-generators true
          :client (Client. "/jepsen" nil)
          :generator (->> (gen/mix [r w])
                          (gen/stagger 1)
                          (gen/nemesis nil)
                          (gen/time-limit 15))
          }
         ))

(defn -main "" 
  [& args]
  (cli/run! (cli/single-test-cmd {:test-fn ocons-test}) 
            args))
