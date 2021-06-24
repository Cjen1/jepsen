(ns jepsen.ocons
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen 
             [checker :as checker]
             [cli :as cli]
             [client :as client]
             [control :as c]
             [db :as db]
             [tests :as tests]
             [generator :as gen]
             [nemesis :as nemesis]
             ]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [knossos.model :as model]
            [cheshire.core :as json]
            )
  (:use [slingshot.slingshot :only [throw+ try+]]))

(def dir    "/opt/ocons/")
(def binary "ocons-main.exe")
(def client-bin "ocons-client.exe")
(def logfile (str dir "ocons.log"))
(def loggerfile (str dir "ocons.logger"))
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
        5 1 
        :-s 500
        :-log-level :info
        :-log-to-file loggerfile
      )
      (Thread/sleep 1000)
      )
   (teardown! [_ test node]
      (info node "tearing down ocons")
      (cu/stop-daemon! binary pidfile)
      (c/su (c/exec :rm :-rf dir))
      )
   db/LogFiles
   (log-files [_ test node] [logfile loggerfile])
   ))

(defn r [_ _] {:type :invoke, :f :read, :value nil})
(defn w [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(def not-found-pattern
  "OCons returns the following when a key does not exist"
  (re-pattern "Key not found"))

(def cas-pattern
  "OCons returns following error for CAS failed"
  (re-pattern "Key \\(.*\\) has value \\(.*\\) rather than \\(.*\\)"))

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

(defn ocons-cas!
  "Compare and swap operation"
  [node test key value_new value_old]
  (try+
    (c/on node
          (c/su
            (c/exec (str dir client-bin) :cas (client-server-addrs test) (c/lit key) (c/lit value_new) (c/lit value_old))
            )
          )
        (catch [:type :jepsen.control/nonzero-exit] ex
          (do 
            (info (:err ex))
            (if (re-find cas-pattern (:err ex))
              false
              (throw+))
            )
      )
    )
  )

(defn parse-long
  "Parses a string to a long. Passes through `nil`."
  [s]
  (info "parse-long" s)
  (when s 
    (if (not (= "null" s))
      (Long/parseLong s)
      nil
      )
    )
  )


(defrecord Client [k conn]
  client/Client

  (open! [this test node]
    (assoc this :conn node)
    )

  (close! [_ _] )

  (setup! [this test]
    (do
      (info "Setting up client")
      (ocons-set! conn test k (json/generate-string nil))
      ))

  (invoke! [client test op]
    (case (:f op)
      :read  (let [resp (ocons-get! conn test k)]
               (assoc op :type :ok :value (parse-long resp))
               )
      :write  (do (->> (:value op)
                       json/generate-string
                       (ocons-set! conn test k))
                  (assoc op :type :ok))
      :cas ( let [[value value'] (:value op)
                  ok? (ocons-cas! conn test k (json/generate-string value) (json/generate-string value'))]
             (assoc op :type (if ok? :ok :fail)))
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
          :nemesis (nemesis/partition-random-halves)
          :checker (checker/linearizable
                     {:model (model/cas-register)
                      :algorithm :linear})
          :generator (->> (gen/mix [r w cas])
                          (gen/stagger 1)
                          (gen/nemesis 
                            (cycle [(gen/sleep 5)
                                    {:type :info, :f :start}
                                    (gen/sleep 5)
                                    {:type :info, :f :stop}
                                    ]))
                          (gen/time-limit (:time-limit opts)))
          }
         ))

(defn -main "" 
  [& args]
  (cli/run! (cli/single-test-cmd {:test-fn ocons-test}) 
            args))
