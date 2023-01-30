(ns solpred.util.external
  (:require [clojure.tools.logging :as log])
  (:import org.zeroturnaround.exec.stream.slf4j.Slf4jStream)
  (:import org.zeroturnaround.exec.ProcessExecutor))

(defn make-executor
  "Construct a process executor that will close with the VM and output to Debug logger"
  [command]
  (doto (ProcessExecutor.)
    (.command command)
    (.redirectOutput (.asDebug (Slf4jStream/ofCaller)))
    (.destroyOnExit)))

(defn execute
  #_(execute ["ls" "-l"])
  "Execute the command synchronously"
  [command]
  (-> (make-executor command)
      (.execute)))

(defn execute-with-output
  "Excecute the command and return the output as a string"
  [command]
  (-> (make-executor command)
      (.readOutput true)
      (.execute)
      (.outputUTF8)))

(defn execute-with-workdir
  "Set the workdir and execute"
  [command directory]
  (-> (make-executor command)
      (.directory directory)
      (.execute)))

(defn execute-with-exit-value
  "Execute the command synchronously, expecting a given exit value"
  ([command]
   (execute-with-exit-value command 0))
  ([command expected-value]
   (-> (make-executor command)
       (.exitValue (int expected-value))
       (.execute))))

(defn execute-with-timeout
  "Execute the command, checking the exit value with a timeout value in minutes"
  [command timeout]
  (try
    (-> (make-executor command)
        (.timeout timeout java.util.concurrent.TimeUnit/MINUTES)
        (.exitValue (int 0))
        (.execute))
    :okay
    (catch org.zeroturnaround.exec.InvalidExitValueException _
      (log/warn "bad exit value")
      :error)
    (catch java.util.concurrent.TimeoutException _
      (log/warn "timed out")
      :timeout)))

(defn start
  "Start the command in the background, returning a future"
  [command]
  (-> (make-executor command)
      (.getFuture)))
