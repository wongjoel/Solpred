(ns solpred.util.time
  "Convenience methods for dealing with Java time"
  (:require
   [solpred.util.runtime-check :as runtime-check])
  (:import (java.time LocalDate LocalDateTime Instant Duration)
           (java.time.format DateTimeFormatter)))

(defn string->localdate
  #_(string->localdate "2020-12-25")
  "Convenience method for parsing a localdate"
  ([date-string]
   (string->localdate date-string "yyyy-MM-dd"))
  ([date-string format-string]
   (LocalDate/parse date-string (DateTimeFormatter/ofPattern format-string))))

(defn datetime->string
  #_(datetime->string (LocalDateTime/now) "yyyy/yyyy-MM-dd_HH-mm-ss")
  "Convenience method for formatting a datetime"
  [datetime format-string]
  (.format datetime (DateTimeFormatter/ofPattern format-string)))

(defn string->datetime
  #_(string->datetime "2020-12-25_12-13-14" "yyyy-MM-dd_HH-mm-ss")
  "Convenience method for parsing a datetime"
  [dt-string format-string]
  (LocalDateTime/parse dt-string (DateTimeFormatter/ofPattern format-string)))

(defn localdate->string
  #_(localdate->string (LocalDate/now) "yyyy-MM-dd")
  "Convenience method for formatting a datetime"
  [date format-string]
  (.format date (DateTimeFormatter/ofPattern format-string)))

(defn now
  #_(now)
  "Get the current instant"
  []
  (Instant/now))

(defn from-milliseconds
  #_(from-milliseconds 1)
  "Convert milliseconds to Instant"
  [milliseconds]
  (Instant/ofEpochMilli milliseconds))

(defn duration-between
  #_(duration-between (from-milliseconds 1) (now))
  "Duration between two temporals"
  [a b]
  (Duration/between a b))

(defn seconds-between
  #_(seconds-between (from-milliseconds 1) (now))
  "Seconds between two temporals"
  [a b]
  (.toSeconds (duration-between a b)))

(defn seconds-since
  #_(seconds-since (from-milliseconds 1))
  "The number of seconds since the given instant and now"
  [instant]
  (seconds-between instant (now)))

(defn gen-date-seq
  #_(gen-date-seq (string->localdate "2020-01-01") (string->localdate "2020-02-01"))
  "Generate the sequence of dates from the start date (inclusive) to the end date (exclusive)"
  [start-date end-date]
  (iterator-seq (.iterator (.datesUntil start-date end-date))))

(defn gen-x-sec-datetime-seq
  #_(gen-x-sec-datetime-seq (string->datetime "2020-01-01_09-10-10" "yyyy-MM-dd_HH-mm-ss") (string->datetime "2020-01-01_09-20-10" "yyyy-MM-dd_HH-mm-ss") {:seconds 10 :gap-size 10})
  "Generate the sequence of datetimes from the start datetime (inclusive) to the end datetime (exclusive)"
  [start-datetime end-datetime {:keys [seconds gap-size] :as args}]
  (runtime-check/map-contains? args [:seconds :gap-size])
  (loop [dt start-datetime
         acc []]
    (if (< (seconds-between dt end-datetime) gap-size)
      acc
      (recur (.plusSeconds dt seconds) (conj acc dt)))))

(defn in-order?
  "Checks if the datetimes are strictly in order.
   There's clearly a way to generalise this to any number of inputs, but no time to implement"
  [dt1 dt2 dt3]
  (and (.isBefore dt1 dt2) (.isBefore dt2 dt3)))