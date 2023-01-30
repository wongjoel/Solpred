(ns solpred.util.tar
  "Take Tar files of days and make Cross-Validation friendly shards"
  (:require
   [solpred.util.file :as file]
   [clojure.java.io :as io])
  (:import
   (org.apache.commons.compress.archivers.tar TarArchiveEntry TarArchiveOutputStream TarArchiveInputStream)
   (java.io ByteArrayOutputStream)
   (java.nio.file Files)))

(defn add-file-to-tar
  "Adds a file to a tar output stream"
  [stream in-path entry-name]
  (.putArchiveEntry stream (.createArchiveEntry stream (file/as-file in-path) entry-name))
  (io/copy (file/as-file in-path) stream)
  (.closeArchiveEntry stream))

(defn flat-folder->sorted-tar
  #_(flat-folder->sorted-tar "/work/def/abc" "/work/abc.tar")
  "Makes a tar from a folder, only taking immediate children into account"
  [input-dir tar-file]
  (let [files (sort (filter (fn [x] (file/file? x)) (file/list-children input-dir)))]
    (with-open [out-stream (TarArchiveOutputStream. (io/output-stream tar-file))]
      (run! (fn [file] (add-file-to-tar out-stream file (str "./" (file/filename file)))) files))))

(defn folder->flat-tar
  #_(folder->flat-tar "/work/def/abc" "/work/abc.tar")
  "Makes a tar from a folder, flattening all files into the main dir"
  [input-dir tar-file]
  (with-open [out-stream (TarArchiveOutputStream. (io/output-stream tar-file))]
    (Files/walkFileTree (file/as-path input-dir)
                        (proxy
                            [java.nio.file.SimpleFileVisitor] []
                          (visitFile [file attrs] (do (add-file-to-tar out-stream file (str "./" (file/filename file)))
                                                      java.nio.file.FileVisitResult/CONTINUE))))))

(defn extract-tar-to-disk
  "Extracts tar file to disk"
  #_(extract-tar-to-disk "/data/test.tar" "/data/test")
  [tar-file output-dir]
  (with-open [istream (TarArchiveInputStream. (io/input-stream tar-file))]
    (loop [entry (.getNextTarEntry istream)]
      (when-not (nil? entry)
        (let [name (.getName entry)
              path (file/resolve-path [output-dir name])]
          (when-not (.canReadEntryData istream entry)
            (str "Problem reading: " name))
          (if (.isDirectory entry)
            (file/make-dirs path)
            (io/copy istream (file/as-file path)))
          (recur (.getNextTarEntry istream)))))))

(defn add-buffer-to-tar
  "Adds a buffer to a tar output stream"
  [stream in-buffer entry-header]
  (.putArchiveEntry stream (TarArchiveEntry. entry-header))
  (io/copy in-buffer stream)
  (.closeArchiveEntry stream))

(defn copy-tar-contents
  "Copies contents from one tar archive to another."
  #_(with-open [istream (TarArchiveInputStream. (io/input-stream "/data/test.tar"))
              ostream (TarArchiveOutputStream. (io/output-stream "/data/out.tar"))]
    (copy-tar-contents istream ostream))
  [istream ostream]
  (loop [entry (.getNextTarEntry istream)]
      (when-not (nil? entry)
        (let [name (.getName entry)
              size (.getSize entry)
              header (byte-array 512)]
          (.writeEntryHeader entry header)
          (when-not (.canReadEntryData istream entry)
            (str "Problem reading: " name))
          (when (.isFile entry)
            (let [tmp-buffer (ByteArrayOutputStream. size)]
              (io/copy istream tmp-buffer)
              (add-buffer-to-tar ostream (.toByteArray tmp-buffer) header)))
          (recur (.getNextTarEntry istream))))))


(defn combine-tars
  "Combine the contents of multiple tar files into one tar file"
  #_(combine-tar-files ["/data/test2.tar" "/data/test3.tar" "/data/test4.tar"] "/data/out.tar")
  [tars output-path]
  (when-not (file/exists? (file/parent output-path))
    (throw (IllegalArgumentException. (str (file/parent output-path) "does not exist"))))
  (with-open [outstream (TarArchiveOutputStream. (io/output-stream output-path))]
    (run! (fn [in-tar]
            (with-open [instream (TarArchiveInputStream. (io/input-stream in-tar))]
              (copy-tar-contents instream outstream)))
          tars)))
