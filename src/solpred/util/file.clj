(ns solpred.util.file
  (:require
   [clojure.spec.alpha :as s]
   [clojure.tools.logging :as log]
   [clojure.spec.test.alpha :as stest]
   [clojure.java.io :as io])
  (:import
   (java.io File)
   (java.nio.file Path Paths Files FileVisitOption LinkOption OpenOption CopyOption)
   (java.nio.file.attribute FileAttribute PosixFilePermission)))

;; clojure.spec.alpha entity definitions

(s/def ::path (partial instance? Path))
(s/def ::path-like (s/or :string string? :path ::path))

(defprotocol Path-like
  "Implement this protocol if your type can be converted to a
  java.nio.file.Path object."
  (^Path as-path [x]))

(extend-protocol Path-like
  String
  (as-path [x] (Paths/get x (into-array String [])))
  Path
  (as-path [x] x)
  File
  (as-path [x] (.toPath x)))

(defprotocol File-like
  "Implement this protocol if your type can be converted to a
  java.io.File object."
  (^File as-file [x]))

(extend-protocol File-like
  String
  (as-file [x] (io/file x))
  Path
  (as-file [x] (.toFile x))
  File
  (as-file [x] x))

(defn get-path
  "Wraps nio Path.get(String first, String... more)."
  ([path]
   (Paths/get path (into-array String [])))
  ([path & rest]
   (Paths/get path (into-array String rest))))

(defn make-dir
  "Creates an empty directory at the path. Parents must exist already."
  [path]
  (Files/createDirectory (as-path path) (into-array FileAttribute [])))

(defn make-dirs
  "Creates an empty directory at the path, including any parent directories, if they don't already exist."
  [path]
  (Files/createDirectories (as-path path) (into-array FileAttribute [])))

(defn make-temp-dir
  "Creates an empty directory with a unique name based on given prefix in given dir, uses default temp dir if none given"
  ([prefix]
   (str (Files/createTempDirectory prefix (into-array FileAttribute []))))
  ([dir prefix]
   (str (Files/createTempDirectory (as-path dir) prefix (into-array FileAttribute [])))))

(defn resolve-path
  #_(resolve-path ["a" "b" "c"])
  #_(resolve-path ["a"])
  #_(resolve-path ["/a" "b" "c/d"])
  "Resolve other onto base"
  [paths]
  (str (reduce (fn [base other] (.resolve (as-path base) (as-path other))) paths)))

(defn resolve-sibling
  #_(resolve-sibling ["a/b" "c"])
  "Resolves a sibling path"
  [path other]
  (str (.resolveSibling (as-path path) (as-path other))))

(defn relativize
  "Relativize other on base"
  [base other]
  (str (.relativize (as-path base) (as-path other))))

(defn filename
  "get the filename"
  [path]
  (str (.getFileName (as-path path))))

(defn parent
  "get the parent path"
  [path]
  (str (.getParent (as-path path))))

(defn as-seq
  "Get the path components as a seq"
  [path]
  (map str (iterator-seq (.iterator (as-path path)))))

(defn without-root
  "the path without the root"
  #_(without-root "/a/b/c/d.txt")
  #_(without-root "a/b/c/d.txt")
  [path]
  (let [p (as-path path)]
    (.subpath p 0 (.getNameCount p))))

(defn get-input-stream
  "Gets an input stream from a path. Caller must close the input stream"
  [path]
  (Files/newInputStream (as-path path) (into-array OpenOption [])))

(defn exists?
  "Tests whether the path exists."
  [path]
  (Files/exists (as-path path) (into-array LinkOption [])))

(defn not-exists?
  "Tests whether the path does not exist."
  [path]
  (Files/notExists (as-path path) (into-array LinkOption [])))

(defn file?
  "Tests whether the path is a file."
  [path]
  (Files/isRegularFile (as-path path) (into-array LinkOption [])))

(defn dir?
  "Tests whether the path is a directory."
  [path]
  (Files/isDirectory (as-path path) (into-array LinkOption [])))

(defn file-size
  "Return the size of a file - non-files are undefined"
  [path]
  (Files/size (as-path path)))

(defn list-children
  "Lists immediate children, including directories"
  [path]
  (with-open [stream (Files/list (as-path path))]
    (doall (iterator-seq (.iterator stream)))))

(defn walk
  "Walk a directory and return the children"
  ([path]
   (-> (as-path path)
       (Files/walk (into-array FileVisitOption []))
       .iterator
       iterator-seq
       doall))
  ([path depth]
   (-> (as-path path)
       (Files/walk depth (into-array FileVisitOption []))
       .iterator
       iterator-seq
       doall)))

(defn move
  "Moves or renames a file"
  [source target]
  (Files/move (as-path source) (as-path target) (into-array CopyOption [])))

(defn copy-file
  "Copies a file from `source` to `target`"
  [source target]
  (io/copy (as-file source) (as-file target)))

(defn copy-recursive
  "Copies a directory  from `source` to `target` including contents"
  [source target]
  (make-dirs target)
  (run!
   (fn [x]
     (cond (file? x) (copy-file x (resolve-path [target (filename x)]))
           (dir? x) (copy-recursive x (resolve-path [target (filename x)]))))
   (list-children source)))

(defn delete
  "Delete a file or empty directory"
  [path]
  (Files/delete (as-path path)))

(defn delete-if-exists
  "Delete a file or empty directory, returns true/false to indicate success"
  [path]
  (Files/deleteIfExists (as-path path)))

(defn delete-recursive
  #_(delete-recursive "/work/test-dir")
  "Delete a directory including contents"
  [path]
  (Files/walkFileTree (as-path path)
                      (reify java.nio.file.FileVisitor
                        (postVisitDirectory [this dir except] (do (delete dir) java.nio.file.FileVisitResult/CONTINUE))
                        (preVisitDirectory [this dir attrs] java.nio.file.FileVisitResult/CONTINUE)
                        (visitFile [this file attrs] (do (delete file) java.nio.file.FileVisitResult/CONTINUE))
                        (visitFileFailed [this file except] (do (log/error except) java.nio.file.FileVisitResult/CONTINUE) )
                        )))

(defn set-executable-bit
  #_(set-executable-bit "test.sh")
  "Sets the POSIX owner executable bit in addition to existing permissions"
  [path]
  (let [existing-perms (set (Files/getPosixFilePermissions (as-path path) (into-array LinkOption [])))]
    (Files/setPosixFilePermissions (as-path path)
                                   (conj existing-perms PosixFilePermission/OWNER_EXECUTE))))

;; clojure.spec.alpha functions
(s/fdef list :args (s/cat :path ::path-like))
(s/fdef walk :args (s/cat :path ::path-like :depth (s/? int?)))
(s/fdef resolve :args (s/cat :base ::path-like :other ::path-like))
(s/fdef make-dir :args (s/cat :path ::path-like))
(s/fdef make-dirs :args (s/cat :path ::path-like))
(s/fdef exists? :args (s/cat :path ::path-like))
(s/fdef file? :args (s/cat :path ::path-like))
(s/fdef dir? :args (s/cat :path ::path-like))

(stest/instrument `[list walk resolve make-dir make-dirs exists? file? dir?])
