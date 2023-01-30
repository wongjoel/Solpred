(ns solpred.util.zip
  "Utilies for using zip files"
  (:require
   [solpred.util.file :as file]
   [clojure.java.io :as io])
  (:import
   (org.apache.commons.compress.archivers.zip ZipFile ZipArchiveOutputStream)))

(defn get-missing-entries
  "returns entries not found in zipfile
  expected-entries is a seq of the entries expected to be contained in the zipfile"
  [expected-entries zip-path]
  (with-open [zf (ZipFile. (file/as-file zip-path))]
    (->> expected-entries
         (map (fn [x] {:path x :entry (.getEntry zf x)}))
         (filter (fn [x] (nil? (:entry x)))))))

(defn add-file-to-zip
  "Adds a file to a zip output stream"
  [zip-stream in-path zip-path]
  (.putArchiveEntry zip-stream (.createArchiveEntry zip-stream (file/as-file in-path) (str zip-path)))
  (io/copy (file/as-file in-path) zip-stream)
  (.closeArchiveEntry zip-stream))

(defn add-folder-to-zip
  "Adds a folder to a zip output stream"
  [zip-stream in-path zip-path]
  (.putArchiveEntry zip-stream (.createArchiveEntry zip-stream (file/as-file in-path) (str zip-path)))
  (.closeArchiveEntry zip-stream))

(defn add-folder-recursive
  "Adds folder and contents to zip archive"
  ([zip-stream in-dir]
   (add-folder-recursive zip-stream in-dir "/"))
  ([zip-stream in-dir zip-folder-base]
   (run!
    (fn [child]
      (cond (file/file? child) (add-file-to-zip zip-stream child (file/resolve-path [zip-folder-base (file/filename child)]))
            (file/dir? child) (let [new-base (file/resolve-path [zip-folder-base (file/filename child)])]
                                (add-folder-to-zip zip-stream child new-base)
                                (add-folder-recursive zip-stream child new-base))))
    (file/list-children in-dir))))

(defn folder->zip
  "Zips a folder"
  #_(folder->zip "/work/test" "/work/test.zip")
  [input-folder output-zip]
  (with-open [zos (ZipArchiveOutputStream. (file/as-file output-zip))]
    (add-folder-recursive zos input-folder)))

(defn folder->enclosed-zip
  "Zips a folder, including the folder itself in the zip"
  #_(folder->enclosed-zip "/work/test" "/work/test.zip")
  [input-folder output-zip]
  (with-open [zos (ZipArchiveOutputStream. (file/as-file output-zip))]
    (let [new-base (file/filename input-folder)]
      (add-folder-to-zip zos input-folder new-base)
      (add-folder-recursive zos input-folder new-base))))

(defn extract-zip-to-disk
  #_(extract-zip-to-disk "/work/test.zip" "/work/test")
  "Extracts zip file to disk"
  [zip-path output-dir]
  (with-open [zf (ZipFile. (file/as-file zip-path))]
    (file/make-dirs output-dir)
    (let [entries (map (fn [entry] {:entry entry
                                    :out-path (file/resolve-path [output-dir (file/without-root (.getName entry))])})
                       (enumeration-seq (.getEntries zf)))
          {:keys [directories files]} (group-by (fn [entry-map] (if (.isDirectory (:entry entry-map)) :directories :files))
                                                entries)]
      (run! (fn [directory] (file/make-dir (:out-path directory))) directories)
      (run! (fn [file] (with-open [in-stream (.getInputStream zf (:entry file))]
                         (io/copy in-stream (file/as-file (:out-path file)))))
            files))))

(defn extract-zip-to-disk-flat
  #_(extract-zip-to-disk-flat "/work/test.zip" "/work/test")
  "Extracts zip file to disk, flattening the folder hierarchy"
  [zip-path output-dir]
  (with-open [zf (ZipFile. (file/as-file zip-path))]
    (file/make-dirs output-dir)
    (let [entries (map (fn [entry] {:entry entry
                                    :out-path (file/resolve-path [output-dir (file/filename (.getName entry))])})
                       (enumeration-seq (.getEntries zf)))
          {:keys [directories files]} (group-by (fn [entry-map] (if (.isDirectory (:entry entry-map)) :directories :files))
                                                entries)]
      (run! (fn [file] (with-open [in-stream (.getInputStream zf (:entry file))]
                         (io/copy in-stream (file/as-file (:out-path file)))))
            files))))
