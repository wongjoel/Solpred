(ns solpred.util.sun-coordinates
  "Converts between azimuth/zenith and x,y coords in the fisheye image
  Coordinate Convention:
  2D image: X=Row, Y=Col
  Azimuth is counter-clockwise from due north 0=north -90=east 180=south west=90
  Invert-Azimuth is clockwise from due north 0=north 90=east 180=south west=-90
  Zenith is from underneath 180=overhead 90=horizon
  Invert-Zenith is from overhead 0=overhead 90=horizon"
  (:require
   [clojure.string :as str]
   [clojure.math :as math]
   [clojure.tools.logging :as log]
   [clojure.data.json :as json]
   [solpred.util.runtime-check :as runtime-check]
   )
  (:import
   (java.time LocalDate LocalDateTime Duration))
  )

(defn invert-azimuth-degrees
  #_(invert-azimuth-degrees {:radius 1 :azimuth 90 :zenith 45})
  [{:keys [azimuth] :as point}]
  (let [invert-azimuth (- 360 azimuth)]
    (assoc point :azimuth invert-azimuth))
  )

(defn invert-azimuth-radians
  #_(invert-azimuth-radians {:radius 1 :azimuth (math/to-radians 45) :zenith 45})
  [{:keys [azimuth] :as point}]
  (let [invert-azimuth (- (math/to-radians 360) azimuth)]
    (assoc point :azimuth invert-azimuth))
  )

(defn invert-zenith-degrees
  #_(invert-zenith-degrees {:radius 1 :azimuth 0 :zenith 45})
  [{:keys [zenith] :as point}]
  (let [invert-zenith (+ 180 zenith)]
    (assoc point :zenith invert-zenith))
  )

(defn invert-zenith-radians
  #_(invert-zenith-radians {:radius 1 :azimuth 0 :zenith (math/to-radians 90)})
  [{:keys [zenith] :as point}]
  (let [invert-zenith (+ (math/to-radians 180) zenith)]
    (assoc point :zenith invert-zenith))
  )

(defn spherical->cartesian
  #_(spherical->cartesian {:radius 1 :azimuth (math/to-radians 0) :zenith (math/to-radians 45)})
  [{:keys [radius azimuth zenith]}]
  {:x (* radius (math/sin zenith) (math/cos azimuth))
   :y (* radius (math/sin zenith) (math/sin azimuth))
   :z (* radius (math/cos zenith))
   }
  )

(defn line-to-double
  #_(line-to-double "5 -1.314797e+002 0.000000e+000 1.848991e-003 2.251206e-007 -7.343233e-010 ")
  [line]
  (->> (str/split line #" ")
       (map parse-double)
       vec))

(defn line-to-int
  #_(line-to-int "768 1024")
  [line]
  (->> (str/split line #" ")
       (map parse-long)
       vec))

(defn parse-line
  #_(parse-line "5 -1.314797e+002 0.000000e+000 1.848991e-003 2.251206e-007 -7.343233e-010 ")
  #_(parse-line "768 1024")
  [line]
  (if (str/includes? line ".")
    (line-to-double line)
    (line-to-int line)))

(defn read-model-from-file!
  #_(read-model-from-file! "/work/calib_results_catadioptric.txt")
  [path]
  (let [lines (->> (str/split (slurp path) #"\n")
                   (filter #(not (= % "")))
                   (filter #(not (str/starts-with? % "#"))))
        centre (parse-line (nth lines 2))
        affine (parse-line (nth lines 3))
        size (parse-line (nth lines 4))]
    {:coefficients-direct (rest (parse-line (nth lines 0)))  ;the first number is the count of coefficients 
     :coefficients-inverse (rest (parse-line (nth lines 1)))  ;the first number is the count of coefficients 
     :centre {:row (first centre) :col (second centre)}
     :affine {:c (first affine) :d (second affine) :e (nth affine 2)}
     :size {:height (first size) :width (second size)}})
  )

(defn cart3d->image
  #_(cart3d->image sample-model {:x 0 :y 0 :z 1})
  [model {:keys [x y z]}]
  (let [norm (math/sqrt (+ (* x x) (* y y)))
        centre-row (get-in model [:centre :row])
        centre-col (get-in model [:centre :col])]
    (if (zero? norm)
      {:row centre-row
       :col centre-col}
      (let [inv-norm (/ 1 norm)
            theta (math/atan (/ z norm))
            rho (loop [rho (first (:coefficients-inverse model))
                       coefficients (rest (:coefficients-inverse model))
                       t-i 1]
                  (if (empty? coefficients)
                    rho
                    (let [t-next (* t-i theta)]
                      (recur
                       (+ rho (* t-next (first coefficients)))
                       (rest coefficients)
                       t-next))))
            x-2d (* x inv-norm rho)
            y-2d (* y inv-norm rho)]
        {:row (+ (* x-2d (get-in model [:affine :c])) (* y-2d (get-in model [:affine :d])) centre-row)
         :col (+ (* x-2d (get-in model [:affine :e])) y-2d centre-col)}))
    ))

(defn spherical->image
  #_(spherical->image sample-model {:radius 1 :azimuth (math/to-radians 0) :zenith (math/to-radians 90)})
  #_(spherical->image (read-model-from-file! "/work/calib_results-Blackmountain.txt")
                      {:radius 1 :azimuth (math/to-radians 35.24) :zenith (math/to-radians 19.29) :dt "2015-01-01_12-00-09"})
  #_(spherical->image (read-model-from-file! "/work/calib_results-Blackmountain.txt")
                      {:radius 1 :azimuth (math/to-radians 293.31) :zenith (math/to-radians 30.62) :dt "2015-01-01_15-00-04"})
  [model sph-coords]
  (->> sph-coords
       invert-zenith-radians
       #_invert-azimuth-radians
       spherical->cartesian
       (cart3d->image model)))

(defn calculate-crop
  #_(calculate-crop {:crop-size 64 :azimuth 35 :zenith 20 :lens-model-path "/work/calib_results-Blackmountain.txt"})
  [{:keys [crop-size azimuth zenith lens-model-path] :as args}]
  (runtime-check/map-contains? args [:crop-size :azimuth :zenith :lens-model-path])
  (let [model (read-model-from-file! lens-model-path)
        sun-loc (spherical->image model {:radius 1 :azimuth (math/to-radians azimuth) :zenith (math/to-radians zenith)})
        sun-loc-round {:row (math/round (:row sun-loc))
                       :col (math/round (:col sun-loc))}
        dist (/ crop-size 2)]
    (runtime-check/throw-on-false (= dist (math/round dist)) (str crop-size " is not even, can't crop cleanly"))
    {:left (- (:col sun-loc-round) dist)
     :upper (- (:row sun-loc-round) dist)
     :right (+ (:col sun-loc-round) dist)
     :lower (+ (:row sun-loc-round) dist)}
    ))

(defn squared
  "Square a number"
  [x]
  (* x x))

(defn distance
  #_(distance [1 2] [-1 -2])
  "Distance between two points on an xy plane"
  [[x1 y1] [x2 y2]]
  (math/sqrt (+ (squared (- x2 x1)) (squared (- y2 y1)))))

(defn strict-radius
  #_(strict-radius (read-model-from-file! "/data/blackmountain/images/calib_results-Blackmountain.txt"))
  "Strict radius, will cut off usable parts of the image"
  [model]
  (min (- (get-in model [:size :height]) (get-in model [:centre :row]))
       (get-in model [:centre :row])))

(defn loose-radius
  #_(loose-radius (read-model-from-file! "/data/blackmountain/images/calib_results-Blackmountain.txt"))
  "Loose radius, may go beyond the image slightly, assumes 1% of image is cut off at top and bottom"
  [model]
  (let [centre-row (/ (get-in model [:size :height]) 2)]
    (+ centre-row (* 0.01 centre-row))))

(defn crop-out-of-bounds?
  #_(crop-out-of-bounds? {:left 885 :upper 566 :right 949 :lower 630 :lens-model-path "/data/blackmountain/images/calib_results-Blackmountain.txt"})
  #_(crop-out-of-bounds? {:left 0 :upper 566 :right 949 :lower 630 :lens-model-path "/data/blackmountain/images/calib_results-Blackmountain.txt"})
  #_(crop-out-of-bounds? {:left 885 :upper 566 :right 2000 :lower 630 :lens-model-path "/data/blackmountain/images/calib_results-Blackmountain.txt"})
  #_(crop-out-of-bounds? {:left 885 :upper -1 :right 949 :lower 630 :lens-model-path  "/data/blackmountain/images/calib_results-Blackmountain.txt"})
  #_(crop-out-of-bounds? {:left 885 :upper 566 :right 949 :lower 1537 :lens-model-path  "/data/blackmountain/images/calib_results-Blackmountain.txt"})
  [{:keys [left upper right lower lens-model-path] :as args}]
  (let [model (read-model-from-file! lens-model-path)
        radius (loose-radius model)
        centre [(get-in model [:centre :col]) (get-in model [:centre :row])]
        corners (for [col [left right]
                      row [upper lower]]
                  [col row])]
    (->> corners
         (map (fn [corner] (distance corner centre)))
         (some (fn [dist] (> dist radius)))
         boolean)))

(def sample-model
  {:coefficients-direct (parse-line "5 -5.517655e+002 0.000000e+000 8.372454e-004 -6.474789e-007 1.235631e-009")
   :coefficients-inverse (parse-line "8 730.949123 315.876984 -177.960849 -352.468231 -678.144608 -615.917273 -262.086205 -42.961956")
   :centre {:row 382.294245 :col 515.323794}
   :affine {:c 0.999845 :d 4.4E-5 :e 8.0E-6}
   :size {:height 768 :width 1024}})

(comment

  (spit
   "/work/coords.json"
   (json/write-str
    (->> (for [x (range -90 100 10)]
           {:radius 1
            :azimuth 0
            :zenith (math/to-radians x)})
         (map (fn [x] (merge x (spherical->image (read-model-from-file! "/work/calib_results-Blackmountain.txt") x))))
         )))
  
  (spit
   "/work/coords.json"
   (json/write-str
    (->> (for [x (range -90 100 10)]
           {:radius 1
            :azimuth 0
            :zenith (math/to-radians x)})
         (map invert-zenith-radians)
         (map invert-azimuth-radians)
         (map (fn [x] (merge x (spherical->cartesian x))))
         (map (fn [x] (merge x (cart3d->image (read-model-from-file! "/work/calib_results-Blackmountain.txt") x))))
         )))

  (spit
   "/work/coords.json"
   (json/write-str
    (->> (for [x (range -180 190 10)]
           {:radius 1
            :azimuth (math/to-radians x)
            :zenith (math/to-radians 45)})
         (map (fn [x] (merge x (spherical->image (read-model-from-file! "/work/calib_results-Blackmountain.txt") x))))
         )))

  (spit
   "/work/coords.json"
   (json/write-str
    (->> (for [x (range 0 100 10)]
           {:radius 1
            :azimuth (math/to-radians x)
            :zenith (math/to-radians 45)})
         (map invert-zenith-radians)
         (map invert-azimuth-radians)
         (map (fn [x] (merge x (spherical->cartesian x))))
         (map (fn [x] (merge x (cart3d->image (read-model-from-file! "/work/calib_results-Blackmountain.txt") x))))
         )))
  )
