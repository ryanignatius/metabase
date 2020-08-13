(ns metabase.query-processor.streaming.pdf
  (:require [cheshire.core :as json]
            [clj-pdf.core :as pdf]
            [java-time :as t]
            [metabase.query-processor.streaming
             [common :as common]
             [interface :as i]]
            [metabase.util
             [date-2 :as u.date]
             [i18n :refer [tru]]])
  (:import java.io.OutputStream))

(defmethod i/stream-options :pdf
  [_ ^java.lang.String card-name]
  {:content-type              "application/pdf"
   :write-keepalive-newlines? false
   :headers                   {"Content-Disposition" (format "attachment; filename=\"%s_%s.pdf\""
                                                             card-name (u.date/format-date (t/zoned-date-time)))}})

(defn- add-header
  "Helper method to write the header"
  [columns]
  (into
    [:pdf-table
      {:width-percent 100
       :border-color [213 213 213]
       :background-color [56 117 172]
      }
      nil
    ]
    [(mapv (fn [name] [:pdf-cell {:align :center :valign :middle :padding-bottom 10 :font {:style :bold }} name]) columns)]
  ))

(defn- add-row
  "Helper method to write the dataset"
  [row]
  (into
    [:pdf-table
      {:width-percent 100
        :color [114 116 121]
        :border-color [213 213 213]
      }
      nil
    ]
    [(mapv (fn [element] [:pdf-cell {:padding-bottom 10} (str element) ]) row )]
  ))

(defn- export-to-pdf
  "Write a PDF to stream with the content in rows vector of sequences"
  [rows os]
  (pdf/pdf [
    {:size :a4
     :orientation :landscape
     :left-margin   10
     :right-margin  10
     :top-margin    15
     :bottom-margin 15
     :font {
        :size 9
        :family :sans-serif}}
    rows]
    os)
  )

;; TODO -- this is obviously not streaming! SAD!
(defmethod i/streaming-results-writer :pdf
  [_ ^OutputStream os]
  (let [rows (volatile! (vector))]
    (reify i/StreamingResultsWriter
      (begin! [_ {{:keys [cols]} :data}]
        (vswap! rows conj (add-header (map (some-fn :display_name :name) cols))))

      (write-row! [_ row _]
        (vswap! rows conj (add-row row)))

      (finish! [_ _]
        (export-to-pdf @rows os)
        (.flush os)))))
