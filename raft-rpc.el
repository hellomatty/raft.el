;; -*- lexical-binding: t; -*-

(defun raft-rpc-request-vote (state)
  (list
   :from (raft-local state)
   :rpc 'request-vote
   :term (plist-get state :term)))

(defun raft-rpc-request-vote-response (state)
  (list
   :from (raft-local state)
   :rpc 'request-vote-response
   :term (plist-get state :term)
   :voted-for (plist-get state :voted-for)))

(defun raft-rpc-accept-entries (state &rest opts)
  (let ((peer-index (raft-peer-index-map-get (plist-get state :peer-index-map) (plist-get opts :peer))))
    (if (numberp peer-index)
        (let ((last-committed-entry (elt (plist-get state :log) peer-index)))
          (list
           :from (raft-local state)
           :rpc 'accept-entries
           :term (cdr (assoc :term last-committed-entry))
           :index (cdr (assoc :index last-committed-entry))
           :entries (subseq (plist-get state :log) (1+ (cdr (assoc :index last-committed-entry))))))
      (list
       :from (raft-local state)
       :rpc 'accept-entries
       :index nil
       :term nil
       :entries (plist-get state :log)))))

(defun raft-rpc-accept-entries-response (state &optional opts)
  (append
   (list
    :from (raft-local state)
    :rpc 'accept-entries-response)
   (if (plist-member opts :rejected)
       (list :rejected t)
     (list
      :committed (raft-peer-index-map-get (plist-get state :peer-index-map) (raft-local state))))))

(defun raft-rpc-redirect-client (state)
  (list
   :from (raft-local state)
   :leader (plist-get state :leader)))

(provide 'raft-rpc)
