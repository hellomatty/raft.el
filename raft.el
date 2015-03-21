;; -*- lexical-binding: t; -*-

(require 'raft-role)

(defun raft-quorum (state)
  (1+ (/ (length (plist-get state :peer-index-map)) 2)))

(defun raft-election-winnerp (state)
  (>=
   (length (plist-get state :votes))
   (raft-quorum state)))

(defun raft-peers (state)
  (rest (mapcar 'car (plist-get state :peer-index-map))))

(defun raft-local (state)
  (first (mapcar 'car (plist-get state :peer-index-map))))

(defun raft-tx (proc to msg)
  (raft-debug proc "tx %s %s" to msg)
  (when (and to msg)
    (set-process-datagram-address proc to)
  (process-send-string proc (prin1-to-string msg))))

(defvar raft-election-interval 0.5
  "Upper bound for time between elections, in seconds.")

(defun raft-election-duration ()
  (let ((i (/ (truncate (* 1000 raft-election-interval)) 2)))
    (/ (+ i (random i)) 1000.0)))

;; TODO assertion should only consider the tail of the log 
(defun raft-log-consistentp (log index term)
  (if index
      (eql term (cdr (assoc :term (elt log index))))
    (and (null log) (null term))))

(defun raft-make-log-entry (index term command)
  (list
   (cons :index index)
   (cons :term term)
   (cons :command command)))

(defun raft-peer-index-map-init (group commit-index)
  (loop
   for peer in group
   collect (cons peer commit-index)))


(defun raft-peer-index-map-get (map peer)
  (cdr (assoc peer map)))

(defun raft-peer-index-map-set (map peer index)
  (let ((map-dash (copy-tree map)))
  (setcdr (assoc peer map-dash) index)
  map-dash))

(defun raft-peer-index-map-count (m)
  (let ((indices (remove-if (apply-partially 'eql nil)
                          (mapcar 'cdr m))))
    (when indices
      (mapcar (lambda (index)
          (cons index (length (remove-if (apply-partially '> index) indices))))
          (number-sequence
          (apply 'min indices)
          (apply 'max indices))))))

(defun raft-quorate-committed-index (state)
  (let ((l (raft-peer-index-map-count (plist-get state :peer-index-map))))
    (when l
           (loop
            for index-count in l
     while (>= (cdr index-count) (raft-quorum state))
     maximize (car index-count)))))

(defun raft-commit (proc command)
  (raft-debug proc "commit: %s" command)
  (let ((fn (plist-get (process-plist proc) :committer)))
    (when fn
      (funcall fn command))))

(defun raft-subseq (l lo hi)
  "LIke subseq, but the lower bound is exclusive, and the upper is inclusive. An Upper bound of nil retruns an empty list."
  (when (numberp hi)
   (subseq
   l
  (if (numberp lo)
      (1+ lo)
0)
  (if (numberp hi)
      (1+ hi)
nil))))

(defun raft-persist (proc)
  (raft-debug proc "persist process state"))

(defun raft-process-init (proc &rest opts)
  (set-process-plist proc (list
                           :committer (plist-get opts :committer)
                           :term 0
                           :role 'raft-role-follower
                           :peer-index-map (raft-peer-index-map-init
                                            (cons
                                             (process-contact proc :local)
                                             (remove-if (apply-partially 'equal (process-contact proc :local)) (plist-get opts :group)))
                                            nil)))  
  (set-process-filter proc (lambda (p dgram)
                             (raft-debug p "rx %s" dgram)
                             (raft-apply p (read dgram))))
  (set-process-sentinel proc (lambda (p e)
                               (raft-debug p "sentinel: %s" e)))
  (raft-apply proc (list :election 'reset)))

(defvar raft-debug-buffer "*raft-debug*")

(defun raft-debug (proc fmt &rest args)
  (condition-case err
  (when raft-debug-buffer
    (let ((state (process-plist proc))
          (b (get-buffer-create raft-debug-buffer)))
      (princ (format "%s\t%s\t%s\t"
                     (raft-local state)
                     (plist-get state :term)
                     (cond
                      ((eql 'raft-role-leader (plist-get state :role)) "l")
                      ((eql 'raft-role-candidate (plist-get state :role)) "c")
                      ((eql 'raft-role-follower (plist-get state :role)) "f")
                      (t "?"))) b)
      (princ (apply 'format (cons fmt args)) b)
      (princ "\n" b)))
  (error (princ (format "broken logging %s %s\n" fmt args)))))

(defun raft-apply (proc msg)
  (when (eql 'open (process-status proc))
    (let ((next-state (apply (process-get proc :role) (list (process-plist proc) msg))))
      (set-process-plist proc (first next-state))
      (dolist (side-effect (rest next-state))
        (cond
         ((functionp side-effect)
          (funcall side-effect proc))
         ((and
           (consp side-effect)
           (symbolp (first side-effect)))
          (apply (first side-effect) (cons proc (rest side-effect))))
         ((and
           (consp side-effect)
           (stringp (first side-effect)))
          (apply 'raft-debug (cons proc side-effect))))))))

(provide 'raft)
