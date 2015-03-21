;; -*- lexical-binding: t; -*-

(require 'raft-rpc)

(defun raft-role-election-reset (state msg)
  (let ((tick (if (plist-member state :election-clock)
                  (1+ (plist-get state :election-clock))
                0))
        (due (raft-election-duration)))
    (list
     (plist-put
      (plist-put
       (copy-sequence state)
       :role
       'raft-role-follower)
      :election-clock
      tick)
     (list "election due in %s secs" due)
     (lambda (proc)
       (run-at-time due nil 'raft-apply proc (list :election 'start :election-clock tick))))))

(defun raft-role-follower (state msg)
  (cond
   ((eql 'reset (plist-get msg :election))
    (raft-role-election-reset state msg))
   ((and
     (eql 'start (plist-get msg :election))
     (>= (plist-get msg :election-clock) (plist-get state :election-clock)))
    (let ((ends (raft-election-duration))
          (tmp (plist-put
                (plist-put
                 (plist-put
                  (copy-sequence state)
                  :role
                  'raft-role-candidate)
                 :votes
                 nil)
                :term
                (1+ (plist-get state :term)))))
      (cons
       tmp
       (append
        (mapcar (lambda (peer)
                  (list 'raft-tx peer (raft-rpc-request-vote tmp)))
                (raft-peers state))
        (list
         (list "election started - over in %s secs" ends)
         (lambda (proc)
           (run-at-time ends nil 'raft-apply proc (list :election 'reset))))))))
   ((and
     (eql 'accept-entries (plist-get msg :rpc))
     (raft-log-consistentp (plist-get state :log) (plist-get msg :index) (plist-get msg :term)))
    (let* ((log (append (plist-get state :log) (plist-get msg :entries)))
          (tmp (plist-put
                (plist-put
                (plist-put
                 (copy-sequence state)
                 :leader
                 (or (plist-get state :leader) (plist-get msg :from)))
                :log
                log)
                :peer-index-map
                (raft-peer-index-map-set
                 (plist-get state :peer-index-map)
                 (raft-local state)
                 (cdr (assoc :index (car (last log))))))))
      (append
      (list
       tmp
       (list 'raft-tx (plist-get msg :from) (raft-rpc-accept-entries-response tmp))
       (list 'raft-apply (list :election 'reset)))
      (mapcar (lambda (command)
                (list 'raft-commit command))
              (mapcar (lambda (entry)
                        (cdr (assoc :command entry)))
                      (plist-get msg :entries)))
      (list
       (list 'raft-persist)))))
   ((eql 'accept-entries (plist-get msg :rpc))
    (list
     state
     (list 'raft-tx (plist-get msg :from) (raft-rpc-accept-entries-response state :rejected))))
   ((and
     (eql 'request-vote (plist-get msg :rpc))
     (>= (plist-get msg :term) (plist-get state :term))
     (null (plist-get state :voted-for)))
    (let ((tmp (plist-put
                       (plist-put
                        (copy-sequence state)
                        :term
                        (plist-get msg :term))
                       :voted-for
                       (plist-get msg :from))))
        (list
         tmp
         (list 'raft-tx (plist-get msg :from) (raft-rpc-request-vote-response tmp))
         (list 'raft-persist))))
   ((plist-member msg :command)
    (list
     state
     (list "redirect to leader %s" (plist-get state :leader))
     (list 'raft-tx (plist-get msg :from) (raft-rpc-redirect-client state))))
   (t
    (list state))))

(defun raft-role-candidate (state msg)
  (cond
   ((eql 'reset (plist-get msg :election))
    (raft-role-election-reset state msg))
   ;; ((and (eql 'election-stop (plist-get msg :timer))
   ;;       (= (plist-get msg :term) (plist-get state :term)))
   ;;  (list
   ;;   (plist-put (copy-sequence state) :role 'raft-role-follower)
   ;;   (lambda (proc)
   ;;     (run-at-time (raft-election-duration) nil 'raft-apply proc (list :timer 'election-start)))))
   ((and
     (eql 'request-vote-response (plist-get msg :rpc))
     (<= (plist-get state :term) (plist-get msg :term))
     (not (member (plist-get msg :from) (plist-get state :votes))))
      (let ((tmp (plist-put
                  (copy-sequence state)
                  :votes
                  (cons (plist-get msg :from) (plist-get state :votes)))))
        (if (raft-election-winnerp tmp)
            (list
             (plist-put
              tmp
              :role
              'raft-role-leader)
             (list "election winner")
             (list 'raft-apply (list :heartbeat))
             (list 'raft-persist))
          (list
           tmp
           (list 'raft-persist)))))
   (t
    (list state))))

(defvar raft-heartbeat-interval 0.1
  "Interval between leader sending accept-entries heartbeat rpcs, in seconds.")

(defun raft-role-leader (state msg)
  (cond
   ((plist-member msg :heartbeat)
    (append
     (list
      state
      (list "broadcasting heartbeats")
      (lambda (proc)
        (run-at-time 1 nil 'raft-apply proc (list :heartbeat))))
     (mapcar (lambda (peer)
               (list 'raft-tx peer (raft-rpc-accept-entries state :peer peer)))
             (raft-peers state))))
   ((plist-member msg :command)
    (let ((tmp (plist-put
                (copy-sequence state)
                :log
                (append
                 (plist-get state :log)
                 (list (raft-make-log-entry
                        (length (plist-get state :log))
                        (plist-get state :term)
                        (plist-get msg :command)))))))
      (append
       (list
        tmp
        (list "command %s" (plist-get msg :command))
        (list 'raft-persist))
       (mapcar (lambda (peer)
                 (list 'raft-tx peer (raft-rpc-accept-entries tmp :peer peer)))
               (raft-peers state)))))
   ((and
     (eql 'accept-entries-response (plist-get msg :rpc))
     (plist-member msg :rejected))
    (let* ((state-peer-index (raft-peer-index-map-get
                              (plist-get state :peer-index-map)
                              (plist-get msg :from)))
           (next-state-peer-index (when (and
                                         (not (null state-peer-index))
                                         (> state-peer-index 0))
                                    (1- state-peer-index)))
           (tmp (plist-put
                        (copy-sequence state)
                        :peer-index-map
                        (raft-peer-index-map-set
                         (plist-get state :peer-index-map)
                         (plist-get msg :from)
                         next-state-peer-index))))
      (list
       tmp
       (list 'raft-tx (plist-get msg :from) (raft-rpc-accept-entries tmp))
       (list 'raft-persist))))
   ((eql 'accept-entries-response (plist-get msg :rpc))
    (let* ((tmp (plist-put
                (copy-sequence state)
                :peer-index-map
                (raft-peer-index-map-set
                (plist-get state :peer-index-map)
                (plist-get msg :from)
                (plist-get msg :committed))))
           (quorum-commit-index (raft-quorate-committed-index tmp)))
      (cons
       (plist-put
        tmp
        :peer-index-map
        (raft-peer-index-map-set
         (plist-get tmp :peer-index-map)
         (raft-local state)
         quorum-commit-index))
       (append
       (mapcar (lambda (command)
                 (list 'raft-commit command))
               (mapcar (lambda (entry)
                         (cdr (assoc :command entry)))
                       (raft-subseq
                        (plist-get state :log)
             (raft-peer-index-map-get
              (plist-get state :peer-index-map)
              (raft-local state))
             quorum-commit-index)))
       (list
        (list 'raft-persist))))))
   (t
   (list state))))

(provide 'raft-role)
