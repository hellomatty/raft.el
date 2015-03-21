;; -*- lexical-binding: t; -*-


(require 'raft-role)

(ert-deftest raft-follower-request-vote-previous-term-test ()
  (let ((next-state (first (raft-role-follower
                            (list
                             :term 1
                             :voted-for nil)
                            '(:rpc request-vote
                                   :from [127 0 0 1 8090]
                                   :term 0)))))
    (should (= 1 (plist-get next-state :term)))
    (should (eql nil (plist-get next-state :voted-for)))))


(ert-deftest raft-follower-request-vote-vote-test ()
  (let ((next-state (first (raft-role-follower
                            (list
                             :term 1
                             :voted-for nil)
                            '(:rpc request-vote
                                   :from [127 0 0 1 8090]
                                   :term 1)))))
    (should (= 1 (plist-get next-state :term)))
    (should (equal [127 0 0 1 8090] (plist-get next-state :voted-for)))))


(ert-deftest raft-follower-request-vote-only-once-test ()
  (let ((next-state (raft-role-follower
                     (list
                      :term 1
                      :voted-for [127 0 0 1 8090])
                     '(:rpc request-vote
                            :from [127 0 0 1 8100]
                            :term 1))))
    (should (= 1 (plist-get (first next-state) :term)))
    (should (equal [127 0 0 1 8090] (plist-get (first next-state) :voted-for)))
    (should (= 0 (length (rest next-state))))))

(ert-deftest raft-follower-request-vote-future-term ()
  (let ((next-state (first (raft-role-follower
                            (list
                             :term 0
                             :voted-for nil)
                            '(:rpc request-vote
                                   :from [127 0 0 1 8090]
                                   :term 1)))))
    (should (= 1 (plist-get next-state :term)))
    (should (equal [127 0 0 1 8090] (plist-get next-state :voted-for)))))

(ert-deftest raft-follower-accept-entries-consistent-log-empty-test ()
  (let ((next-state (first (raft-role-follower (list
                                                :log nil
                                                :peer-index-map (raft-peer-index-map-init (list
                                                                                           [127 0 01 8080] [127 0 0 1 80890] [127 0 0 1 8100]) nil)
                                                :leader nil)
                                               (list
                                                :from [127 0 0 1 8080]
                                                :rpc 'accept-entries
                                                :index nil
                                                :term nil
                                                :entries
                                                (list
                                                 '(:index 0 :term 0 :command 'test)))))))
    (should (null (raft-peer-index-map-get (plist-get next-state :peer-index-map) (raft-local next-state))))
    (should (= 1 (length (plist-get next-state :log))))
    (should (equal '(:index 0 :term 0 :command 'test) (first (plist-get next-state :log))))
    (should (equal [127 0 0 1 8080] (plist-get next-state :leader)))))

(ert-deftest raft-follower-accept-entries-consistent-log-test ()
  (let ((next-state (first (raft-role-follower (list
                                                :log (list
                                                      '((:index . 0) (:term . 0) (:command . 'test)))
                                                :peer-index-map (raft-peer-index-map-init (list
                                                                                           [127 0 0 1 8080] [127 0 0 1 8090] [127 0 0 1 8100]) nil)
                                                :leader nil)
                                               (list
                                                :from [127 0 0 1 8080]
                                                :rpc 'accept-entries
                                                :index 0
                                                :term 0
                                                :entries
                                                (list
                                                 '((:index . 1) (:term . 2) (:command . 'test2))))))))
    (should (= 1 (raft-peer-index-map-get (plist-get next-state :peer-index-map) (raft-local next-state))))
    (should (= 2 (length (plist-get next-state :log))))
    (should (equal '((:index . 0) (:term . 0) (:command . 'test)) (first (plist-get next-state :log))))
    (should (equal '((:index . 1) (:term . 2) (:command . 'test2)) (second (plist-get next-state :log))))
    (should (equal [127 0 0 1 8080] (plist-get next-state :leader)))))


(ert-deftest raft-follower-accept-entries-inconsistent-log-test ()
  (let ((next-state (first (raft-role-follower (list
                                                :log nil
                                                :leader nil)
                                               (list
                                                :from [127 0 0 1 8080]
                                                :rpc 'accept-entries
                                                :index 2
                                                :term 3
                                                :entries
                                                (list
                                                 '((:index . 3) (:term . 5) (:command . 'test))))))))
    (should (null (plist-get next-state :log)))
    (should (null (plist-get next-state :leader)))))


(ert-deftest raft-follower-election-reset-test ()
  (let* ((clock 0)
         (next-state (first (raft-role-follower (list
                                        :election-clock clock
                                        :role 'raft-role-follower)
                                       (list
                                        :election 'reset)))))
    (should (> (plist-get next-state :election-clock) clock))
    (should (eql 'raft-role-follower (plist-get next-state :role)))))
    
(ert-deftest raft-follower-election-start-test ()
  (let* ((clock 0)
         (term 0)
         (next-state (first (raft-role-follower (list
                                                 :election-clock clock
                                                 :term term
                                        :role 'raft-role-follower)
                                       (list
                                        :election 'start :election-clock clock)))))
    (should (= clock (plist-get next-state :election-clock)))
    (should (eql 'raft-role-candidate (plist-get next-state :role)))
    (should (= (1+ term) (plist-get next-state :term)))))

(ert-deftest raft-follower-election-start-ignorable-test ()
  (let* ((clock 0)
         (next-state (first (raft-role-follower (list
                                        :election-clock (1+ clock)
                                        :role 'raft-role-follower)
                                       (list
                                        :election 'start :election-clock clock)))))
    (should (= (1+ clock) (plist-get next-state :election-clock)))
    (should (eql 'raft-role-follower (plist-get next-state :role)))))

(ert-deftest raft-follower-client-command-test ()
  (let* ((state (list
                                                :leader [127 0 0 1 8080]))
         (next-state (first (raft-role-follower state
                                               (list :command 'fixture)))))
  (should (equal state next-state))))
