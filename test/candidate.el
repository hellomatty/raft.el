;; -*- lexical-binding: t; -*-

(require 'raft)

(ert-deftest raft-candidate-request-vote-response-previous-term-test ()
  (let ((next-state (first (raft-role-candidate
                            (list
                             :term 1
                             :votes nil)
                            (list
                             :rpc 'request-vote-response
                             :from [127 0 0 1 8080]
                             :term 0)))))
    (should (= 1 (plist-get next-state :term)))
    (should (= 0 (length (plist-get next-state :votes))))))

(ert-deftest raft-candidate-request-vote-response-test ()
  (let ((next-state (first (raft-role-candidate
                            (list
                             :role 'raft-role-candidate
                             :peer-index-map (raft-peer-index-map-init (list [127 0 0 1 8080] [127 0 0 1 8090] [127 0 0 1 8090]) nil)
                             :term 1
                             :votes nil)
                            (list
                             :rpc 'request-vote-response
                             :from [127 0 0 1 8080]
                             :term 1)))))
    (should (= 1 (plist-get next-state :term)))
    (should (= 1 (length (plist-get next-state :votes))))
    (should (member [127 0 0 1 8080] (plist-get next-state :votes)))
    (should (not (raft-election-winnerp next-state)))
    (should (eql 'raft-role-candidate (plist-get next-state :role)))))

(ert-deftest raft-candidate-request-vote-response-duplicate ()
  (let ((next-state (first (raft-role-candidate
                            (list
                             :role 'raft-role-candidate
                             :peer-index-map (raft-peer-index-map-init (list [127 0 0 1 8080] [127 0 0 1 8090] [127 0 0 1 8090]) nil)
                             :term 1
                             :votes (list [127 0 0 1 8080]))
                            (list
                             :rpc 'request-vote-response
                             :from [127 0 0 1 8080]
                             :term 1)))))
    (should (= 1 (plist-get next-state :term)))
    (should (= 1 (length (plist-get next-state :votes))))
    (should (member [127 0 0 1 8080] (plist-get next-state :votes)))
    (should (eql 'raft-role-candidate (plist-get next-state :role)))))

(ert-deftest raft-candidate-request-vote-response-quorum-vote-test ()
  (let ((next-state (first (raft-role-candidate
                            (list
                             :role 'raft-role-candidate
                             :peer-index-map (raft-peer-index-map-init (list [127 0 0 1 8080] [127 0 0 1 8090] [127 0 0 1 8090]) nil)
                             :term 1
                             :votes (list [127 0 0 1 8080]))
                            (list
                             :rpc 'request-vote-response
                             :from [127 0 0 1 8090]
                             :term 1)))))
    (should (= 1 (plist-get next-state :term)))
    (should (= 2 (length (plist-get next-state :votes))))
    (should (member [127 0 0 1 8080] (plist-get next-state :votes)))
    (should (member [127 0 0 1 8090] (plist-get next-state :votes)))
    (should (raft-election-winnerp next-state))
    (should (eql 'raft-role-leader (plist-get next-state :role)))))


(ert-deftest raft-candidate-election-reset-test ()
  (let* ((clock 0)
         (next-state (first (raft-role-candidate (list
                                        :election-clock clock
                                        :role 'raft-role-candidate)
                                       (list
                                        :election 'reset)))))
    (should (> (plist-get next-state :election-clock) clock))
    (should (eql 'raft-role-follower (plist-get next-state :role)))))
    

