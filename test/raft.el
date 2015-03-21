;; -*- lexical-binding: t; -*-

(require 'raft)

(ert-deftest raft-quorum-test ()
  (should (= 2 (raft-quorum (list
                             :peer-index-map (raft-peer-index-map-init (list [127 0 0 1 8080] [127 0 0 1 8090] [127 0 0 1 8100]) nil))))))


(ert-deftest raft-election-winner-test ()
  (should (not (raft-election-winnerp (list
                                       :peer-index-map  (raft-peer-index-map-init (list [127 0 0 1 8080] [127 0 0 1 8090] [127 0 0 1 8100]) nil)
                                       :votes (list [127 0 0 1 8080])))))

  (should (raft-election-winnerp (list
                                  :peer-index-map (raft-peer-index-map-init (list [127 0 0 1 8080] [127 0 0 1 8090] [127 0 0 1 8100]) nil)
                                  :votes (list [127 0 0 1 8080] [127 0 0 1 8090])))))

(ert-deftest raft-peers-test ()
  (let ((peers (raft-peers (list
                            :peer-index-map (raft-peer-index-map-init (list [127 0 0 1 8080] [127 0 0 1 8090] [127 0 0 1 8100]) nil)))))
    (should (not (member [127 0 0 1 8080] peers)))
    (should (member [127 0 0 1 8090] peers))
    (should (member [127 0 0 1 8100] peers))))

(ert-deftest raft-local-test ()
  (should (equal [127 0 0 1 8080] (raft-local (list
                                               :peer-index-map (raft-peer-index-map-init (list [127 0 0 1 8080] [127 0 -0 1 8090] [127 0 0 1 8100]) nil))))))

(ert-deftest raft-log-consistent-empty-test ()
  (should (raft-log-consistentp nil nil nil))
  (should (not (raft-log-consistentp nil 0 0))))

(ert-deftest raft-log-consistent-test ()
  (let ((log (list
              '((:index . 0) (:term . 0) (:command . 'fixture)))))
    (should (raft-log-consistentp log 0 0))
    (should (not (raft-log-consistentp log nil nil)))))

(ert-deftest raft-make-log-entry-test ()
  (let ((entry (raft-make-log-entry 10 20 'command)))
    (should (= 10 (cdr (assoc :index entry))))
    (should (= 20 (cdr (assoc :term entry))))
    (should (eql 'command (cdr (assoc :command entry))))))
(ert-deftest raft-peer-index-map-test ()
  (let ((map (raft-peer-index-map-init (list
                                        [127 0 0 1 8080] [127 0 0 1 8090]  [127 0 0 1 8100]) 3)))
    (should (eql nil (raft-peer-index-map-get map [127 0 0 1 8070])))
    (should (= 3 (raft-peer-index-map-get map [127 0 0 1 8080])))
    (should (= 3 (raft-peer-index-map-get map [127 0 0 1 8090])))
    (should (= 3 (raft-peer-index-map-get map [127 0 0 1 8100]))))

  (let ((map (raft-peer-index-map-init (list [127 0 0 1 8080]) nil)))
    (should (= 3 (raft-peer-index-map-get
                  (raft-peer-index-map-set
                   map
                   [127 0 0 1 8080]
                   3)
                  [127 0 0 1 8080])))
    (should (eql nil (raft-peer-index-map-get map [127 0 0 1 8080])))))

(ert-deftest raft-quorate-index-test ()
  (should (= 1 (raft-quorate-committed-index (list
                                              :peer-index-map
                (raft-peer-index-map-set 
                 (raft-peer-index-map-init (list
                                            [127 0 0 1 8080] [127 0 0 1 8090] [127 0 0 1 8100]) 1)
                 [127 0 0 1 8080] 0)))))
  
  (should (= 0 (raft-quorate-committed-index (list
                                              :peer-index-map
                (raft-peer-index-map-set 
                 (raft-peer-index-map-init (list
                                            [127 0 0 1 8080] [127 0 0 1 8090] [127 0 0 1 8100]) 0)
                 [127 0 0 1 8080] 1)))))

  (should (eql nil (raft-quorate-committed-index (list
                                                  :peer-index-map
                    (raft-peer-index-map-init (list
                                               [127 0 0 1 8080] [127 0 0 1 8090] [127 0 0 1 8100]) nil))))))

(ert-deftest raft-subseq-test ()
(let ((l (list 0 1 2 3)))
  (should (eql nil (raft-subseq l nil nil)))
  (should (equal (list 0) (raft-subseq l nil 0)))
  (should (equal (list 0 1 2) (raft-subseq l nil 2)))
  (should (eql nil (raft-subseq l 0 0)))
  (should (equal (list 1 2) (raft-subseq l 0 2)))))
