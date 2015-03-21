;; -*- lexical-binding: t; -*-

(require 'raft)

(ert-deftest raft-leader-client-command-test ()
  (let ((next-state (first (raft-role-leader (list
                                        :term 1
                                        :log (list
                                              (raft-make-log-entry 0 0 'fixture)))
                                       (list :command 'fixture2)))))
    (should (= 2 (length (plist-get next-state :log))))
    (should (equal (raft-make-log-entry 1 1 'fixture2) (second (plist-get next-state :log))))
    (should (equal (raft-make-log-entry 1 1 'fixture2) (second (plist-get next-state :log))))))
 
(ert-deftest raft-leader-accept-entries-response-test ()
  (let ((next-state (first (raft-role-leader (list
                                              :log (list
                                                    (raft-make-log-entry 0 0 'command))
                                              :peer-index-map (raft-peer-index-map-init (list
                                                                                         [127 0 0 1 8080] [127 0 0 1 8090] [127 0 0 1 8100]) nil))
                                             (list
                                              :rpc 'accept-entries-response :from [127 0 0 1 8090] :committed 0)))))
    (should (eql nil (raft-peer-index-map-get (plist-get next-state :peer-index-map) [127 0 0 1 8080])))
    (should (= 0 (raft-peer-index-map-get (plist-get next-state :peer-index-map) [127 0 0 1 8090])))
    (should (eql nil (raft-peer-index-map-get (plist-get next-state :peer-index-map) [127 0 0 1 8100])))))

(ert-deftest raft-leader-accept-entries-response-quorum-commit-test ()
  (let ((next-state (first (raft-role-leader (list
                                              :log (list
                                                    (raft-make-log-entry 0 0 'cmd1)
                                                    (raft-make-log-entry 1 1 'cmd1))
                                              :peer-index-map (raft-peer-index-map-set (raft-peer-index-map-init (list
                                                                                                                  [127 0 0 1 8080] [127 0 0 1 8090] [127 0 0 1 8100]) 0)
                                                                                       [127 0 0 1 8100] 1))
                                             (list
                                              :rpc 'accept-entries-response
                                              :from [127 0 0 1 8090]
                                              :committed 1)))))
    (should (= 1 (raft-peer-index-map-get (plist-get next-state :peer-index-map) [127 0 0 1 8080])))))

(ert-deftest raft-leader-accept-entries-response-rejected-test ()
  (let ((next-state (first (raft-role-leader (list
                                              :log (list
                                                    (raft-make-log-entry 0 0 'command))
                                              :peer-index-map (raft-peer-index-map-init (list
                                                                                         [127 0 0 1 8080] [127 0 0 1 8090] [127 0 0 1 8100]) 3))
                                             (list
                                              :rpc 'accept-entries-response :from [127 0 0 1 8090] :rejected)))))
    (should (= 2 (raft-peer-index-map-get (plist-get next-state :peer-index-map) [127 0 0 1 8090])))))



(ert-deftest raft-leader-accept-entries-response-rejected-empty-test ()
  (let ((next-state (first (raft-role-leader (list
                                              :log (list
                                                    (raft-make-log-entry 0 0 'command))
                                              :peer-index-map (raft-peer-index-map-init (list
                                                                                         [127 0 0 1 8080] [127 0 0 1 8090] [127 0 0 1 8100]) 0))
                                             (list
                                              :rpc 'accept-entries-response :from [127 0 0 1 8090] :rejected)))))
    (should (eql nil (raft-peer-index-map-get (plist-get next-state :peer-index-map) [127 0 0 1 8090])))))

