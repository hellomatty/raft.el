;; -*- lexical-binding: t; -*-

(require 'raft)

(ert-deftest raft-rpc-accept-entries-test ()
  (let* ((log (list
                      (raft-make-log-entry 0 0 'cmd0)
                      (raft-make-log-entry 1 1 'cmd1)
                      (raft-make-log-entry 2 2 'cmd2)
                      (raft-make-log-entry 3 3 'cmd3)))
         (msg (raft-rpc-accept-entries (list
                :log log
                :peer-index-map (raft-peer-index-map-init (list
                                                           [127 0 0 1 8080] [127 0 0 1 8090]) 1))
                                       :peer [127 0 0 1 8090])))
    (should (= 1 (plist-get msg :index) ))
    (should (= 1 (plist-get msg :term)))
    (should (equal (plist-get msg :entries) (subseq log 2)))) )

(ert-deftest raft-rpc-accept-entries-cold-follower-test ()
  (let* ((log (list
                      (raft-make-log-entry 0 0 'cmd0)
                      (raft-make-log-entry 1 1 'cmd1)
                      (raft-make-log-entry 2 2 'cmd2)
                      (raft-make-log-entry 3 3 'cmd3)))
         (msg (raft-rpc-accept-entries (list
                :log log
                :peer-index-map (raft-peer-index-map-set (raft-peer-index-map-init (list
                                                                                    [127 0 0 1 8080] [127 0 0 1 8090]) 1)
                                                         [127 0 0 1 8090] nil))
                                       :peer [127 0 0 1 8090])))
    (should (null (plist-get msg :index) ))
    (should (null (plist-get msg :term)))
    (should (equal (plist-get msg :entries) log))))
