(require 'raft)

(when (get-buffer raft-debug-buffer)
  (kill-buffer raft-debug-buffer))

(setq raft-election-interval 10
      raft-heartbeat-interval 1
      raft-debug-buffer "*raft-debug*")

(setq my-group (list
                [127 0 0 1 8080]
                [127 0 0 1 8090]
                [127 0 0 1 8100]))

(setq my-procs (mapcar (lambda (local)
                         (let ((proc (make-network-process
                                      :server t
                                      :type 'datagram
                                      :local local
                                      :name "raft-server"
                                      :filter t)))
                           (raft-process-init proc
                                              :committer (lambda (command)
                                                           (message (format "command %s" command)))
                                              :group my-group)
                           (process-name proc)))
                       my-group))

(run-at-time 10 nil (lambda ()
                      (dolist (proc my-procs)
                        (let ((command "hello"))
                          (raft-apply (get-process proc) (list :command command))))))

(run-at-time 15 nil (lambda ()
                      (mapc 'delete-process my-procs)))
