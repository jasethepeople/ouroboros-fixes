;;; Load dependencies
;;; clack replaces the old hand-rolled usocket accept loop for the WebSocket
;;; server — see websocket.lisp for why. usocket is still used below only
;;; for the Unix-domain socket connection to the Rust engine.
(ql:quickload '(:usocket :clack :websocket-driver :json :cl-ppcre :bordeaux-threads :drakma :babel))

(defpackage :ouroboros
  (:use :cl)
  (:export :main))

(in-package :ouroboros)

;;; Forward declarations for submodules
(load "vibe.lisp")
(load "websocket.lisp")
(load "ai.lisp")

;;; Main daemon class
(defclass daemon ()
  ((engine-socket :accessor engine-socket :initform nil)
   (profiles :accessor profiles :initform (make-hash-table :test 'equal))
   (active-vibe :accessor active-vibe :initform nil)
   (running-p :accessor running-p :initform t)
   (frame-counter :accessor frame-counter :initform 0)
   (bpm :accessor bpm :initform 120)
   (automation-thread :accessor automation-thread :initform nil)
   (ws-server :accessor ws-server :initform nil) ; holds the clack:clackup handle
   (socket-path :accessor socket-path :initform "/tmp/ouroboros.sock")))

(defun make-daemon ()
  (make-instance 'daemon))

;;; Engine communication
(defun connect-engine (path)
  (usocket:socket-connect path :element-type 'octet))

(defun send-command (socket command)
  ;; Terminate every command with a newline: the Rust-side socket server
  ;; reads newline-delimited lines, so commands sent back-to-back without a
  ;; delimiter could be concatenated or split across reads on the other end.
  (let* ((json-string (json:encode-json-to-string command))
         (line (concatenate 'string json-string (string #\Newline)))
         (bytes (babel:string-to-octets line :encoding :utf-8)))
    (usocket:socket-send socket bytes (length bytes))))

(defun set-param (socket node param value)
  (send-command socket `(("method" . "set_param")
                         ("params" . ((("node" . ,node)
                                      ("param" . ,param)
                                      ("value" . ,value)))))))

;;; The Rust engine matches node types as underscored snake_case strings
;;; ("wavetable_osc", "sv_filter", "modulated_delay", ...). Profile routing
;;; forms use ordinary hyphenated Lisp symbols ('wavetable-osc, 'sv-filter,
;;; ...). Converting here means any hyphenated symbol name lines up with its
;;; engine-side counterpart automatically, instead of every profile silently
;;; producing "Unknown node type" on the Rust side.
(defun node-type-string (symbol)
  (substitute #\_ #\- (string-downcase (symbol-name symbol))))

;;; Profile loading
(defun load-profile (daemon profile-name)
  (let ((file (format nil "profiles/~a.lisp" profile-name)))
    (load file)
    (let ((vibe (symbol-value (intern (string-upcase profile-name) :ouroboros.vibes))))
      (apply-vibe daemon vibe))))

(defun apply-vibe (daemon vibe)
  (setf (active-vibe daemon) vibe)
  (let ((socket (engine-socket daemon))
        (node-names '()))
    (send-command socket `(("method" . "clear_graph")))

    (dolist (entry (vibe-routing vibe))
      (destructuring-bind (node-type . args) entry
        (let ((node-name (format nil "~a_~d" (node-type-string node-type) (random 10000))))
          (send-command socket
                        `(("method" . "create_node")
                          ("params" . ((("type" . ,(node-type-string node-type))
                                       ("name" . ,node-name))))))
          ;; Apply inline parameters declared right in the routing form, e.g.
          ;; (wavetable-osc :morph 0.3) -> set_param node "morph" 0.3.
          ;; Previously these were destructured into `args` and never used.
          (loop for (param value) on args by #'cddr
                when (numberp value)
                  do (set-param socket node-name (string-downcase (symbol-name param)) value))
          (push node-name node-names))))
    (setf node-names (nreverse node-names))

    (loop for (src dst) on node-names
          while dst
          do (send-command socket
                           `(("method" . "connect")
                             ("params" . ((("src" . ,src)
                                          ("dst" . ,dst)))))))

    (loop for (node-name . params-alist) in (vibe-parameters vibe)
          do (loop for (param . value) in params-alist
                   do (set-param socket node-name (string-downcase (symbol-name param)) value)))

    (when (vibe-bpm vibe)
      (setf (bpm daemon) (vibe-bpm vibe))
      (send-command socket
                    `(("method" . "set_bpm")
                      ("params" . ((("bpm" . ,(vibe-bpm vibe))))))))

    (send-command socket `(("method" . "swap")))))

;;; Automation loop
(defun start-automation (daemon)
  (when (automation-thread daemon)
    (bt:destroy-thread (automation-thread daemon)))
  (setf (automation-thread daemon)
        (bt:make-thread
         (lambda ()
           (loop while (running-p daemon)
                 do (sleep 0.01)
                    (let ((vibe (active-vibe daemon)))
                      (when (and vibe (vibe-automation vibe))
                        (funcall (vibe-automation vibe)
                                 (engine-socket daemon)
                                 (incf (frame-counter daemon))))))
         :name "Automation")))

;;; Main entry point
(defun main ()
  (let* ((daemon (make-daemon))
         (socket (connect-engine (socket-path daemon))))
    (setf (engine-socket daemon) socket)
    (start-automation daemon)
    (start-websocket daemon)
    (format t "Ouroboros daemon running. WebSocket on port 8080~%")
    (loop while (running-p daemon)
          do (sleep 1))))

(defun stop-daemon (daemon)
  (setf (running-p daemon) nil)
  (when (ws-server daemon)
    (clack:stop (ws-server daemon)))
  (when (usocket:socket-open-p (engine-socket daemon))
    (usocket:socket-close (engine-socket daemon))))

(sb-ext:save-lisp-and-die "ouroboros-daemon" :executable t :toplevel 'main)
