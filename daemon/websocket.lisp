(in-package :ouroboros)

;;; The previous version of this file called websocket-driver:make-server-driver
;;; on a raw usocket stream and drove it with a hand-rolled polling loop
;;; (wait-for-input + process-events). That's not how websocket-driver is
;;; meant to be used — it's designed to sit on top of Clack: the server
;;; function receives a Clack env (not a raw socket), and returns a closure
;;; that Clack calls with a responder to hand off an async connection. This
;;; rewrites the server as a small Clack app, matching the pattern from
;;; websocket-driver's own docs and the CL Cookbook's websocket tutorial
;;; (both of which run it under plain clackup / Hunchentoot — no extra
;;; evented-server dependency needed).

(defun make-ws-app (daemon)
  "Returns a Clack app: for each incoming HTTP request, upgrade it to a
   WebSocket connection and wire up message handling."
  (lambda (env)
    (let ((ws (wsd:make-server env)))
      (wsd:on :message ws
              (lambda (message)
                (handle-websocket-message daemon ws message)))
      (wsd:on :error ws
              (lambda (error)
                (format *error-output* "WebSocket error: ~a~%" error)))
      ;; websocket-driver apps hand back a closure that Clack invokes with
      ;; the async responder, rather than returning the connection result
      ;; directly — omitting this layer was a bug in the first draft.
      (lambda (responder)
        (declare (ignore responder))
        (wsd:start-connection ws)))))

(defun handle-websocket-message (daemon driver data)
  (let ((msg (json:decode-json-from-string data)))
    ;; cl-json decodes object keys as keyword symbols (:action, :profile,
    ;; :code, :text) by default, not strings.
    (let ((action (cdr (assoc :action msg))))
      (cond
        ((string= action "load_vibe")
         (let ((profile-name (cdr (assoc :profile msg))))
           (handler-case
               (progn
                 (load-profile daemon profile-name)
                 (wsd:send driver "{\"status\":\"ok\"}"))
             (error (e)
               (wsd:send driver (format nil "{\"error\":\"~a\"}" e))))))

        ((string= action "eval")
         (let ((code (cdr (assoc :code msg))))
           (handler-case
               (progn
                 (eval (read-from-string code))
                 (wsd:send driver "{\"status\":\"ok\"}"))
             (error (e)
               (wsd:send driver (format nil "{\"error\":\"~a\"}" e))))))

        ((string= action "prompt")
         (let ((text (cdr (assoc :text msg))))
           (multiple-value-bind (lisp-code error)
               (generate-lisp-from-prompt text)
             (if error
                 (wsd:send driver (format nil "{\"error\":\"~a\"}" error))
                 (handler-case
                     (progn
                       (eval (read-from-string lisp-code))
                       (wsd:send driver "{\"status\":\"ok\"}"))
                   (error (e)
                     (wsd:send driver (format nil "{\"error\":\"~a\"}" e))))))))

        (t
         (wsd:send driver "{\"error\":\"unknown action\"}"))))))

(defun start-websocket (daemon)
  "Starts the Clack app and stores the handle on the daemon so stop-daemon
   can shut it down cleanly via clack:stop."
  (setf (ws-server daemon)
        (clack:clackup (make-ws-app daemon) :port 8080)))
