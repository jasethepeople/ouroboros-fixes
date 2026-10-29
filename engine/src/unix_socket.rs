use crossbeam_channel::Sender;
use std::io::{BufRead, BufReader};
use std::os::unix::net::UnixListener;
use std::thread;

pub struct UnixSocketServer;

impl UnixSocketServer {
    /// Accepts connections and forwards newline-delimited JSON commands to
    /// `cmd_tx`. One command per line — the daemon must terminate each
    /// command it sends with '\n'. This fixes two problems in the previous
    /// version: it used nix APIs that don't match nix 0.27's actual
    /// signatures (accept() returns a single RawFd, not a tuple; there is no
    /// SockaddrUnix type — it's UnixAddr), and it had no message framing, so
    /// a single read() could contain half a command or several concatenated
    /// commands.
    pub fn run(path: &str, cmd_tx: Sender<String>) {
        let _ = std::fs::remove_file(path);
        let listener = UnixListener::bind(path).expect("Failed to bind unix socket");

        for stream in listener.incoming() {
            let stream = match stream {
                Ok(s) => s,
                Err(_) => continue,
            };
            let cmd_tx = cmd_tx.clone();
            thread::spawn(move || {
                let reader = BufReader::new(stream);
                for line in reader.lines() {
                    match line {
                        Ok(cmd) => {
                            let cmd = cmd.trim();
                            if !cmd.is_empty() {
                                let _ = cmd_tx.send(cmd.to_string());
                            }
                        }
                        Err(_) => break,
                    }
                }
            });
        }
    }
}
