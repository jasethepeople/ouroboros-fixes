use crate::graph::Graph;
use crate::nodes::*;
use crate::unix_socket::UnixSocketServer;
use atomic_float::AtomicF32;
use crossbeam_channel::{bounded, Receiver};
use pipewire as pw;
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

pub struct AudioEngine {
    // Only ever written by the control thread, only ever read by the RT thread.
    current_graph: Arc<AtomicPtr<Graph>>,
    pub transport: Arc<Transport>,
}

pub struct Transport {
    pub bpm: AtomicF32,
    pub frame_counter: AtomicU64,
    pub bar_counter: AtomicU64,
}

impl AudioEngine {
    pub fn new() -> Self {
        let (cmd_tx, cmd_rx) = bounded(1024);
        let current_graph: Arc<AtomicPtr<Graph>> = Arc::new(AtomicPtr::new(ptr::null_mut()));
        let transport = Arc::new(Transport {
            bpm: AtomicF32::new(120.0),
            frame_counter: AtomicU64::new(0),
            bar_counter: AtomicU64::new(0),
        });

        // Socket listener: just parses framed lines off the wire and forwards
        // them as strings. No graph/audio work happens on this thread either.
        thread::spawn(move || {
            UnixSocketServer::run("/tmp/ouroboros.sock", cmd_tx);
        });

        // Control thread: owns ALL graph construction (heap allocation,
        // HashMap/Vec growth, etc). This used to happen inline inside the
        // PipeWire process callback, which is a real-time thread — doing
        // allocation there risks underruns every time a vibe loads. Now the
        // RT thread only ever does a lock-free atomic pointer load/swap.
        let graph_ptr_for_control = current_graph.clone();
        let transport_for_control = transport.clone();
        thread::spawn(move || {
            Self::control_loop(cmd_rx, graph_ptr_for_control, transport_for_control);
        });

        Self {
            current_graph,
            transport,
        }
    }

    fn control_loop(
        cmd_rx: Receiver<String>,
        current_graph: Arc<AtomicPtr<Graph>>,
        transport: Arc<Transport>,
    ) {
        let mut staging: Option<Graph> = None;
        while let Ok(cmd) = cmd_rx.recv() {
            Self::handle_command(&cmd, &mut staging, &current_graph, &transport);
        }
    }

    fn handle_command(
        cmd: &str,
        staging: &mut Option<Graph>,
        current_graph: &Arc<AtomicPtr<Graph>>,
        transport: &Arc<Transport>,
    ) {
        let value: serde_json::Value = match serde_json::from_str(cmd) {
            Ok(v) => v,
            Err(_) => {
                eprintln!("Invalid JSON: {}", cmd);
                return;
            }
        };
        let method = value["method"].as_str().unwrap_or("");

        match method {
            "clear_graph" => {
                *staging = Some(Graph::new());
            }

            "create_node" => {
                let node_type = value["params"]["type"].as_str().unwrap_or("");
                let name = value["params"]["name"].as_str().unwrap_or("");
                if staging.is_none() {
                    *staging = Some(Graph::new());
                }
                if let Some(graph) = staging.as_mut() {
                    // NOTE: these strings must match the underscored node-type
                    // strings the Lisp daemon sends (see node-type-string in
                    // the daemon). "modulated_delay" was previously named
                    // "ping_pong_delay" here, which never matched anything
                    // the daemon actually sent — every vibe silently failed
                    // to build a single node.
                    let node: Box<dyn AudioNode> = match node_type {
                        "wavetable_osc" => Box::new(WavetableOscillator::new()),
                        "wavefolder" => Box::new(Wavefolder::new()),
                        "sv_filter" => Box::new(SVFilter::new()),
                        "modulated_delay" => Box::new(ModulatedDelay::new()),
                        "noise_gen" => Box::new(NoiseGen::new()),
                        "distortion" => Box::new(Distortion::new()),
                        "compressor" => Box::new(Compressor::new()),
                        "plate_reverb" => Box::new(PlateReverb::new()),
                        "pass_through" => Box::new(PassThrough::new()),
                        _ => {
                            eprintln!("Unknown node type: {}", node_type);
                            return;
                        }
                    };
                    graph.add_node(name.to_string(), node);
                }
            }

            "connect" => {
                // Still a no-op: nodes process serially in insertion order.
            }

            "set_param" => {
                let node_name = value["params"]["node"].as_str().unwrap_or("");
                let param = value["params"]["param"].as_str().unwrap_or("");
                let val = value["params"]["value"].as_f64().unwrap_or(0.0) as f32;
                if let Some(graph) = staging.as_mut() {
                    graph.set_param(node_name, param, val);
                }
            }

            "set_bpm" => {
                let bpm = value["params"]["bpm"].as_f64().unwrap_or(120.0) as f32;
                transport.bpm.store(bpm, Ordering::Relaxed);
            }

            "swap" => {
                if let Some(new_graph) = staging.take() {
                    let old_ptr = current_graph.swap(
                        Box::into_raw(Box::new(new_graph)),
                        Ordering::Release,
                    );
                    if !old_ptr.is_null() {
                        // Free the old graph off the RT thread.
                        let old_box = unsafe { Box::from_raw(old_ptr) };
                        thread::spawn(move || drop(old_box));
                    }
                }
            }

            _ => eprintln!("Unknown command: {}", method),
        }
    }

    pub fn run(&self) {
        pw::init();
        let mainloop = pw::MainLoop::new(None).expect("Failed to create main loop");
        let context = pw::Context::new(&mainloop).expect("Failed to create context");

        let stream = pw::stream::Stream::new(
            &context,
            "ouroboros_out",
            pw::properties! {
                *pw::keys::MEDIA_TYPE => "Audio",
                *pw::keys::MEDIA_CATEGORY => "Playback",
                *pw::keys::MEDIA_ROLE => "Music",
            },
        )
        .expect("Failed to create stream");

        let target = pw::properties! {
            *pw::keys::NODE_TARGET => "default",
        };

        // NOTE: stream format negotiation (AudioInfoRaw -> Pod) needs to be
        // rebuilt against the actual pipewire-rs API you're pinned to; the
        // original VideoInfo::new_audio(...) call doesn't exist and was
        // never going to compile. Flagging rather than guessing an API here
        // — check the pipewire-rs version in Cargo.lock and use its
        // format_utils / audio format-building helpers.
        let mut params = spa::param::audio::AudioInfoRaw::new();
        params.set_format(spa::param::audio::AudioFormat::F32LE);
        params.set_rate(48000);
        params.set_channels(2);

        let engine_ptr = self as *const Self as *mut libc::c_void;
        stream.set_process_callback(Some(Self::process_callback), engine_ptr);
        stream.set_error_callback(Some(Self::error_callback), ptr::null_mut());

        mainloop.run();
    }

    // Real-time thread: no locks, no allocation, no command parsing.
    // Just load a pointer and process.
    extern "C" fn process_callback(
        stream: &pw::stream::Stream,
        user_data: *mut libc::c_void,
    ) -> isize {
        let engine = unsafe { &*(user_data as *const AudioEngine) };
        let buffer = match stream.dequeue_buffer() {
            Some(b) => b,
            None => return 0,
        };
        let datas = buffer.datas();
        if datas.is_empty() {
            return 0;
        }
        let data = &datas[0];
        let n_frames = data.chunk.size() as usize / (std::mem::size_of::<f32>() * 2);
        if n_frames == 0 {
            return 0;
        }

        let ptr = data.chunk.data() as *mut f32;
        let samples = unsafe { std::slice::from_raw_parts_mut(ptr, n_frames * 2) };

        let graph_ptr = engine.current_graph.load(Ordering::Acquire);
        if !graph_ptr.is_null() {
            let graph = unsafe { &mut *graph_ptr };
            graph.process_clear(samples, n_frames);
        } else {
            samples.fill(0.0);
        }

        engine
            .transport
            .frame_counter
            .fetch_add(n_frames as u64, Ordering::Relaxed);
        let bpm = engine.transport.bpm.load(Ordering::Relaxed);
        let frames_per_bar = (60.0 / bpm as f64) * 48000.0 * 4.0;
        let total_frames = engine.transport.frame_counter.load(Ordering::Relaxed);
        let bars = (total_frames as f64 / frames_per_bar) as u64;
        engine.transport.bar_counter.store(bars, Ordering::Relaxed);

        stream.queue_buffer(buffer);
        0
    }

    extern "C" fn error_callback(_stream: &pw::stream::Stream, seq: i32, res: i32, msg: &str) {
        eprintln!("PipeWire error: {} (seq {}, res {})", msg, seq, res);
    }
}
