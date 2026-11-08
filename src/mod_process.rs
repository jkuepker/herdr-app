//! Programs a mod keeps talking to (`spawn`): text into stdin, stdout by the
//! line, then the exit. Each copy of a mod owns its programs, and dropping the
//! copy stops them with everything they started.

use std::collections::HashMap;
use std::future::Future;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

/// Longest stdout line passed on whole; the rest of a longer line is dropped.
const LINE_MAX: usize = 8 << 20;
/// Lines read ahead of the mod. Past this the program waits on its stdout, so
/// a mod that stops reading can't fill memory.
const LINES_AHEAD: usize = 256;
/// The end of stderr kept for `processExited`.
const STDERR_TAIL: usize = 64 << 10;

const STDIN_CLOSED: &str = "the program's stdin is closed";

#[derive(Clone)]
pub struct Exit {
    /// None when a signal ended it.
    pub code: Option<i32>,
    pub stderr: String,
}

/// Bytes for stdin, and where to say they were written.
type Pending = (Vec<u8>, async_channel::Sender<Result<(), String>>);

struct Process {
    pid: u32,
    /// Set under its lock before the program is reaped: past that its pid may
    /// name someone else's process (group), so nothing signals it.
    gone: Arc<Mutex<bool>>,
    input: async_channel::Sender<Pending>,
    lines: async_channel::Receiver<String>,
    /// Closes once the program has exited and `exit` is set.
    exited: async_channel::Receiver<()>,
    exit: Arc<OnceLock<Exit>>,
}

impl Drop for Process {
    fn drop(&mut self) {
        stop(self.pid, &self.gone);
    }
}

/// One copy's programs, by id.
#[derive(Default)]
pub struct Processes {
    last: u64,
    live: HashMap<u64, Process>,
}

impl Processes {
    pub fn start(&mut self, command: &str, args: &[String], cwd: PathBuf) -> Result<u64, String> {
        let mut cmd = crate::spawn::command(command);
        cmd.args(args).current_dir(cwd).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped());
        #[cfg(unix)]
        std::os::unix::process::CommandExt::process_group(&mut cmd, 0);
        let mut child = cmd.spawn().map_err(|e| format!("cannot run {command}: {e}"))?;
        let pid = child.id();
        let mut stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let mut stderr = child.stderr.take().unwrap();

        let (input, writes) = async_channel::unbounded::<Pending>();
        let closer = writes.clone();
        std::thread::spawn(move || {
            while let Ok((bytes, done)) = writes.recv_blocking() {
                let r = stdin.write_all(&bytes).and_then(|()| stdin.flush()).map_err(|e| e.to_string());
                let failed = r.is_err();
                let _ = done.try_send(r);
                if failed { break; }
            }
            // Refuse later writes, and answer the ones already queued.
            writes.close();
            while let Ok((_, done)) = writes.try_recv() {
                let _ = done.try_send(Err(STDIN_CLOSED.into()));
            }
        });

        let (line_tx, lines) = async_channel::bounded(LINES_AHEAD);
        std::thread::spawn(move || {
            let mut out = BufReader::new(stdout);
            while let Ok(Some(line)) = next_line(&mut out) {
                if line_tx.send_blocking(line).is_err() { break; }
            }
        });

        let tail = Arc::new(Mutex::new(Vec::new()));
        let (drained_tx, drained) = std::sync::mpsc::channel::<()>();
        std::thread::spawn({
            let tail = tail.clone();
            move || {
                let mut buf = [0; 8192];
                while let Ok(n @ 1..) = stderr.read(&mut buf) {
                    let mut t = tail.lock().unwrap();
                    t.extend_from_slice(&buf[..n]);
                    let over = t.len().saturating_sub(STDERR_TAIL);
                    t.drain(..over);
                }
                drop(drained_tx);
            }
        });

        let gone = Arc::new(Mutex::new(false));
        let exit = Arc::new(OnceLock::new());
        let (exited_tx, exited) = async_channel::bounded::<()>(1);
        std::thread::spawn({
            let (gone, exit) = (gone.clone(), exit.clone());
            move || {
                #[cfg(unix)]
                {
                    wait_unreaped(pid);
                    let mut gone = gone.lock().unwrap();
                    // What it left running goes with it, while its group id is still its own.
                    unsafe { libc::killpg(pid as libc::pid_t, libc::SIGTERM) };
                    *gone = true;
                }
                let code = child.wait().ok().and_then(|s| s.code());
                // Windows keeps the pid while `child` holds its handle, dropped below.
                #[cfg(windows)]
                { *gone.lock().unwrap() = true; }
                // Something it started may hold stderr open; don't wait on that for long.
                let _ = drained.recv_timeout(Duration::from_millis(200));
                let stderr = String::from_utf8_lossy(&tail.lock().unwrap()).into_owned();
                let _ = exit.set(Exit { code, stderr });
                closer.close();
                drop(exited_tx);
            }
        });

        self.last += 1;
        self.live.insert(self.last, Process { pid, gone, input, lines, exited, exit });
        Ok(self.last)
    }

    fn get(&self, id: u64) -> Result<&Process, String> {
        self.live.get(&id).ok_or_else(|| format!("no process {id}"))
    }

    /// Resolves once the text is written.
    pub fn write(&self, id: u64, text: String) -> Result<impl Future<Output = Result<(), String>> + Send + use<>, String> {
        let (done, written) = async_channel::bounded(1);
        self.get(id)?.input.try_send((text.into_bytes(), done)).map_err(|_| STDIN_CLOSED.to_owned())?;
        Ok(async move { written.recv().await.unwrap_or_else(|_| Err(STDIN_CLOSED.into())) })
    }

    /// Closes stdin once what was written before reaches it.
    pub fn end(&self, id: u64) -> Result<(), String> {
        self.get(id)?.input.close();
        Ok(())
    }

    /// Resolves with the next stdout line, or None once stdout closes.
    pub fn next_line(&self, id: u64) -> Result<impl Future<Output = Option<String>> + Send + use<>, String> {
        let lines = self.get(id)?.lines.clone();
        Ok(async move { lines.recv().await.ok() })
    }

    pub fn exited(&self, id: u64) -> Result<impl Future<Output = Exit> + Send + use<>, String> {
        let p = self.get(id)?;
        let (exited, exit) = (p.exited.clone(), p.exit.clone());
        Ok(async move {
            let _ = exited.recv().await;
            exit.get().cloned().unwrap_or(Exit { code: None, stderr: String::new() })
        })
    }

    /// Stops it; a later call with the id fails.
    pub fn kill(&mut self, id: u64) {
        self.live.remove(&id);
    }

    pub fn kill_all(&mut self) {
        self.live.clear();
    }

    pub fn ids(&self) -> Vec<u64> {
        self.live.keys().copied().collect()
    }
}

/// The next line without its newline (or CRLF), at most LINE_MAX bytes of it;
/// None at the end.
fn next_line(r: &mut impl BufRead) -> std::io::Result<Option<String>> {
    let mut line = Vec::new();
    loop {
        let buf = r.fill_buf()?;
        if buf.is_empty() {
            return Ok((!line.is_empty()).then(|| String::from_utf8_lossy(&line).into_owned()));
        }
        let newline = buf.iter().position(|&b| b == b'\n');
        let part = &buf[..newline.unwrap_or(buf.len())];
        let room = LINE_MAX.saturating_sub(line.len());
        line.extend_from_slice(&part[..part.len().min(room)]);
        let used = newline.map_or(buf.len(), |i| i + 1);
        r.consume(used);
        if newline.is_some() {
            if line.last() == Some(&b'\r') { line.pop(); }
            return Ok(Some(String::from_utf8_lossy(&line).into_owned()));
        }
    }
}

/// Waits for the program to exit but leaves it unreaped, so its pid (and
/// group id) can't be reused while what it left running is stopped.
#[cfg(unix)]
fn wait_unreaped(pid: u32) {
    let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
    while unsafe { libc::waitid(libc::P_PID, pid as libc::id_t, &mut info, libc::WEXITED | libc::WNOWAIT) } == -1
        && std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted
    {}
}

/// Ends a program and what it started, unless it is gone: its process group
/// on Unix (TERM, then KILL), its process tree on Windows.
fn stop(pid: u32, gone: &Arc<Mutex<bool>>) {
    #[cfg(unix)]
    {
        let signal = move |gone: &Mutex<bool>, sig| {
            if !*gone.lock().unwrap() {
                unsafe { libc::killpg(pid as libc::pid_t, sig) };
            }
        };
        signal(gone, libc::SIGTERM);
        let gone = gone.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_secs(2));
            signal(&gone, libc::SIGKILL);
        });
    }
    #[cfg(windows)]
    {
        let gone = gone.clone();
        std::thread::spawn(move || {
            let gone = gone.lock().unwrap();
            if !*gone {
                let _ = crate::spawn::command("taskkill").args(["/PID", &pid.to_string(), "/T", "/F"]).stdout(Stdio::null()).stderr(Stdio::null()).status();
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exit_of(p: &Processes, id: u64) -> Exit {
        let process = p.get(id).unwrap();
        let _ = process.exited.recv_blocking();
        process.exit.get().cloned().unwrap()
    }

    #[test]
    fn lines_split_on_newlines_drop_crs_and_cut_long_lines() {
        let long = "x".repeat(LINE_MAX + 5);
        let text = format!("a\r\nb\n\n{long}\nlast");
        let mut r = BufReader::with_capacity(16, text.as_bytes());
        assert_eq!(next_line(&mut r).unwrap().as_deref(), Some("a"));
        assert_eq!(next_line(&mut r).unwrap().as_deref(), Some("b"));
        assert_eq!(next_line(&mut r).unwrap().as_deref(), Some(""));
        assert_eq!(next_line(&mut r).unwrap().map(|l| l.len()), Some(LINE_MAX));
        assert_eq!(next_line(&mut r).unwrap().as_deref(), Some("last"));
        assert_eq!(next_line(&mut r).unwrap(), None);
    }

    #[cfg(unix)]
    #[test]
    fn a_program_echoes_lines_until_stdin_ends() {
        let mut p = Processes::default();
        let id = p.start("cat", &[], std::env::temp_dir()).unwrap();
        let _ = p.write(id, "one\ntwo\n".into()).unwrap();
        let lines = p.get(id).unwrap().lines.clone();
        assert_eq!(lines.recv_blocking().unwrap(), "one");
        assert_eq!(lines.recv_blocking().unwrap(), "two");
        p.end(id).unwrap();
        assert!(lines.recv_blocking().is_err());
        assert_eq!(exit_of(&p, id).code, Some(0));
        assert!(p.write(id, "late\n".into()).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn the_exit_carries_the_code_and_the_end_of_stderr() {
        let mut p = Processes::default();
        let id = p.start("/bin/sh", &["-c".into(), "echo oops >&2; exit 3".into()], std::env::temp_dir()).unwrap();
        let exit = exit_of(&p, id);
        assert_eq!(exit.code, Some(3));
        assert_eq!(exit.stderr, "oops\n");
        assert!(p.start("/no/such/program", &[], std::env::temp_dir()).unwrap_err().contains("cannot run"));
    }

    #[cfg(unix)]
    fn wait_until_gone(pid: libc::pid_t) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while unsafe { libc::kill(pid, 0) } == 0 {
            assert!(std::time::Instant::now() < deadline, "{pid} outlived its program");
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    #[cfg(unix)]
    #[test]
    fn what_a_program_leaves_running_stops_when_it_exits_and_its_group_is_never_signalled_again() {
        let mut p = Processes::default();
        let id = p.start("/bin/sh", &["-c".into(), "sleep 30 & echo $!".into()], std::env::temp_dir()).unwrap();
        let leftover: libc::pid_t = p.get(id).unwrap().lines.recv_blocking().unwrap().parse().unwrap();
        assert_eq!(exit_of(&p, id).code, Some(0));
        assert!(*p.get(id).unwrap().gone.lock().unwrap());
        wait_until_gone(leftover);
    }

    #[cfg(unix)]
    #[test]
    fn dropping_the_copy_stops_what_its_programs_started() {
        let mut p = Processes::default();
        let id = p.start("/bin/sh", &["-c".into(), "sleep 30 & echo $!; wait".into()], std::env::temp_dir()).unwrap();
        let grandchild: libc::pid_t = p.get(id).unwrap().lines.recv_blocking().unwrap().parse().unwrap();
        drop(p);
        wait_until_gone(grandchild);
    }
}
