use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use tokio::sync::Mutex;

use super::{Backend, BackendStatus, Role};
use crate::command::{Command, Outcome};
use crate::error::{Error, Result};
use crate::state::IdState;

const STATE_FILE: &str = "state.json";
const TMP_FILE: &str = "state.json.tmp";
const LOCK_FILE: &str = "LOCK";

/// Durable single-node backend.
///
/// Every command is applied to a copy of the state, which is written to
/// `state.json.tmp`, fsync'd, atomically renamed over `state.json`, and the
/// directory fsync'd before the command is acknowledged. The state is a few
/// bytes per namespace and is written once per leased block, so rewriting it
/// whole is cheap and avoids log compaction entirely.
///
/// An exclusive OS file lock on `LOCK` prevents two processes from serving
/// from the same directory (which would hand out duplicate IDs).
pub struct FileBackend {
    dir: PathBuf,
    state: Mutex<IdState>,
    _lock: File,
}

impl FileBackend {
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(dir.join(LOCK_FILE))?;
        // Contention (`WouldBlock`) is by far the likeliest failure, hence the
        // hint; the underlying reason is kept for the rare other cases.
        lock.try_lock().map_err(|e| {
            Error::Storage(format!(
                "cannot lock {}, is another quorid process using it? ({e})",
                dir.display()
            ))
        })?;

        let state = match fs::read(dir.join(STATE_FILE)) {
            Ok(bytes) => IdState::restore(&bytes)?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => IdState::new(),
            Err(e) => return Err(e.into()),
        };
        // Leftover from a crash mid-write; the renamed state.json is authoritative.
        let _ = fs::remove_file(dir.join(TMP_FILE));

        Ok(FileBackend {
            dir,
            state: Mutex::new(state),
            _lock: lock,
        })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub async fn state(&self) -> IdState {
        self.state.lock().await.clone()
    }
}

/// Each step runs only once the previous one succeeded; the temporary file is
/// closed before the rename.
fn persist(dir: &Path, bytes: &[u8]) -> io::Result<()> {
    let tmp = dir.join(TMP_FILE);
    File::create(&tmp)
        .and_then(|mut file| file.write_all(bytes).and_then(|()| file.sync_all()))
        .and_then(|()| fs::rename(&tmp, dir.join(STATE_FILE)))
        .and_then(|()| File::open(dir))
        .and_then(|dir| dir.sync_all())
}

impl Backend for FileBackend {
    async fn execute(&self, command: Command) -> Result<Outcome> {
        // Held across the write: commands are serialized, and memory only
        // advances once the new state is on disk.
        let mut state = self.state.lock().await;
        let mut next = state.clone();
        let outcome = next.apply(&command)?;

        let dir = self.dir.clone();
        let bytes = next.snapshot();
        tokio::task::spawn_blocking(move || persist(&dir, &bytes))
            .await
            .map_err(Error::from)
            .and_then(|persisted| persisted.map_err(Error::from))?;

        *state = next;
        Ok(outcome)
    }

    fn status(&self) -> BackendStatus {
        BackendStatus {
            kind: "file",
            role: Role::Standalone,
            durable: true,
            node_id: None,
            leader: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::{IdRange, Namespace};

    fn reserve(count: u64) -> Command {
        Command::Reserve {
            namespace: Namespace::new("users").unwrap(),
            count,
        }
    }

    #[tokio::test]
    async fn survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        {
            let backend = FileBackend::open(dir.path()).unwrap();
            assert_eq!(
                backend.execute(reserve(100)).await.unwrap(),
                Outcome::Reserved(IdRange::new(1, 101))
            );
        }
        let backend = FileBackend::open(dir.path()).unwrap();
        assert_eq!(
            backend.execute(reserve(10)).await.unwrap(),
            Outcome::Reserved(IdRange::new(101, 111))
        );
        assert!(!dir.path().join(TMP_FILE).exists());
    }

    #[tokio::test]
    async fn rejected_commands_are_not_persisted() {
        let dir = tempfile::tempdir().unwrap();
        let backend = FileBackend::open(dir.path()).unwrap();
        assert!(backend.execute(reserve(0)).await.is_err());
        assert!(!dir.path().join(STATE_FILE).exists());
    }

    #[test]
    fn directory_is_exclusive() {
        let dir = tempfile::tempdir().unwrap();
        let first = FileBackend::open(dir.path()).unwrap();
        let err = FileBackend::open(dir.path())
            .err()
            .expect("second open must fail");
        assert!(
            err.to_string()
                .contains("is another quorid process using it?"),
            "{err}"
        );
        drop(first);
        FileBackend::open(dir.path()).unwrap();
    }

    fn open_error(dir: &Path) -> String {
        FileBackend::open(dir)
            .err()
            .expect("open must fail")
            .to_string()
    }

    #[test]
    fn corrupt_state_is_reported() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join(STATE_FILE), b"garbage").unwrap();
        let err = open_error(dir.path());
        assert!(err.starts_with("storage error: corrupt snapshot"), "{err}");
    }

    // Directories stand in for files below: opening or reading them fails
    // with an I/O error other than NotFound.

    #[test]
    fn unusable_dir_is_reported() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("not-a-dir");
        fs::write(&file, b"").unwrap();
        let err = open_error(&file);
        assert!(err.starts_with("storage error: "), "{err}");
    }

    #[test]
    fn unreadable_state_is_reported() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir(dir.path().join(STATE_FILE)).unwrap();
        let err = open_error(dir.path());
        assert!(err.starts_with("storage error: "), "{err}");
    }

    #[test]
    fn unopenable_lock_is_reported() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir(dir.path().join(LOCK_FILE)).unwrap();
        let err = open_error(dir.path());
        assert!(err.starts_with("storage error: "), "{err}");
    }

    #[tokio::test]
    async fn failed_writes_are_not_acknowledged() {
        let dir = tempfile::tempdir().unwrap();
        let backend = FileBackend::open(dir.path()).unwrap();
        let users = Namespace::new("users").unwrap();
        backend.execute(reserve(5)).await.unwrap();

        fs::create_dir(dir.path().join(TMP_FILE)).unwrap();
        let err = backend.execute(reserve(5)).await.unwrap_err().to_string();
        assert!(err.starts_with("storage error: "), "{err}");
        assert_eq!(backend.state().await.next_id(&users), 6);

        fs::remove_dir(dir.path().join(TMP_FILE)).unwrap();
        assert_eq!(
            backend.execute(reserve(5)).await.unwrap(),
            Outcome::Reserved(IdRange::new(6, 11))
        );
    }

    #[tokio::test]
    async fn accessors_and_status() {
        let dir = tempfile::tempdir().unwrap();
        let backend = FileBackend::open(dir.path()).unwrap();
        assert_eq!(backend.dir(), dir.path());
        backend.execute(reserve(5)).await.unwrap();
        assert_eq!(
            backend
                .state()
                .await
                .next_id(&Namespace::new("users").unwrap()),
            6
        );

        let status = backend.status();
        assert_eq!(status.kind, "file");
        assert_eq!(status.role, Role::Standalone);
        assert!(status.durable);
        assert_eq!(status.node_id, None);
        assert_eq!(status.leader, None);
    }
}
