#![allow(unused)]
#[cfg(target_os = "macos")]
mod darwin;
mod unix;

use std::process::Stdio;

pub use self::unix::Command;

/// A token that is used to detect if a process has been escalated.
///
/// If the child process spits this out on stdout that means we escalated successfully.
pub const SUCCESS_TOKEN: &'static str = "znxbnvm,,xbnzcvnmxzv,.,,,";

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Could not become root! Searched for sudo, doas, su")]
    UnixNotDetected,

    #[cfg(target_os = "macos")]
    #[error("User failed to confirm")]
    MacOSDenial,
}

pub async fn run_escalate(
    cmd: &Command<'_>,
    modify: impl FnOnce(&mut tokio::process::Command) -> (),
) -> anyhow::Result<tokio::process::Child> {
    use self::unix::EscalationMethod;

    let mut cmd: tokio::process::Command = EscalationMethod::detect()?.wrap_command(cmd).into();
    cmd.stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    modify(&mut cmd);

    let proc = cmd.spawn()?;
    let stderr = proc.stdout.take().unwrap();
    let stdin = proc.stdin.take().unwrap();

    tokio::spawn(async move {
        let stdout = proc.stdout.take().unwrap();
    });
    Ok()
}

struct EscalationInProgress {
    _child: tokio::process::Child,
    listener: JoinHandle<bool>,
}

impl EscalationInProgress {
    pub fn await_escalation(self) -> anyhow::Result<()> {
        self.listener.await?;
        Ok(())
    }
}
