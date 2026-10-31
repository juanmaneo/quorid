//! Repository automation: `cargo xtask <task>`.
//!
//! The cargo-xtask pattern keeps project tasks in plain, dependency-free
//! Rust instead of ad-hoc shell scripts. The aliases in `.cargo/config.toml`
//! expose them as cargo subcommands, e.g. `cargo coverage`.

use std::process::{Command, ExitCode};

const USAGE: &str = "\
usage: cargo xtask <task> [args]

tasks:
  coverage [--fail-under <PCT>] [--fail-under-regions <PCT>] [--html] [flags]
      Run the test suite under a coverage tool and print the report:
      cargo-llvm-cov when installed, otherwise cargo-tarpaulin.
      --fail-under exits non-zero below PCT percent line coverage, and
      --fail-under-regions below PCT percent region coverage (llvm-cov only).
      --html also writes an HTML report under target/, and any other flags
      are passed through to the tool, e.g. `--show-missing-lines`.
";

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("coverage") => coverage(&args.collect::<Vec<_>>()),
        Some("help" | "-h" | "--help") => {
            eprint!("{USAGE}");
            ExitCode::SUCCESS
        }
        Some(task) => {
            eprintln!("error: unknown task `{task}`\n");
            eprint!("{USAGE}");
            ExitCode::FAILURE
        }
        None => {
            eprint!("{USAGE}");
            ExitCode::SUCCESS
        }
    }
}

/// Measures test coverage of the product code; `xtask` itself is excluded, it
/// is automation rather than product.
fn coverage(args: &[String]) -> ExitCode {
    let opts = match parse_coverage_args(args) {
        Ok(opts) => opts,
        Err(msg) => {
            eprintln!("error: {msg}");
            return ExitCode::FAILURE;
        }
    };

    if has_subcommand("llvm-cov") {
        let mut cmd = Command::new("cargo");
        cmd.args([
            "llvm-cov",
            "--workspace",
            "--exclude",
            "xtask",
            "--all-targets",
        ]);
        if let Some(pct) = opts.fail_under {
            cmd.arg("--fail-under-lines").arg(pct.to_string());
        }
        if let Some(pct) = opts.fail_under_regions {
            cmd.arg("--fail-under-regions").arg(pct.to_string());
        }
        if opts.html {
            cmd.arg("--html");
        }
        return finish(
            run(cmd.args(&opts.forwarded)),
            opts.html,
            "target/llvm-cov/html/index.html",
        );
    }

    if has_subcommand("tarpaulin") {
        if opts.fail_under_regions.is_some() {
            eprintln!("warning: tarpaulin does not measure regions, ignoring --fail-under-regions");
        }
        let mut cmd = Command::new("cargo");
        cmd.args(["tarpaulin", "--workspace"]);
        if let Some(pct) = opts.fail_under {
            cmd.arg("--fail-under").arg(pct.to_string());
        }
        if opts.html {
            cmd.args(["--out", "Html"]);
        }
        return finish(
            run(cmd.args(&opts.forwarded)),
            opts.html,
            "target/tarpaulin/tarpaulin-report.html",
        );
    }

    eprintln!(
        "error: no coverage tool found; install one of:
  cargo install cargo-llvm-cov   # recommended
  cargo install cargo-tarpaulin"
    );
    ExitCode::FAILURE
}

#[derive(Debug, Default)]
struct CoverageOptions {
    /// Minimum line coverage percentage, enforced after the run.
    fail_under: Option<f64>,
    /// Minimum region coverage percentage, enforced after the run.
    fail_under_regions: Option<f64>,
    /// Also write an HTML report.
    html: bool,
    /// Flags the task does not know about, passed through to the tool.
    forwarded: Vec<String>,
}

fn parse_coverage_args(args: &[String]) -> Result<CoverageOptions, String> {
    let mut opts = CoverageOptions::default();
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--html" => opts.html = true,
            flag @ ("--fail-under" | "--fail-under-regions") => {
                let pct = parse_percentage(flag, args.get(i + 1))?;
                if flag == "--fail-under" {
                    opts.fail_under = Some(pct);
                } else {
                    opts.fail_under_regions = Some(pct);
                }
                i += 1;
            }
            flag => opts.forwarded.push(flag.to_string()),
        }
        i += 1;
    }
    Ok(opts)
}

fn parse_percentage(flag: &str, value: Option<&String>) -> Result<f64, String> {
    match value.map(|v| v.parse::<f64>()) {
        Some(Ok(pct)) if (0.0..=100.0).contains(&pct) => Ok(pct),
        _ => Err(format!("{flag} wants a percentage 0-100, e.g. {flag} 90")),
    }
}

/// True if the cargo subcommand is installed. The probe's output is captured,
/// so a missing subcommand does not print cargo's own error message.
fn has_subcommand(sub: &str) -> bool {
    Command::new("cargo")
        .args([sub, "--version"])
        .output()
        .is_ok_and(|out| out.status.success())
}

/// Runs a child process to completion, letting its output flow through.
fn run(cmd: &mut Command) -> bool {
    match cmd.status() {
        Ok(status) => status.success(),
        Err(e) => {
            eprintln!("error: failed to run {cmd:?}: {e}");
            false
        }
    }
}

fn finish(ok: bool, html: bool, report: &str) -> ExitCode {
    if !ok {
        return ExitCode::FAILURE;
    }
    if html {
        eprintln!("html report: {report}");
    }
    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn parses_threshold_and_html() {
        let opts = parse_coverage_args(&args(&["--fail-under", "90", "--html"])).unwrap();
        assert_eq!(opts.fail_under, Some(90.0));
        assert!(opts.html);
    }

    #[test]
    fn rejects_bad_thresholds() {
        assert!(parse_coverage_args(&args(&["--fail-under", "101"])).is_err());
        assert!(parse_coverage_args(&args(&["--fail-under", "abc"])).is_err());
        assert!(parse_coverage_args(&args(&["--fail-under"])).is_err());
        assert_eq!(
            parse_coverage_args(&args(&["--fail-under-regions", "-1"])).unwrap_err(),
            "--fail-under-regions wants a percentage 0-100, e.g. --fail-under-regions 90"
        );
    }

    #[test]
    fn parses_region_threshold_and_last_value_wins() {
        let opts = parse_coverage_args(&args(&[
            "--fail-under",
            "100",
            "--fail-under-regions",
            "100",
            "--fail-under",
            "80",
        ]))
        .unwrap();
        assert_eq!(opts.fail_under, Some(80.0));
        assert_eq!(opts.fail_under_regions, Some(100.0));
        assert!(opts.forwarded.is_empty());
    }

    #[test]
    fn forwards_unknown_flags() {
        let opts =
            parse_coverage_args(&args(&["--show-missing-lines", "--fail-under", "80"])).unwrap();
        assert_eq!(opts.fail_under, Some(80.0));
        assert_eq!(opts.forwarded, ["--show-missing-lines"]);
    }
}
