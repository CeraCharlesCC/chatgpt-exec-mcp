//! Optional build-summary excerpts. Unknown commands keep ordinary head/tail.
//! This is a display hint, never an execution/permission or success policy.

use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;

use crate::output_store::{Projection, read_utf8_safe_head, read_utf8_safe_tail};

const SCAN_BYTES: u64 = 64 * 1024;
const MAX_LINE_BYTES: usize = 512;
const MAX_EXCERPTS: usize = 3;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BuildOutput {
    Rustc,
    Cargo,
    Gradle,
}

impl BuildOutput {
    pub(crate) const DEFAULT_MAX_OUTPUT_TOKENS: usize = 2_000;

    pub(crate) fn for_command(command: &str) -> Option<Self> {
        // Deliberately not a shell parser. We only recognize simple command
        // lists joined by &&, ||, ;, or newlines. Quoting, expansion, pipes,
        // redirects and grouping fall back to ordinary head/tail.
        if command.contains([
            '\r', '\'', '"', '\\', '$', '`', '<', '>', '(', ')', '{', '}',
        ]) {
            return None;
        }

        let commands = command
            .replace("&&", "\n")
            .replace("||", "\n")
            .replace(';', "\n");
        if commands.contains(['&', '|']) {
            return None;
        }

        let mut build = None;
        for command in commands
            .lines()
            .map(str::trim)
            .filter(|command| !command.is_empty())
        {
            if let Some(kind) = Self::for_simple_command(command) {
                if build.is_some_and(|existing| existing != kind) {
                    return None;
                }
                build = Some(kind);
            } else if !is_setup_command(command) {
                return None;
            }
        }
        build
    }

    fn for_simple_command(command: &str) -> Option<Self> {
        let mut words = command.split_whitespace();
        let mut program = words.next()?;
        if program == "exec" {
            program = words.next()?;
        }
        match Path::new(program).file_name()?.to_str()? {
            "rustc" => Some(Self::Rustc),
            "gradle" | "gradlew" => Some(Self::Gradle),
            "cargo" if matches!(words.next(), Some("build" | "check" | "test")) => {
                Some(Self::Cargo)
            }
            _ => None,
        }
    }

    fn is_summary(self, line: &str) -> bool {
        match self {
            Self::Rustc => {
                line.starts_with("error: aborting due to ")
                    || (line.starts_with("warning: ") && line.ends_with(" emitted"))
            }
            Self::Cargo => {
                (line.starts_with("Finished ") && line.contains(" target(s) in "))
                    || line.starts_with("test result: ok. ")
                    || line.starts_with("test result: FAILED. ")
                    || (line.starts_with("error: could not compile `") && line.contains(" due to "))
            }
            Self::Gradle => {
                line.starts_with("BUILD SUCCESSFUL in ")
                    || line.starts_with("BUILD FAILED in ")
                    || line == "FAILURE: Build failed with an exception."
                    || line
                        .split_once(" actionable task")
                        .is_some_and(|(count, rest)| {
                            !count.is_empty()
                                && count.bytes().all(|byte| byte.is_ascii_digit())
                                && (rest.starts_with(": ") || rest.starts_with("s: "))
                        })
            }
        }
    }
}

fn is_setup_command(command: &str) -> bool {
    matches!(
        command.split_whitespace().next(),
        Some("cd" | "pushd" | "popd" | "export" | "unset" | "set" | "umask" | "true")
    )
}

struct Excerpt {
    start: u64,
    end: u64,
    bytes: Vec<u8>,
}

impl Excerpt {
    fn label(&self) -> String {
        format!(
            "\n[build summary excerpt; bytes {}..{}]\n",
            self.start, self.end
        )
    }
}

// Recognize ordinary SGR-colored lines, but copy their original bytes into
// the response. Other controls, invalid UTF-8 and oversized lines are skipped.
fn summary_text(bytes: &[u8]) -> Option<String> {
    if bytes.len() > MAX_LINE_BYTES {
        return None;
    }
    let text = std::str::from_utf8(bytes)
        .ok()?
        .trim_end_matches(['\r', '\n']);
    let mut chars = text.chars().peekable();
    let mut plain = String::new();
    while let Some(ch) = chars.next() {
        if ch == '\x1b' {
            if chars.next()? != '[' {
                return None;
            }
            while chars
                .peek()
                .is_some_and(|ch| ch.is_ascii_digit() || *ch == ';')
            {
                chars.next();
            }
            if chars.next()? != 'm' {
                return None;
            }
        } else if ch.is_control() && ch != '\t' {
            return None;
        } else {
            plain.push(ch);
        }
    }
    Some(plain.trim().to_owned())
}

fn omitted(bytes: u64) -> String {
    format!("\n... {bytes} bytes omitted ...\n")
}

pub(crate) fn add_build_summary(
    mut baseline: Projection,
    kind: Option<BuildOutput>,
    budget: usize,
) -> Projection {
    if let Some(kind) = kind
        && baseline.truncated
        && !baseline.encoding_loss
        && budget >= 512
        && let Ok(Some(output)) = summary_projection(&baseline, kind, budget)
    {
        baseline.output = output;
    }
    // Optional scan/read/recognition failure does not invalidate a successful
    // head/tail read. Capture status and cursor are owned by the caller.
    baseline
}

fn summary_projection(
    baseline: &Projection,
    kind: BuildOutput,
    budget: usize,
) -> io::Result<Option<String>> {
    let snapshot = &baseline.snapshot;
    let range = snapshot.end - snapshot.start;
    let scan_start = snapshot.end - range.min(SCAN_BYTES);
    let mut file = File::open(&snapshot.path)?;
    file.seek(SeekFrom::Start(scan_start))?;
    let mut scan = Vec::new();
    Read::by_ref(&mut file)
        .take(range.min(SCAN_BYTES))
        .read_to_end(&mut scan)?;
    let marker_cost = omitted(range).len();
    let mut candidates = Vec::new();
    let mut offset = scan_start;
    for line in scan.split_inclusive(|byte| *byte == b'\n') {
        let start = offset;
        offset += line.len() as u64;
        // Never interpret a partial first/last line at a scan or poll boundary.
        if (start == scan_start && scan_start != 0) || !line.ends_with(b"\n") {
            continue;
        }
        if let Some(text) = summary_text(line)
            && kind.is_summary(&text)
            && !baseline
                .output
                .contains(std::str::from_utf8(line).expect("validated UTF-8"))
        {
            candidates.push(Excerpt {
                start,
                end: offset,
                bytes: line.to_vec(),
            });
        }
    }

    // At most 20% (and 1 KiB) for excerpts plus their labels/extra markers.
    // Prefer the latest fitting lines; preserve source order in the response.
    let mut allowance = (budget / 5).min(1024);
    let mut selected = Vec::new();
    for candidate in candidates.into_iter().rev() {
        let cost = candidate.bytes.len() + candidate.label().len() + marker_cost;
        if cost <= allowance {
            allowance -= cost;
            selected.push(candidate);
            if selected.len() == MAX_EXCERPTS {
                break;
            }
        }
    }
    if selected.is_empty() {
        return Ok(None);
    }
    selected.reverse();
    let used = (budget / 5).min(1024) - allowance;
    let content_budget = budget - used - marker_cost;
    let head = read_utf8_safe_head(&mut file, snapshot.start, range, content_budget / 2)?;
    let tail = read_utf8_safe_tail(
        &mut file,
        snapshot.start,
        snapshot.end,
        content_budget - content_budget / 2,
    )?;
    let tail_start = snapshot.end - tail.len() as u64;
    let mut cursor = snapshot.start + head.len() as u64;
    let mut bytes = head;
    for excerpt in selected {
        if excerpt.start < cursor || excerpt.end > tail_start {
            return Ok(None);
        }
        if excerpt.start > cursor {
            bytes.extend_from_slice(omitted(excerpt.start - cursor).as_bytes());
        }
        bytes.extend_from_slice(excerpt.label().as_bytes());
        bytes.extend_from_slice(&excerpt.bytes);
        cursor = excerpt.end;
    }
    if tail_start > cursor {
        bytes.extend_from_slice(omitted(tail_start - cursor).as_bytes());
    }
    bytes.extend_from_slice(&tail);
    Ok(String::from_utf8(bytes)
        .ok()
        .filter(|text| text.len() <= budget))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::output_store::{OutputStore, OutputStoreManager};
    use std::time::Duration;
    use tempfile::TempDir;

    fn store(bytes: &[u8]) -> (TempDir, OutputStore) {
        let root = TempDir::new().unwrap();
        let manager = OutputStoreManager::open(
            root.path(),
            Duration::from_secs(60),
            Duration::ZERO,
            4 * 1024 * 1024,
            0,
            32,
        )
        .unwrap();
        let mut store = manager.create_artifact().unwrap();
        store.append(bytes).unwrap();
        (root, store)
    }

    fn log(summary: &str) -> Vec<u8> {
        [
            b"head\n".as_slice(),
            &b"progress\n".repeat(1000),
            summary.as_bytes(),
            &b"cleanup\n".repeat(1000),
            b"tail\n",
        ]
        .concat()
    }

    #[test]
    fn known_build_invocations_and_safe_command_lists_opt_in() {
        for (command, kind) in [
            ("rustc main.rs", BuildOutput::Rustc),
            ("/usr/bin/rustc --version", BuildOutput::Rustc),
            ("exec ./gradlew build --console=plain", BuildOutput::Gradle),
            ("gradle test", BuildOutput::Gradle),
            ("cargo build --release", BuildOutput::Cargo),
            ("cargo check", BuildOutput::Cargo),
            ("cargo test --all-targets", BuildOutput::Cargo),
            ("cd repo && ./gradlew build", BuildOutput::Gradle),
            ("cd repo; gradle clean && gradle test", BuildOutput::Gradle),
            (
                "export RUST_BACKTRACE=1\ncargo check || cargo test",
                BuildOutput::Cargo,
            ),
            ("pushd repo && exec rustc main.rs; popd", BuildOutput::Rustc),
        ] {
            assert_eq!(BuildOutput::for_command(command), Some(kind), "{command}");
        }
        for command in [
            "batnr 30:40 main.rs",
            "bat main.rs",
            "cat build.log",
            "apply_patch <<'PATCH'\nBUILD FAILED in 1s\nPATCH",
            "rg rustc build.log",
            "ripgrep FAILURE build.log",
            "ast-grep outline src",
            "cargo metadata",
            "cargo run",
            "bash build.sh",
            "rustc x.rs; cat secret",
            "rustc x.rs | cat",
            "git status && ./gradlew build",
            "cargo check && ./gradlew build",
            "echo gradle",
            "env cargo build",
            "'rustc' main.rs",
            "gradle $(cat task)",
            "gradle build\ncat file",
        ] {
            assert_eq!(BuildOutput::for_command(command), None, "{command}");
        }
    }

    #[test]
    fn buried_summary_keeps_head_tail_and_exact_raw_byte_accounting() {
        let bytes = log("BUILD FAILED in 1s\n");
        let (_root, store) = store(&bytes);
        let baseline = store.project(store.snapshot(0), 1024, false).unwrap();
        assert!(!baseline.output.contains("BUILD FAILED"));
        let result = add_build_summary(baseline, Some(BuildOutput::Gradle), 1024);
        assert!(result.output.starts_with("head\n"));
        assert!(result.output.ends_with("tail\n"));
        assert!(result.output.contains("BUILD FAILED in 1s\n"));
        assert!(
            result
                .output
                .contains("[build summary excerpt; bytes 9005..9024]")
        );
        assert!(result.truncated);
        assert!(!result.encoding_loss);
        assert!(result.output.len() <= 1024);
        assert_eq!(result.snapshot.end, bytes.len() as u64);
        assert_eq!(std::fs::read(&result.snapshot.path).unwrap(), bytes);

        // Remove only generated markers/labels. Visible raw bytes + actual
        // omitted byte counts must cover the snapshot exactly.
        let mut rendered = result.output.clone();
        let mut omitted_bytes = 0;
        while let Some(start) = rendered.find("\n... ") {
            let end = start
                + rendered[start..].find(" bytes omitted ...\n").unwrap()
                + " bytes omitted ...\n".len();
            let number = rendered[start + 5..end].split(' ').next().unwrap();
            omitted_bytes += number.parse::<usize>().unwrap();
            rendered.replace_range(start..end, "");
        }
        let start = rendered.find("\n[build summary excerpt;").unwrap();
        let end = start + rendered[start..].find("]\n").unwrap() + 2;
        rendered.replace_range(start..end, "");
        assert_eq!(rendered.len() + omitted_bytes, bytes.len());
    }

    #[test]
    fn ordinary_commands_and_small_or_already_visible_outputs_are_unchanged() {
        for bytes in [
            log("BUILD FAILED in 1s\n"),
            b"BUILD FAILED in 1s\n".to_vec(),
            [b"progress\n".repeat(1000), b"BUILD FAILED in 1s\n".to_vec()].concat(),
        ] {
            let (_root, store) = store(&bytes);
            let baseline = store.project(store.snapshot(0), 1024, false).unwrap();
            let result = add_build_summary(baseline.clone(), None, 1024);
            assert_eq!(result.output, baseline.output);
            if bytes.len() < 1024 || bytes.ends_with(b"BUILD FAILED in 1s\n") {
                let result = add_build_summary(baseline.clone(), Some(BuildOutput::Gradle), 1024);
                assert_eq!(result.output, baseline.output);
            }
        }
    }

    #[test]
    fn summary_does_not_cross_snapshot_boundaries_or_scan_the_whole_log() {
        let bytes = log("BUILD FAILED in 1s\n");
        let (_root, mut store) = store(&bytes);
        let summary_start = 9005;
        let mut snapshot = store.snapshot(0);
        snapshot.end = summary_start + 8;
        let baseline = store.project(snapshot, 1024, true).unwrap();
        let result = add_build_summary(baseline.clone(), Some(BuildOutput::Gradle), 1024);
        assert_eq!(result.output, baseline.output);
        // A poll beginning inside the keyword must not recover prior output.
        let baseline = store
            .project(store.snapshot(summary_start + 8), 1024, false)
            .unwrap();
        let result = add_build_summary(baseline.clone(), Some(BuildOutput::Gradle), 1024);
        assert_eq!(result.output, baseline.output);
        store.append(&b"cleanup\n".repeat(10_000)).unwrap();
        let baseline = store.project(store.snapshot(0), 1024, false).unwrap();
        let result = add_build_summary(baseline.clone(), Some(BuildOutput::Gradle), 1024);
        assert_eq!(result.output, baseline.output);
    }

    #[test]
    fn compiler_summary_formats_and_color_are_recognized_without_rewriting() {
        for (summary, kind) in [
            (
                "error: aborting due to 8 previous errors\n",
                BuildOutput::Rustc,
            ),
            ("warning: 2 warnings emitted\n", BuildOutput::Rustc),
            (
                "    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.2s\n",
                BuildOutput::Cargo,
            ),
            (
                "test result: FAILED. 1 passed; 1 failed; 0 ignored\n",
                BuildOutput::Cargo,
            ),
            (
                "error: could not compile `app` (lib) due to 1 previous error\n",
                BuildOutput::Cargo,
            ),
            ("\x1b[31mBUILD FAILED\x1b[0m in 1s\r\n", BuildOutput::Gradle),
            (
                "12 actionable tasks: 1 executed, 11 up-to-date\n",
                BuildOutput::Gradle,
            ),
        ] {
            let (_root, store) = store(&log(summary));
            let baseline = store.project(store.snapshot(0), 2048, false).unwrap();
            let result = add_build_summary(baseline, Some(kind), 2048);
            assert!(
                result.output.contains("[build summary excerpt;"),
                "{summary}"
            );
            assert!(result.output.contains(summary), "{summary}");
        }
        assert!(!BuildOutput::Rustc.is_summary("BUILD FAILED in 1s"));
        assert!(!BuildOutput::Gradle.is_summary("example: BUILD FAILED in 1s"));
        assert!(summary_text(b"BUILD FAILED\x00 in 1s\n").is_none());
        assert!(summary_text(b"BUILD FAILED\rforged\n").is_none());
    }

    #[test]
    fn all_budgets_remain_bounded_with_multibyte_text_and_multiple_summaries() {
        let bytes = [
            "あいうえお\n".repeat(1000).as_bytes(),
            b"BUILD FAILED in 1s\n10 actionable tasks: 2 executed, 8 up-to-date\n",
            "かきくけこ\n".repeat(1000).as_bytes(),
        ]
        .concat();
        let (_root, store) = store(&bytes);
        for budget in [1, 32, 128, 511, 512, 1024, 4096] {
            let baseline = store.project(store.snapshot(0), budget, false).unwrap();
            let result = add_build_summary(baseline.clone(), Some(BuildOutput::Gradle), budget);
            assert!(result.output.len() <= budget);
            assert!(!result.encoding_loss);
            assert_eq!(result.snapshot.end, baseline.snapshot.end);
            if budget < 512 {
                assert_eq!(result.output, baseline.output);
            }
        }
    }

    #[test]
    fn latest_three_excerpts_are_ordered_and_optional_read_failure_falls_back() {
        let summaries: String = (0..10)
            .map(|n| format!("BUILD SUCCESSFUL in {n}s\n"))
            .collect();
        let (_root, store) = store(&log(&summaries));
        let baseline = store.project(store.snapshot(0), 8192, false).unwrap();
        let result = add_build_summary(baseline.clone(), Some(BuildOutput::Gradle), 8192);
        assert_eq!(result.output.matches("[build summary excerpt;").count(), 3);
        assert!(!result.output.contains("BUILD SUCCESSFUL in 6s"));
        assert!(
            result.output.find("BUILD SUCCESSFUL in 7s").unwrap()
                < result.output.find("BUILD SUCCESSFUL in 9s").unwrap()
        );
        std::fs::remove_file(&baseline.snapshot.path).unwrap();
        let result = add_build_summary(baseline.clone(), Some(BuildOutput::Gradle), 8192);
        assert_eq!(result.output, baseline.output);
    }
}
