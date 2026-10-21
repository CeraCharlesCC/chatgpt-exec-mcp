//! Optional build-summary excerpts. Unknown commands keep ordinary head/tail.
//! This is a display hint, never an execution/permission or success policy.

use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;

use crate::output_postprocessing::OutputProcessor;
use crate::output_projection::Projection;
use crate::output_projection::{omitted_marker, read_utf8_safe_head, read_utf8_safe_tail};

/// Internal processor policy, ready for future configuration without changing
/// session capture or delivery. Defaults retain the existing summary limits.
#[derive(Clone, Copy, Debug)]
struct SummaryLimits {
    scan_bytes: u64,
    max_line_bytes: usize,
    max_excerpts: usize,
    max_excerpt_bytes: usize,
}

impl Default for SummaryLimits {
    fn default() -> Self {
        Self {
            scan_bytes: 64 * 1024,
            max_line_bytes: 512,
            max_excerpts: 3,
            max_excerpt_bytes: 1024,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct BuildSummary {
    kind: BuildOutput,
    limits: SummaryLimits,
}

impl BuildSummary {
    pub(crate) fn for_command(command: &str) -> Option<Self> {
        BuildOutput::for_command(command).map(|kind| Self {
            kind,
            limits: SummaryLimits::default(),
        })
    }
}

impl OutputProcessor for BuildSummary {
    fn default_max_output_tokens(&self) -> usize {
        2_000
    }

    fn replacement(&self, baseline: &Projection, budget: usize) -> io::Result<Option<String>> {
        if !baseline.truncated || baseline.encoding_loss || budget < 512 {
            return Ok(None);
        }
        summary_projection(baseline, self.kind, budget, self.limits)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BuildOutput {
    Rustc,
    Cargo,
    Gradle,
}

impl BuildOutput {
    fn for_command(command: &str) -> Option<Self> {
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
fn summary_text(bytes: &[u8], max_line_bytes: usize) -> Option<String> {
    if bytes.len() > max_line_bytes {
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

fn summary_projection(
    baseline: &Projection,
    kind: BuildOutput,
    budget: usize,
    limits: SummaryLimits,
) -> io::Result<Option<String>> {
    if limits.max_excerpts == 0 || limits.scan_bytes == 0 {
        return Ok(None);
    }
    let snapshot = &baseline.snapshot;
    let range = snapshot.end - snapshot.start;
    let scan_start = snapshot.end - range.min(limits.scan_bytes);
    let mut file = File::open(&snapshot.path)?;
    file.seek(SeekFrom::Start(scan_start))?;
    let mut scan = Vec::new();
    Read::by_ref(&mut file)
        .take(range.min(limits.scan_bytes))
        .read_to_end(&mut scan)?;
    let marker_cost = omitted_marker(range).len();
    let mut candidates = Vec::new();
    let mut offset = scan_start;
    for line in scan.split_inclusive(|byte| *byte == b'\n') {
        let start = offset;
        offset += line.len() as u64;
        // Never interpret a partial first/last line at a scan or poll boundary.
        if (start == scan_start && scan_start != 0) || !line.ends_with(b"\n") {
            continue;
        }
        if let Some(text) = summary_text(line, limits.max_line_bytes)
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

    // At most 20% and the processor's byte cap (1 KiB by default) for
    // excerpts plus their labels/extra markers.
    // Prefer the latest fitting lines; preserve source order in the response.
    let mut allowance = (budget / 5).min(limits.max_excerpt_bytes);
    let mut selected = Vec::new();
    for candidate in candidates.into_iter().rev() {
        let cost = candidate.bytes.len() + candidate.label().len() + marker_cost;
        if cost <= allowance {
            allowance -= cost;
            selected.push(candidate);
            if selected.len() == limits.max_excerpts {
                break;
            }
        }
    }
    if selected.is_empty() {
        return Ok(None);
    }
    selected.reverse();
    let used = (budget / 5).min(limits.max_excerpt_bytes) - allowance;
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
            bytes.extend_from_slice(omitted_marker(excerpt.start - cursor).as_bytes());
        }
        bytes.extend_from_slice(excerpt.label().as_bytes());
        bytes.extend_from_slice(&excerpt.bytes);
        cursor = excerpt.end;
    }
    if tail_start > cursor {
        bytes.extend_from_slice(omitted_marker(tail_start - cursor).as_bytes());
    }
    bytes.extend_from_slice(&tail);
    Ok(String::from_utf8(bytes)
        .ok()
        .filter(|text| text.len() <= budget))
}

#[cfg(test)]
mod tests;
