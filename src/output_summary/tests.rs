use super::*;
use crate::output_projection::project;
use crate::output_store::{OutputStore, OutputStoreManager};
use std::time::Duration;
use tempfile::TempDir;

fn summarize(baseline: Projection, kind: Option<BuildOutput>, budget: usize) -> Projection {
    let summary = kind.map(|kind| BuildSummary {
        kind,
        limits: SummaryLimits::default(),
    });
    crate::output_postprocessing::apply_processor(
        baseline,
        summary
            .as_ref()
            .map(|summary| summary as &dyn OutputProcessor),
        budget,
    )
}

fn store(bytes: &[u8]) -> (TempDir, OutputStore) {
    let root = TempDir::new().unwrap();
    let manager =
        OutputStoreManager::open(root.path(), Duration::from_secs(60), 4 * 1024 * 1024).unwrap();
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
    let summary = "BUILD FAILED in 1s\n";
    let bytes = log(summary);
    let (_root, store) = store(&bytes);
    let baseline = project(store.snapshot(0), 1024, false).unwrap();
    assert!(!baseline.output.contains("BUILD FAILED"));
    let result = summarize(baseline, Some(BuildOutput::Gradle), 1024);
    assert!(result.output.starts_with("head\n"));
    assert!(result.output.ends_with("tail\n"));
    assert!(result.output.contains("BUILD FAILED in 1s\n"));
    let summary_start = bytes
        .windows(summary.len())
        .position(|window| window == summary.as_bytes())
        .unwrap();
    assert!(result.output.contains(&format!(
        "[build summary excerpt; bytes {}..{}]",
        summary_start,
        summary_start + summary.len()
    )));
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
        let baseline = project(store.snapshot(0), 1024, false).unwrap();
        let result = summarize(baseline.clone(), None, 1024);
        assert_eq!(result.output, baseline.output);
        if bytes.len() < 1024 || bytes.ends_with(b"BUILD FAILED in 1s\n") {
            let result = summarize(baseline.clone(), Some(BuildOutput::Gradle), 1024);
            assert_eq!(result.output, baseline.output);
        }
    }
}

#[test]
fn summary_does_not_cross_snapshot_boundaries_or_scan_the_whole_log() {
    let summary = "BUILD FAILED in 1s\n";
    let bytes = log(summary);
    let (_root, mut store) = store(&bytes);
    let summary_start = bytes
        .windows(summary.len())
        .position(|window| window == summary.as_bytes())
        .unwrap() as u64;
    let mut snapshot = store.snapshot(0);
    snapshot.end = summary_start + 8;
    let baseline = project(snapshot, 1024, true).unwrap();
    let result = summarize(baseline.clone(), Some(BuildOutput::Gradle), 1024);
    assert_eq!(result.output, baseline.output);
    // A poll beginning inside the keyword must not recover prior output.
    let baseline = project(store.snapshot(summary_start + 8), 1024, false).unwrap();
    let result = summarize(baseline.clone(), Some(BuildOutput::Gradle), 1024);
    assert_eq!(result.output, baseline.output);
    store.append(&b"cleanup\n".repeat(10_000)).unwrap();
    let baseline = project(store.snapshot(0), 1024, false).unwrap();
    let result = summarize(baseline.clone(), Some(BuildOutput::Gradle), 1024);
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
        let baseline = project(store.snapshot(0), 2048, false).unwrap();
        let result = summarize(baseline, Some(kind), 2048);
        assert!(
            result.output.contains("[build summary excerpt;"),
            "{summary}"
        );
        assert!(result.output.contains(summary), "{summary}");
    }
    assert!(!BuildOutput::Rustc.is_summary("BUILD FAILED in 1s"));
    assert!(!BuildOutput::Gradle.is_summary("example: BUILD FAILED in 1s"));
    assert!(summary_text(b"BUILD FAILED\x00 in 1s\n", 512).is_none());
    assert!(summary_text(b"BUILD FAILED\rforged\n", 512).is_none());
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
        let baseline = project(store.snapshot(0), budget, false).unwrap();
        let result = summarize(baseline.clone(), Some(BuildOutput::Gradle), budget);
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
    let baseline = project(store.snapshot(0), 8192, false).unwrap();
    let result = summarize(baseline.clone(), Some(BuildOutput::Gradle), 8192);
    assert_eq!(result.output.matches("[build summary excerpt;").count(), 3);
    assert!(!result.output.contains("BUILD SUCCESSFUL in 6s"));
    assert!(
        result.output.find("BUILD SUCCESSFUL in 7s").unwrap()
            < result.output.find("BUILD SUCCESSFUL in 9s").unwrap()
    );
    std::fs::remove_file(&baseline.snapshot.path).unwrap();
    let result = summarize(baseline.clone(), Some(BuildOutput::Gradle), 8192);
    assert_eq!(result.output, baseline.output);
}

#[test]
fn processor_limits_bound_scanning_lines_and_excerpts() {
    use crate::output_postprocessing::apply_processor;

    let summaries: String = (0..5)
        .map(|n| format!("BUILD SUCCESSFUL in {n}s\n"))
        .collect();
    let (_root, store) = store(&log(&summaries));
    let baseline = project(store.snapshot(0), 4096, false).unwrap();
    for limits in [
        SummaryLimits {
            scan_bytes: 0,
            ..SummaryLimits::default()
        },
        // A nonzero scan window containing only cleanup lines.
        SummaryLimits {
            scan_bytes: 100,
            ..SummaryLimits::default()
        },
        SummaryLimits {
            max_line_bytes: 10,
            ..SummaryLimits::default()
        },
        SummaryLimits {
            max_excerpts: 0,
            ..SummaryLimits::default()
        },
        SummaryLimits {
            max_excerpt_bytes: 0,
            ..SummaryLimits::default()
        },
        // The excerpt text fits, but its byte-range label and markers do not.
        SummaryLimits {
            max_excerpt_bytes: 24,
            ..SummaryLimits::default()
        },
    ] {
        let processor = BuildSummary {
            kind: BuildOutput::Gradle,
            limits,
        };
        let result = apply_processor(baseline.clone(), Some(&processor), 4096);
        assert_eq!(result.output, baseline.output, "{limits:?}");
    }

    let processor = BuildSummary {
        kind: BuildOutput::Gradle,
        limits: SummaryLimits {
            max_excerpts: 1,
            ..SummaryLimits::default()
        },
    };
    let result = apply_processor(baseline.clone(), Some(&processor), 4096);
    assert_eq!(result.output.matches("[build summary excerpt;").count(), 1);
    assert!(result.output.contains("BUILD SUCCESSFUL in 4s\n"));
    assert!(!result.output.contains("BUILD SUCCESSFUL in 3s\n"));
    assert!(result.output.len() <= 4096);
    assert_eq!(result.snapshot.end, baseline.snapshot.end);
}

#[test]
fn invalid_utf8_and_held_partial_scalars_keep_original_projection() {
    use crate::output_postprocessing::OutputPostprocessing;

    let mut bytes = log("BUILD FAILED in 1s\n");
    bytes[0] = 0xff;
    let (_root, store) = store(&bytes);
    let baseline = project(store.snapshot(0), 1024, false).unwrap();
    assert!(baseline.encoding_loss);
    let stage = OutputPostprocessing::for_command("gradle test");
    let result = stage.apply(baseline.clone(), 1024);
    assert_eq!(result.output, baseline.output);
    assert!(result.encoding_loss);
    assert_eq!(result.snapshot.end, baseline.snapshot.end);

    let (_root, store) = self::store(b"progress\n\xe2\x82");
    let baseline = project(store.snapshot(0), 1024, true).unwrap();
    assert_eq!(baseline.output, "progress\n");
    let result = stage.apply(baseline.clone(), 1024);
    assert_eq!(result.output, baseline.output);
    assert_eq!(result.snapshot.end, b"progress\n".len() as u64);
    assert_eq!(
        result.snapshot.stored_bytes,
        b"progress\n\xe2\x82".len() as u64
    );
}
