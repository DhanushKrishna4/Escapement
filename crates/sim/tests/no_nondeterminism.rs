//! A grep over the source for the constructs that break determinism silently.
//!
//! Every one of these compiles and runs fine; the only symptom is that two runs
//! of the same seed stop matching, possibly thousands of events later and
//! possibly only on some machines. Catching them at commit time is far cheaper
//! than debugging a trace divergence, so this is a test rather than a lint.

use std::path::{Path, PathBuf};

/// Substrings that must not appear in core source, with the reason shown when
/// one does.
const BANNED: &[(&str, &str)] = &[
    (
        "Instant::now",
        "wall-clock time; all time is a virtual tick from the simulator",
    ),
    (
        "SystemTime",
        "wall-clock time; all time is a virtual tick from the simulator",
    ),
    (
        "std::time",
        "wall-clock time; all time is a virtual tick from the simulator",
    ),
    (
        "thread_rng",
        "OS-seeded randomness; use the seeded Rng threaded from the simulator",
    ),
    (
        "getrandom",
        "OS-seeded randomness; use the seeded Rng threaded from the simulator",
    ),
    (
        "HashMap",
        "randomly-seeded hasher makes iteration order vary per process; use BTreeMap",
    ),
    (
        "HashSet",
        "randomly-seeded hasher makes iteration order vary per process; use BTreeSet",
    ),
    (
        "RandomState",
        "randomly-seeded hasher; use a BTree collection",
    ),
    ("unsafe ", "unsafe code is forbidden in this project"),
    (
        "f32",
        "floats are not guaranteed identical across platforms; use integers",
    ),
    (
        "f64",
        "floats are not guaranteed identical across platforms; use integers",
    ),
    (
        "tokio",
        "async schedulers are nondeterministic; the core is synchronous",
    ),
    (
        "async ",
        "async schedulers are nondeterministic; the core is synchronous",
    ),
    (
        "Date::now",
        "browser wall-clock time; all time is a virtual tick",
    ),
    (
        "Math::random",
        "browser randomness; all randomness comes from the seeded Rng",
    ),
];

/// Strip `//` comments so that prose explaining *why* HashMap is banned does
/// not trip the check on itself.
fn strip_comments(src: &str) -> String {
    src.lines()
        .map(|line| match line.find("//") {
            Some(i) => &line[..i],
            None => line,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    // Sorted, so a failure names the same file every time.
    let mut paths: Vec<PathBuf> = entries.filter_map(|e| e.ok()).map(|e| e.path()).collect();
    paths.sort();
    for path in paths {
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

#[test]
fn core_source_is_free_of_nondeterminism() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("crates/sim/../.. is the workspace root")
        .to_path_buf();

    let mut files = Vec::new();
    // The wasm boundary is scanned too. It only marshals, but it is the one
    // place where reaching for a browser clock or a JS random would be easy and
    // would silently destroy reproducibility.
    for crate_name in ["raft", "kvstore", "sim", "wasm"] {
        rust_files(
            &workspace.join("crates").join(crate_name).join("src"),
            &mut files,
        );
    }
    assert!(
        !files.is_empty(),
        "found no source to scan; the path is wrong"
    );

    let mut violations = Vec::new();
    for file in &files {
        let src = std::fs::read_to_string(file).expect("readable source");
        let code = strip_comments(&src);
        for (line_no, line) in code.lines().enumerate() {
            for (needle, why) in BANNED {
                if line.contains(needle) {
                    violations.push(format!(
                        "{}:{}: `{}` -- {}\n    {}",
                        file.strip_prefix(&workspace).unwrap_or(file).display(),
                        line_no + 1,
                        needle,
                        why,
                        line.trim()
                    ));
                }
            }
        }
    }

    assert!(
        violations.is_empty(),
        "determinism hazards in core source:\n{}",
        violations.join("\n")
    );
}

/// The scan is worthless if it cannot actually see a violation.
#[test]
fn the_scan_detects_what_it_is_looking_for() {
    let sample = "let m = HashMap::new(); // BTreeMap is fine here\n";
    let stripped = strip_comments(sample);
    assert!(
        stripped.contains("HashMap"),
        "real code must still be scanned"
    );

    let only_a_comment = "// never use a HashMap in core\n";
    assert!(
        !strip_comments(only_a_comment).contains("HashMap"),
        "prose about the rule must not trip the rule"
    );
}
