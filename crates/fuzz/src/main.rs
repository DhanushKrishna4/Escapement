//! Run N seeds, report violations, minimize the repro.
//!
//! ```text
//! cargo run --release -p fuzz -- --seeds 10000
//! cargo run --release -p fuzz -- --seeds 2000 --bug commit-rule    # prove it can find one
//! ```

use std::path::PathBuf;
use std::process::ExitCode;

use fuzz::{report, sweep, trace_seed, Options};
use raft::BugSwitches;

struct Args {
    start: u64,
    seeds: u64,
    threads: usize,
    ticks: u64,
    bugs: BugSwitches,
    stale_reads: bool,
    bug_label: Option<String>,
    trace_dir: Option<PathBuf>,
    minimize: bool,
    describe: bool,
}

impl Default for Args {
    fn default() -> Self {
        Args {
            start: 0,
            seeds: 1_000,
            threads: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4),
            ticks: 30_000,
            bugs: BugSwitches::default(),
            stale_reads: false,
            bug_label: None,
            trace_dir: None,
            minimize: true,
            describe: false,
        }
    }
}

const USAGE: &str = "\
usage: fuzz [options]

  --start N          first seed (default 0)
  --seeds N          how many seeds to run (default 1000)
  --threads N        OS threads (default: all cores)
  --ticks N          virtual ticks per run (default 30000)
  --trace-dir DIR    write traces and repros for failing seeds here
  --no-minimize      report failures without shrinking them
  --describe         print each seed's configuration and exit
  --bug NAME         enable a deliberate bug, to check the harness can find one:
                       commit-rule    commit previous-term entries by replica count
                       double-vote    ignore votedFor
                       blind-commit   take leaderCommit uncapped
                       no-persist     never write currentTerm/votedFor to disk
                       stale-read     leaders answer reads locally, with no
                                      ReadIndex round (only the linearizability
                                      checker can catch this one)
                     regressions actually found in this implementation:
                       compaction-anchor  index 0 anchors even after compaction
                       no-reconcile       snapshot and log not reconciled on recovery
                       early-read-index   read index captured when the request arrives
  -h, --help         this
";

fn parse() -> Result<Args, String> {
    let mut args = Args::default();
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        let mut value = || {
            it.next()
                .ok_or_else(|| format!("{arg} needs a value"))
                .and_then(|v| v.parse::<u64>().map_err(|_| format!("{arg}: not a number")))
        };
        match arg.as_str() {
            "--start" => args.start = value()?,
            "--seeds" => args.seeds = value()?,
            "--threads" => args.threads = value()? as usize,
            "--ticks" => args.ticks = value()?,
            "--no-minimize" => args.minimize = false,
            "--describe" => args.describe = true,
            "--trace-dir" => {
                let dir = it.next().ok_or("--trace-dir needs a path")?;
                args.trace_dir = Some(PathBuf::from(dir));
            }
            "--bug" => {
                let name = it.next().ok_or("--bug needs a name")?;
                args.bugs = match name.as_str() {
                    "commit-rule" => BugSwitches {
                        commit_prior_term_entries: true,
                        ..BugSwitches::default()
                    },
                    "double-vote" => BugSwitches {
                        vote_twice_per_term: true,
                        ..BugSwitches::default()
                    },
                    "blind-commit" => BugSwitches {
                        trust_leader_commit_blindly: true,
                        ..BugSwitches::default()
                    },
                    "no-persist" => BugSwitches {
                        skip_hard_state_persistence: true,
                        ..BugSwitches::default()
                    },
                    "compaction-anchor" => BugSwitches {
                        compaction_anchor_at_zero: true,
                        ..BugSwitches::default()
                    },
                    "no-reconcile" => BugSwitches {
                        skip_snapshot_reconcile: true,
                        ..BugSwitches::default()
                    },
                    "early-read-index" => BugSwitches {
                        read_index_at_arrival: true,
                        ..BugSwitches::default()
                    },
                    "stale-read" => {
                        args.stale_reads = true;
                        BugSwitches::default()
                    }
                    other => return Err(format!("unknown bug: {other}")),
                };
                args.bug_label = Some(name);
            }
            "-h" | "--help" => {
                print!("{USAGE}");
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }
    Ok(args)
}

fn main() -> ExitCode {
    let args = match parse() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("{e}\n\n{USAGE}");
            return ExitCode::from(2);
        }
    };

    let opts = Options {
        ticks: args.ticks,
        bugs: args.bugs.clone(),
        stale_reads: args.stale_reads,
        record_trace: false,
        ..Options::default()
    };

    if args.describe {
        for seed in args.start..args.start + args.seeds {
            println!("{}", fuzz::describe(seed, &opts));
        }
        return ExitCode::SUCCESS;
    }

    match &args.bug_label {
        Some(name) => println!(
            "fuzzing seeds {}..{} on {} threads WITH the '{name}' bug enabled",
            args.start,
            args.start + args.seeds,
            args.threads
        ),
        None => println!(
            "fuzzing seeds {}..{} on {} threads",
            args.start,
            args.start + args.seeds,
            args.threads
        ),
    }

    let result = sweep(args.start, args.seeds, args.threads, &opts);
    print!("{}", report::summary(&result, args.seeds));

    if result.failures.is_empty() {
        if args.bug_label.is_some() {
            println!(
                "\nNO violations found, but a bug was deliberately enabled. \
                 Either the bug cannot manifest under these schedules or the checkers missed it."
            );
            return ExitCode::from(1);
        }
        println!("\nno violations");
        return ExitCode::SUCCESS;
    }

    println!("\n{} failing seed(s)", result.failures.len());
    for failure in result.failures.iter().take(10) {
        print!("{}", report::failure(failure));
        println!("  config: {}", fuzz::describe(failure.seed, &opts));

        let minimized = if args.minimize {
            let m = fuzz::minimize(failure.seed, &opts);
            if let Some(m) = &m {
                print!("{}", report::minimized(m));
            }
            m
        } else {
            None
        };

        if let Some(dir) = &args.trace_dir {
            let traced = trace_seed(failure.seed, &opts);
            match report::write_artifacts(
                dir,
                failure,
                minimized.as_ref(),
                &traced.trace().to_json_pretty(),
            ) {
                Ok(paths) => {
                    for p in paths {
                        println!("  wrote {}", p.display());
                    }
                }
                Err(e) => eprintln!("  could not write artifacts: {e}"),
            }
        }
    }
    if result.failures.len() > 10 {
        println!(
            "\n... and {} more failing seeds",
            result.failures.len() - 10
        );
    }

    ExitCode::from(1)
}
