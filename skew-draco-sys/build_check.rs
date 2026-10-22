// The compiler-command check of skew-draco-sys (Codex 114 item 1).
//
// `include!`d by build.rs (where it decides whether the Draco C++ may be
// compiled at all) AND by the crate's unit tests, so the very function the
// build relies on is tested. Fully qualified paths only: it is spliced into
// two different modules.
//
// What is checked is the FULL command cc runs for every source, i.e.
// `cc::Tool::to_command()` (program + every argument + environment
// overrides), not `Tool::args()`: cc keeps extra words of `CXX` (e.g.
// `CXX='c++ -ffast-math'`) and a compiler wrapper (`CXX='ccache c++'`,
// `RUSTC_WRAPPER=sccache`) apart from `args()` and only adds them when it
// builds the command. cc appends `-o <obj> -c <src>` per file afterwards.

/// Compiler wrappers, refused whatever their position: cc's known list.
pub const COMPILER_WRAPPERS: &[&str] = &["ccache", "distcc", "sccache", "icecc", "cachepot", "buildcache"];

/// The only environment override allowed on the command: cc sets
/// `LC_ALL=C` for every non-MSVC compiler (English diagnostics; no effect on
/// code generation).
pub const ALLOWED_COMMAND_ENV: &[(&str, &str)] = &[("LC_ALL", "C")];

/// `Ok` iff the command runs the compiler itself (no wrapper, neither by
/// name nor behind a symlink such as `/usr/lib/ccache/c++`), with exactly
/// the `allowed` arguments in that order, and exactly the environment
/// overrides of [`ALLOWED_COMMAND_ENV`].
/// `program_resolved` is the program after the PATH lookup and symlink
/// resolution ([`resolve_program`]); `compiler` is `Tool::path()`.
pub fn check_compiler_command(
    program: &std::ffi::OsStr,
    program_resolved: Option<&std::path::Path>,
    compiler: &std::path::Path,
    args: &[std::ffi::OsString],
    env: &[(std::ffi::OsString, Option<std::ffi::OsString>)],
    allowed: &[String],
) -> Result<(), String> {
    if std::path::Path::new(program) != compiler {
        return Err(format!(
            "a compiler wrapper is refused: the command runs {program:?}, not the compiler {compiler:?}"
        ));
    }
    let candidates = [Some(std::path::Path::new(program)), program_resolved];
    for candidate in candidates.into_iter().flatten() {
        if let Some(stem) = candidate.file_stem().and_then(|s| s.to_str()) {
            if COMPILER_WRAPPERS.contains(&stem) {
                return Err(format!("a compiler wrapper is refused: {program:?} is {candidate:?}"));
            }
        }
    }
    let got: Vec<String> = args.iter().map(|a| a.to_string_lossy().into_owned()).collect();
    if got.as_slice() != allowed {
        let extra: Vec<&String> = got.iter().filter(|a| !allowed.contains(a)).collect();
        let missing: Vec<&String> = allowed.iter().filter(|a| !got.contains(a)).collect();
        return Err(format!(
            "the compiler arguments are not exactly the registered ones\n  got:     {got:?}\n  allowed: {allowed:?}\n  extra:   {extra:?}\n  missing: {missing:?}"
        ));
    }
    let env_got: Vec<(String, Option<String>)> = env
        .iter()
        .map(|(k, v)| (k.to_string_lossy().into_owned(), v.as_ref().map(|v| v.to_string_lossy().into_owned())))
        .collect();
    let env_allowed: Vec<(String, Option<String>)> =
        ALLOWED_COMMAND_ENV.iter().map(|(k, v)| (k.to_string(), Some(v.to_string()))).collect();
    if env_got != env_allowed {
        return Err(format!("the compiler command's environment overrides {env_got:?} are not exactly {env_allowed:?}"));
    }
    Ok(())
}

/// The program as the OS would run it: a bare name is looked up in `PATH`
/// (first regular file), then every symlink is resolved. `None` if not found.
pub fn resolve_program(program: &std::ffi::OsStr) -> Option<std::path::PathBuf> {
    let path = std::path::Path::new(program);
    let found = if path.components().count() > 1 {
        Some(path.to_path_buf())
    } else {
        std::env::var_os("PATH")
            .and_then(|dirs| std::env::split_paths(&dirs).map(|dir| dir.join(path)).find(|c| c.is_file()))
    };
    found.and_then(|file| std::fs::canonicalize(file).ok())
}
