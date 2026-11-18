//! Output-path safety boundary.
//!
//! Original Phoenix archives must never be written. Defenses, in order:
//! 1. Lexical deny: the requested string itself, and its absolute form,
//!    must not equal or lie under a protected root (case-insensitive, `/`
//!    and `\` equivalent), so `E:/phoenixresearch/x` is refused on any OS.
//! 2. No `..` components anywhere (no traversal out of a safe root).
//! 3. Every existing component is inspected with `symlink_metadata`;
//!    symlinks and (on Windows) any reparse point, which includes
//!    junctions and mount points, are refused rather than followed.
//! 4. Canonical deny: the deepest existing ancestor is canonicalized and
//!    the projected path compared with canonicalized protected roots, so an
//!    alias (subst drive, hard-linked parent, 8.3 name) that resolves into a
//!    protected root is refused.
//! 5. The final directory is created by this call or must already be empty;
//!    afterwards steps 3–4 are repeated on the real directory and its
//!    canonical form must equal the projection (time-of-check/time-of-use).
//! 6. Files are written via `create_new` temporaries in that directory and
//!    renamed only onto names that do not exist; nothing is overwritten.
//!
//! Remaining limitation: an attacker with write access to a parent can
//! still race between step 5 and a later file creation. This boundary
//! protects against operator mistakes and aliasing, not a hostile local
//! user.
use std::fs::{self, Metadata, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::{Component, Path, PathBuf};

pub const DEFAULT_PROTECTED: [&str; 2] = [r"E:\PhoenixResearch", r"E:\TradingNextData"];

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathError {
    Traversal,
    Link(PathBuf),
    Protected(PathBuf),
    NotEmpty(PathBuf),
    NotDirectory(PathBuf),
    Exists(PathBuf),
    Changed,
    Io(String),
}
fn io(e: std::io::Error) -> PathError {
    PathError::Io(e.to_string())
}

/// Defaults plus `PHOENIX_PROTECTED_ROOTS` (`;`-separated). Never empty.
pub fn protected_roots() -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = DEFAULT_PROTECTED.iter().map(PathBuf::from).collect();
    if let Ok(extra) = std::env::var("PHOENIX_PROTECTED_ROOTS") {
        v.extend(
            extra
                .split(';')
                .filter(|s| !s.trim().is_empty())
                .map(PathBuf::from),
        );
    }
    v
}

fn key(p: &Path) -> String {
    let mut s = p.to_string_lossy().replace('/', "\\").to_lowercase();
    while s.len() > 1 && s.ends_with('\\') {
        s.pop();
    }
    s
}
fn lexically_inside(p: &Path, root: &Path) -> bool {
    let (p, r) = (key(p), key(root));
    p == r || p.starts_with(&format!("{r}\\"))
}

#[cfg(windows)]
fn is_link(md: &Metadata) -> bool {
    use std::os::windows::fs::MetadataExt;
    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x400;
    md.file_type().is_symlink() || md.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
}
#[cfg(not(windows))]
fn is_link(md: &Metadata) -> bool {
    md.file_type().is_symlink()
}

/// Rejects `..` and any existing symlink/reparse component. Returns the
/// deepest existing prefix and the non-existing remainder.
fn inspect(abs: &Path) -> Result<(PathBuf, PathBuf), PathError> {
    let mut cur = PathBuf::new();
    let mut rest = PathBuf::new();
    let mut missing = false;
    for comp in abs.components() {
        match comp {
            Component::ParentDir => return Err(PathError::Traversal),
            Component::CurDir => continue,
            // Not stat-able as links: stat of a verbatim prefix (`\\?\C:`) opens
            // the raw volume and fails with ERROR_INVALID_FUNCTION on Windows.
            Component::Prefix(_) | Component::RootDir => {
                cur.push(comp);
                continue;
            }
            // Verbatim paths keep `..`/`.` as literal Normal components.
            Component::Normal(n) if n == ".." => return Err(PathError::Traversal),
            Component::Normal(n) if n == "." => continue,
            Component::Normal(_) => {}
        }
        if missing {
            rest.push(comp);
            continue;
        }
        let next = cur.join(comp);
        match fs::symlink_metadata(&next) {
            Ok(md) if is_link(&md) => return Err(PathError::Link(next)),
            Ok(_) => cur = next,
            Err(e) if e.kind() == ErrorKind::NotFound => {
                missing = true;
                rest.push(comp);
            }
            Err(e) => return Err(io(e)),
        }
    }
    Ok((cur, rest))
}

fn deny(p: &Path, roots: &[PathBuf]) -> Result<(), PathError> {
    for r in roots {
        if lexically_inside(p, r) {
            return Err(PathError::Protected(p.to_path_buf()));
        }
        if let Ok(cr) = fs::canonicalize(r) {
            if p.starts_with(&cr) || lexically_inside(p, &cr) {
                return Err(PathError::Protected(p.to_path_buf()));
            }
        }
    }
    Ok(())
}

/// Validates (and creates if absent) an empty output directory. Returns its
/// canonical path, which callers must use for every subsequent write.
pub fn prepare_output_dir(requested: &Path, roots: &[PathBuf]) -> Result<PathBuf, PathError> {
    deny(requested, roots)?;
    let abs = if requested.is_absolute() {
        requested.to_path_buf()
    } else {
        std::env::current_dir().map_err(io)?.join(requested)
    };
    deny(&abs, roots)?;
    let (existing, rest) = inspect(&abs)?;
    let base = if existing.as_os_str().is_empty() {
        PathBuf::from(".")
    } else {
        existing
    };
    let projected = fs::canonicalize(base).map_err(io)?.join(rest);
    deny(&projected, roots)?;
    match fs::symlink_metadata(&abs) {
        Ok(md) => {
            if is_link(&md) {
                return Err(PathError::Link(abs));
            }
            if !md.is_dir() {
                return Err(PathError::NotDirectory(abs));
            }
            if fs::read_dir(&abs).map_err(io)?.next().is_some() {
                return Err(PathError::NotEmpty(abs));
            }
        }
        Err(e) if e.kind() == ErrorKind::NotFound => {
            if let Some(parent) = abs.parent() {
                fs::create_dir_all(parent).map_err(io)?;
            }
            fs::create_dir(&abs).map_err(|e| {
                if e.kind() == ErrorKind::AlreadyExists {
                    PathError::Changed
                } else {
                    io(e)
                }
            })?;
        }
        Err(e) => return Err(io(e)),
    }
    // Re-verify the real directory after creation.
    let (_, rest_after) = inspect(&abs)?;
    if !rest_after.as_os_str().is_empty() {
        return Err(PathError::Changed);
    }
    let canon = fs::canonicalize(&abs).map_err(io)?;
    if canon != projected {
        return Err(PathError::Changed);
    }
    deny(&canon, roots)?;
    Ok(canon)
}

/// Same checks as [`prepare_output_dir`] but the directory may already
/// hold files (used for the long-lived experiment registry).
pub fn safe_state_dir(requested: &Path, roots: &[PathBuf]) -> Result<PathBuf, PathError> {
    deny(requested, roots)?;
    let abs = if requested.is_absolute() {
        requested.to_path_buf()
    } else {
        std::env::current_dir().map_err(io)?.join(requested)
    };
    deny(&abs, roots)?;
    inspect(&abs)?;
    fs::create_dir_all(&abs).map_err(io)?;
    let (_, rest) = inspect(&abs)?;
    if !rest.as_os_str().is_empty() {
        return Err(PathError::Changed);
    }
    let canon = fs::canonicalize(&abs).map_err(io)?;
    deny(&canon, roots)?;
    Ok(canon)
}

/// Writes `bytes` to `dir/name` via a `create_new` temporary and rename.
/// Fails if `dir/name` already exists; never overwrites.
pub fn write_new_file(dir: &Path, name: &str, bytes: &[u8]) -> Result<PathBuf, PathError> {
    if name.contains(['/', '\\']) || name == ".." || name == "." || name.is_empty() {
        return Err(PathError::Traversal);
    }
    let target = dir.join(name);
    match fs::symlink_metadata(&target) {
        Ok(_) => return Err(PathError::Exists(target)),
        Err(e) if e.kind() == ErrorKind::NotFound => {}
        Err(e) => return Err(io(e)),
    }
    let tmp = dir.join(format!(".{name}.{}.tmp", std::process::id()));
    {
        let mut f = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp)
            .map_err(io)?;
        f.write_all(bytes).map_err(io)?;
        f.sync_all().map_err(io)?;
    }
    if fs::symlink_metadata(&target).is_ok() {
        let _ = fs::remove_file(&tmp);
        return Err(PathError::Exists(target));
    }
    fs::rename(&tmp, &target).map_err(io)?;
    Ok(target)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("phoenix-paths-{}-{name}", std::process::id()));
        let _ = fs::remove_dir_all(&d);
        fs::create_dir_all(&d).unwrap();
        fs::canonicalize(d).unwrap()
    }

    #[test]
    fn default_protected_roots_are_refused_lexically_on_any_os() {
        let r = protected_roots();
        for p in [
            r"E:\PhoenixResearch",
            r"e:\phoenixresearch\out",
            "E:/TradingNextData/x/",
            r"E:\PhoenixResearch\",
        ] {
            assert!(
                matches!(
                    prepare_output_dir(Path::new(p), &r),
                    Err(PathError::Protected(_))
                ),
                "{p}"
            );
        }
        assert!(!lexically_inside(
            Path::new(r"E:\PhoenixResearchOutputs"),
            Path::new(r"E:\PhoenixResearch")
        ));
    }

    #[test]
    fn traversal_is_refused() {
        let d = scratch("trav");
        // Built from a string: `join` on a Windows verbatim (`\\?\`) base
        // normalizes `..` away, which would hide the traversal under test.
        let sep = std::path::MAIN_SEPARATOR;
        let p = PathBuf::from(format!("{}{sep}a{sep}..{sep}b", d.display()));
        assert_eq!(prepare_output_dir(&p, &[]), Err(PathError::Traversal));
    }

    #[test]
    fn safe_scratch_dir_is_created_and_must_be_empty() {
        let d = scratch("ok");
        let out = d.join("runs").join("r1");
        let c = prepare_output_dir(&out, &protected_roots()).unwrap();
        assert!(c.is_dir());
        write_new_file(&c, "report.json", b"{}").unwrap();
        assert!(matches!(
            write_new_file(&c, "report.json", b"x"),
            Err(PathError::Exists(_))
        ));
        assert_eq!(fs::read(c.join("report.json")).unwrap(), b"{}");
        assert!(matches!(
            prepare_output_dir(&out, &[]),
            Err(PathError::NotEmpty(_))
        ));
        assert_eq!(write_new_file(&c, "../x", b""), Err(PathError::Traversal));
    }

    #[test]
    fn canonical_protected_root_and_aliases_are_refused() {
        let d = scratch("canon");
        let prot = d.join("Archive");
        fs::create_dir_all(&prot).unwrap();
        fs::write(prot.join("original.parquet"), b"ORIGINAL").unwrap();
        let roots = vec![prot.clone()];
        assert!(matches!(
            prepare_output_dir(&prot.join("out"), &roots),
            Err(PathError::Protected(_))
        ));
        // Non-canonical spelling with "." components still resolves inside.
        let dotted = d.join(".").join("Archive").join(".").join("o");
        assert!(matches!(
            prepare_output_dir(&dotted, &roots),
            Err(PathError::Protected(_))
        ));
        #[cfg(unix)]
        {
            let alias = d.join("alias");
            std::os::unix::fs::symlink(&prot, &alias).unwrap();
            assert!(matches!(
                prepare_output_dir(&alias.join("out"), &roots),
                Err(PathError::Link(_))
            ));
            // Even with no protected roots configured, links are refused.
            assert!(matches!(
                prepare_output_dir(&alias.join("out"), &[]),
                Err(PathError::Link(_))
            ));
            let file_link = d.join("flink");
            std::os::unix::fs::symlink(prot.join("original.parquet"), &file_link).unwrap();
            assert!(matches!(
                prepare_output_dir(&file_link, &[]),
                Err(PathError::Link(_))
            ));
        }
        assert!(matches!(
            safe_state_dir(&prot.join("reg"), &roots),
            Err(PathError::Protected(_))
        ));
        let reg = safe_state_dir(&d.join("reg"), &roots).unwrap();
        fs::write(reg.join("x"), b"1").unwrap();
        assert_eq!(safe_state_dir(&d.join("reg"), &roots).unwrap(), reg);
        assert_eq!(
            fs::read(prot.join("original.parquet")).unwrap(),
            b"ORIGINAL"
        );
        assert_eq!(fs::read_dir(&prot).unwrap().count(), 1);
    }
}
