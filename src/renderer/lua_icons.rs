use std::path::{Path, PathBuf};

pub fn extract_lua_string(line: &str, field: &str) -> Option<String> {
    let pattern = format!("{} = \"", field);
    let pos = line.find(&pattern)?;
    if pos > 0 && (line.as_bytes()[pos - 1].is_ascii_alphanumeric() || line.as_bytes()[pos - 1] == b'_') {
        return None;
    }
    let start = pos + pattern.len();
    let rest = &line[start..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

pub fn resolve_factorio_path(lua_path: &str, factorio_path: &Path) -> Option<PathBuf> {
    if let Some(rest) = lua_path.strip_prefix("__base__/") {
        Some(factorio_path.join("base").join(rest))
    } else if let Some(rest) = lua_path.strip_prefix("__core__/") {
        Some(factorio_path.join("core").join(rest))
    } else {
        None
    }
}
