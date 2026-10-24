//! Host-side id→text dictionaries and the GE task-type table.
//!
//! `hash_dic` and `type_info_dic` are plain text (`key:value`) and are loaded
//! once into `Ctx` before any node runs, so no node ever resolves an id itself.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// `MSPROF_REPORT_RUNTIME_LEVEL`: the `type_info_dic` level for runtime task types.
pub const RUNTIME_LEVEL: u16 = 5000;

/// GE hash → text (op names, kernel names, …).
#[derive(Debug, Default)]
pub struct HashDic(pub HashMap<u64, String>);

impl HashDic {
    pub fn get(&self, hash: u64) -> Option<&str> {
        self.0.get(&hash).map(String::as_str)
    }
}

/// `(level, type)` → text, from `unaging.additional.type_info_dic.*`.
#[derive(Debug, Default)]
pub struct TypeDic(pub HashMap<u16, HashMap<u64, String>>);

impl TypeDic {
    pub fn get(&self, level: u16, ty: u64) -> Option<&str> {
        self.0
            .get(&level)
            .and_then(|m| m.get(&ty))
            .map(String::as_str)
    }
}

/// `geTaskTypeMap` from `persistence/host/number_mapping.cpp`.
pub fn ge_task_type(t: u32) -> &'static str {
    match t {
        0 => "AI_CORE",
        1 => "AI_CPU",
        2 => "AI_VECTOR_CORE",
        3 => "WRITE_BACK",
        4 => "MIX_AIC",
        5 => "MIX_AIV",
        6 => "FFTS_PLUS",
        7 => "DSA_SQE",
        8 => "DVPP",
        9 => "COMMUNICATION",
        10 => "FUSION",
        11 => "INVALID",
        256 => "HCCL_AI_CPU",
        _ => "N/A",
    }
}

/// Load both dictionaries from `<PROF>/host/data`. Missing files yield empty
/// dictionaries; nodes then fall back to `N/A` placeholders.
pub fn load_dictionaries(host_data: &Path) -> (HashDic, TypeDic) {
    let mut hash = HashMap::new();
    for file in files_with_prefix(
        host_data,
        &[
            "unaging.additional.hash_dic.slice",
            "aging.additional.hash_dic.slice",
        ],
    ) {
        let Ok(text) = std::fs::read_to_string(&file) else {
            continue;
        };
        for line in text.lines() {
            let Some((key, value)) = line.split_once(':') else {
                continue;
            };
            if let Ok(key) = key.trim().parse::<u64>() {
                hash.insert(key, value.trim().to_string());
            }
        }
    }

    let mut types: HashMap<u16, HashMap<u64, String>> = HashMap::new();
    for file in files_with_prefix(
        host_data,
        &[
            "unaging.additional.type_info_dic.slice",
            "aging.additional.type_info_dic.slice",
        ],
    ) {
        let Ok(text) = std::fs::read_to_string(&file) else {
            continue;
        };
        for line in text.lines() {
            let Some((key, value)) = line.split_once(':') else {
                continue;
            };
            let Some((level, ty)) = key.split_once('_') else {
                continue;
            };
            let (Ok(level), Ok(ty)) = (level.parse::<u16>(), ty.parse::<u64>()) else {
                continue;
            };
            types
                .entry(level)
                .or_default()
                .insert(ty, value.trim().to_string());
        }
    }

    if hash.is_empty() {
        eprintln!(
            "warning: no hash_dic slices under {}; names will render as N/A",
            host_data.display()
        );
    }
    if types.is_empty() {
        eprintln!(
            "warning: no type_info_dic slices under {}; kernel types will render as N/A",
            host_data.display()
        );
    }
    (HashDic(hash), TypeDic(types))
}

fn files_with_prefix(dir: &Path, prefixes: &[&str]) -> Vec<PathBuf> {
    crate::stream::files_with_prefix(dir, prefixes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ge_task_type_table() {
        assert_eq!(ge_task_type(0), "AI_CORE");
        assert_eq!(ge_task_type(2), "AI_VECTOR_CORE");
        assert_eq!(ge_task_type(5), "MIX_AIV");
        assert_eq!(ge_task_type(999), "N/A");
    }
}
