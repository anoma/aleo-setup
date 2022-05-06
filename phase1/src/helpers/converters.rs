use crate::{ContributionMode, ProvingSystem};
use serde::{Deserialize, Serialize};

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub enum CurveKind {
    Bls12_381,
    Bls12_377,
    BW6,
}

pub fn curve_from_str(src: &str) -> Result<CurveKind, String> {
    let curve = match src.to_lowercase().as_str() {
        "bls12_381" => CurveKind::Bls12_381,
        "bls12_377" => CurveKind::Bls12_377,
        "bw6" => CurveKind::BW6,
        _ => return Err("unsupported curve".to_string()),
    };
    Ok(curve)
}

pub fn contribution_mode_from_str(src: &str) -> Result<ContributionMode, String> {
    let mode = match src.to_lowercase().as_str() {
        "full" => ContributionMode::Full,
        "chunked" => ContributionMode::Chunked,
        _ => return Err("unsupported contribution mode. Currently supported: full, chunked".to_string()),
    };
    Ok(mode)
}

pub fn proving_system_from_str(src: &str) -> Result<ProvingSystem, String> {
    let system = match src.to_lowercase().as_str() {
        "groth16" => ProvingSystem::Groth16,
        "marlin" => ProvingSystem::Marlin,
        _ => return Err("unsupported proving system. Currently supported: groth16, marlin".to_string()),
    };
    Ok(system)
}
