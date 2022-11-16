use crate::{
    environment::Environment,
    objects::{ContributionFileSignature, ContributionInfo, Round, TrimmedContributionInfo},
    CoordinatorError,
    CoordinatorState,
};
use phase2::helpers::CurveKind;
use snarkvm_curves::{bls12_377::Bls12_377, bw6_761::BW6_761};

use serde::{Deserialize, Serialize};
use std::{
    convert::TryFrom,
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
};

use super::Disk;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct ContributionLocator {
    round_height: u64,
    chunk_id: u64,
    contribution_id: u64,
    is_verified: bool,
}

// Parameters generated from `masp-mpc` crate have size 84_720_180, to this add the needed 64 bytes for the hash of the contribution that is placed at the head of the contribution file.
#[cfg(not(debug_assertions))]
pub const ANOMA_BASE_FILE_SIZE: u64 = 84_720_244; // 145_449_460 prod: 84_720_244, testing: 2_332
#[cfg(debug_assertions)]
pub const ANOMA_BASE_FILE_SIZE: u64 = 2_332; // prod: 84_720_244, testing: 2_332
// With `masp-mpc` the contribution file grows by 1632 bytes on each new contribution
#[cfg(not(debug_assertions))]
pub const ANOMA_PER_ROUND_FILE_SIZE_INCREASE: u64 = 1_632; // prod: 1_632, testing: 544
#[cfg(debug_assertions)]
pub const ANOMA_PER_ROUND_FILE_SIZE_INCREASE: u64 = 544; // prod: 1_632, testing: 544

impl ContributionLocator {
    pub fn new(round_height: u64, chunk_id: u64, contribution_id: u64, is_verified: bool) -> Self {
        Self {
            round_height,
            chunk_id,
            contribution_id,
            is_verified,
        }
    }

    pub fn round_height(&self) -> u64 {
        self.round_height
    }

    pub fn chunk_id(&self) -> u64 {
        self.chunk_id
    }

    pub fn contribution_id(&self) -> u64 {
        self.contribution_id
    }

    pub fn is_verified(&self) -> bool {
        self.is_verified
    }
}

#[derive(Debug, Default, Clone, Copy, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct ContributionSignatureLocator {
    round_height: u64,
    chunk_id: u64,
    contribution_id: u64,
    is_verified: bool,
}

impl ContributionSignatureLocator {
    pub fn new(round_height: u64, chunk_id: u64, contribution_id: u64, is_verified: bool) -> Self {
        Self {
            round_height,
            chunk_id,
            contribution_id,
            is_verified,
        }
    }

    pub fn round_height(&self) -> u64 {
        self.round_height
    }

    pub fn chunk_id(&self) -> u64 {
        self.chunk_id
    }

    pub fn contribution_id(&self) -> u64 {
        self.contribution_id
    }

    pub fn is_verified(&self) -> bool {
        self.is_verified
    }
}

/// A data structure representing all possible types of keys in storage.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum Locator {
    CoordinatorState,
    RoundHeight,
    RoundState { round_height: u64 },
    RoundFile { round_height: u64 },
    ContributionFile(ContributionLocator),
    ContributionFileSignature(ContributionSignatureLocator),
    ContributionInfoFile { round_height: u64 },
    ContributionsInfoSummary,
}

impl From<ContributionLocator> for Locator {
    fn from(locator: ContributionLocator) -> Self {
        Self::ContributionFile(locator)
    }
}

impl From<ContributionSignatureLocator> for Locator {
    fn from(locator: ContributionSignatureLocator) -> Self {
        Self::ContributionFileSignature(locator)
    }
}

/// A data structure representing all possible types of values in storage.
#[derive(Debug, Clone)]
pub enum Object {
    CoordinatorState(CoordinatorState),
    RoundHeight(u64),
    RoundState(Round),
    RoundFile(Vec<u8>),
    ContributionFile(Vec<u8>),
    ContributionFileSignature(ContributionFileSignature),
    ContributionInfoFile(ContributionInfo),
    ContributionsInfoSummary(Vec<TrimmedContributionInfo>),
}

impl Object {
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Object::CoordinatorState(state) => {
                serde_json::to_vec_pretty(state).expect("coordinator state to bytes failed")
            }
            Object::RoundHeight(height) => serde_json::to_vec(height).expect("round height to bytes failed"),
            Object::RoundState(round) => serde_json::to_vec_pretty(round).expect("round state to bytes failed"),
            Object::RoundFile(round) => round.to_vec(),
            Object::ContributionFile(contribution) => contribution.to_vec(),
            Object::ContributionFileSignature(signature) => {
                serde_json::to_vec_pretty(signature).expect("contribution file signature to bytes failed")
            }
            Object::ContributionInfoFile(info) => {
                serde_json::to_vec_pretty(info).expect("Contribution info file to bytes failed")
            }
            Object::ContributionsInfoSummary(summary) => {
                serde_json::to_vec_pretty(summary).expect("Contribution info summary to bytes failed")
            }
        }
    }

    /// Returns the size in bytes of the object.
    pub fn size(&self) -> u64 {
        match self {
            Object::CoordinatorState(_) => self.to_bytes().len() as u64,
            Object::RoundHeight(_) => self.to_bytes().len() as u64,
            Object::RoundState(_) => self.to_bytes().len() as u64,
            Object::RoundFile(round) => round.len() as u64,
            Object::ContributionFile(contribution) => contribution.len() as u64,
            Object::ContributionFileSignature(_) => self.to_bytes().len() as u64,
            Object::ContributionInfoFile(_) => self.to_bytes().len() as u64,
            Object::ContributionsInfoSummary(_) => self.to_bytes().len() as u64,
        }
    }

    /// Returns the expected file size of an aggregated round.
    pub fn round_file_size(environment: &Environment) -> u64 {
        let compressed = environment.compressed_inputs();
        let settings = environment.parameters();

        match settings.curve() {
            // TODO: change round_filesize
            CurveKind::Bls12_381 => ANOMA_BASE_FILE_SIZE,
            CurveKind::Bls12_377 => round_filesize!(Bls12_377, settings, compressed),
            CurveKind::BW6 => round_filesize!(BW6_761, settings, compressed),
        }
    }

    /// Returns the expected file size of a chunked contribution.
    pub fn contribution_file_size(environment: &Environment, chunk_id: u64, verified: bool) -> u64 {
        let settings = environment.parameters();
        let curve = settings.curve();

        let compressed = match verified {
            // The verified contribution file is used as *input* in the next computation.
            true => environment.compressed_inputs(),
            // The unverified contribution file the *output* of the current computation.
            false => environment.compressed_outputs(),
        };

        match (curve, verified) {
            // TODO: add correct verified_contribution_size
            (CurveKind::Bls12_381, true) => ANOMA_BASE_FILE_SIZE,
            (CurveKind::Bls12_381, false) => ANOMA_BASE_FILE_SIZE,
            (CurveKind::Bls12_377, true) => verified_contribution_size!(Bls12_377, settings, chunk_id, compressed),
            (CurveKind::Bls12_377, false) => unverified_contribution_size!(Bls12_377, settings, chunk_id, compressed),
            (CurveKind::BW6, true) => verified_contribution_size!(BW6_761, settings, chunk_id, compressed),
            (CurveKind::BW6, false) => unverified_contribution_size!(BW6_761, settings, chunk_id, compressed),
        }
    }

    /// Returns dynamically the expected file size of a contribution file.
    pub fn anoma_contribution_file_size(round_height: u64, contribution_id: u64) -> u64 {
        match round_height {
            0 => ANOMA_BASE_FILE_SIZE,
            _ => ANOMA_BASE_FILE_SIZE + (ANOMA_PER_ROUND_FILE_SIZE_INCREASE * (round_height + contribution_id - 1)),
        }
    }

    /// Returns the expected file size of a contribution signature.
    pub fn contribution_file_signature_size(verified: bool) -> u64 {
        // TODO (raychu86): Calculate contribution signature file size instead of using hard coded values.
        match verified {
            true => 628,  // Json object with signature + challenge_hash + response hash + next challenge hash
            false => 471, // Json object with signature + challenge_hash + response hash
        }
    }
}

pub trait ObjectReader: AsRef<[u8]> + Deref<Target = [u8]> {}

pub trait ObjectWriter: AsMut<[u8]> + DerefMut<Target = [u8]> {
    fn flush(&self) -> std::io::Result<()>;
}

/// The path to a resource defined by a [Locator].
#[derive(Clone, Serialize, Deserialize, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub struct LocatorPath(String);

impl AsRef<Path> for LocatorPath {
    fn as_ref(&self) -> &Path {
        self.as_path()
    }
}

impl Into<PathBuf> for LocatorPath {
    fn into(self) -> PathBuf {
        self.as_path().into()
    }
}

impl std::fmt::Debug for LocatorPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::fmt::Display for LocatorPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl LocatorPath {
    pub fn new(path: String) -> Self {
        Self(path)
    }

    pub fn as_path(&self) -> &Path {
        Path::new(&self.0)
    }
}

impl From<String> for LocatorPath {
    fn from(path: String) -> Self {
        Self::new(path)
    }
}

impl From<&str> for LocatorPath {
    fn from(path: &str) -> Self {
        Self::new(path.to_owned())
    }
}

impl TryFrom<&Path> for LocatorPath {
    type Error = CoordinatorError;

    fn try_from(path: &Path) -> Result<Self, Self::Error> {
        path.to_str()
            .ok_or(CoordinatorError::StorageLocatorFormatIncorrect)
            .map(|s| Self::new(s.to_owned()))
    }
}

/// An enum containing a [Locator] or [LocatorPath].
///
/// **Note:** This can probably be refactored out in the future so
/// that we only use [Locator].
#[derive(Clone, PartialEq, Debug)]
pub enum LocatorOrPath {
    Path(LocatorPath),
    Locator(Locator),
}

impl LocatorOrPath {
    pub fn try_into_locator(self, storage: &Disk) -> Result<Locator, CoordinatorError> {
        match self {
            LocatorOrPath::Path(path) => storage.to_locator(&path),
            LocatorOrPath::Locator(locator) => Ok(locator),
        }
    }

    pub fn try_into_path(self, storage: &Disk) -> Result<LocatorPath, CoordinatorError> {
        match self {
            LocatorOrPath::Path(path) => Ok(path),
            LocatorOrPath::Locator(locator) => storage.to_path(&locator),
        }
    }
}

impl From<LocatorPath> for LocatorOrPath {
    fn from(path: LocatorPath) -> Self {
        Self::Path(path)
    }
}

impl From<Locator> for LocatorOrPath {
    fn from(locator: Locator) -> Self {
        Self::Locator(locator)
    }
}

/// An action to remove an item from [Storage].
#[derive(Clone, PartialEq, Debug)]
pub struct RemoveAction {
    locator_or_path: LocatorOrPath,
}

impl RemoveAction {
    /// Create a new [RemoveAction]
    pub fn new(locator: impl Into<LocatorOrPath>) -> Self {
        Self {
            locator_or_path: locator.into(),
        }
    }

    /// Obtain the location of the item to be removed from [Storage]
    /// as a [LocatorOrPath].
    pub fn locator_or_path(&self) -> &LocatorOrPath {
        &self.locator_or_path
    }

    /// Obtain the location of the item to be removed from [Storage]
    /// as a [Locator].
    pub fn try_into_locator(self, storage: &Disk) -> Result<Locator, CoordinatorError> {
        self.locator_or_path.try_into_locator(storage)
    }

    pub fn try_into_path(self, storage: &Disk) -> Result<LocatorPath, CoordinatorError> {
        self.locator_or_path.try_into_path(storage)
    }
}

/// An action to update an item in [Storage].
pub struct UpdateAction {
    pub locator: Locator,
    pub object: Object,
}

/// An action taken to mutate [Storage], which can be processed by
/// [Storage::process()].
#[non_exhaustive]
pub enum StorageAction {
    Remove(RemoveAction),
    Update(UpdateAction),
    ClearRoundFiles(u64),
}

pub trait StorageLocator {
    /// Returns a locator path corresponding to the given locator.
    fn to_path(&self, locator: &Locator) -> Result<LocatorPath, CoordinatorError>;

    /// Returns a locator corresponding to the given locator path string.
    fn to_locator(&self, path: &LocatorPath) -> Result<Locator, CoordinatorError>;
}

pub trait StorageObject {
    type Reader: ObjectReader;
    type Writer: ObjectWriter;

    /// Returns an object reader for the given locator.
    fn reader(&self, locator: &Locator) -> Result<Self::Reader, CoordinatorError>;

    /// Returns an object writer for the given locator.
    fn writer(&self, locator: &Locator) -> Result<Self::Writer, CoordinatorError>;
}
