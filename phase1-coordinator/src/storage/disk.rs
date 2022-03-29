use crate::{
    environment::Environment,
    objects::{ContributionFileSignature, Round},
    storage::{
        ContributionLocator, ContributionSignatureLocator, Locator, Object, ObjectReader, ObjectWriter, StorageLocator,
        StorageObject,
    },
    CoordinatorError, CoordinatorState,
};

use anyhow::Result;
use fs_err::{self as fs, File, OpenOptions};
use itertools::Itertools;
use memmap::MmapOptions;

use std::{
    convert::TryFrom,
    io::{Read, Write},
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
    str::FromStr,
};
use tracing::{debug, error, trace};

use super::{LocatorPath, StorageAction};

#[derive(Debug)]
pub struct Disk {
    environment: Environment,
    resolver: DiskResolver,
}

impl Disk {
    /// Loads a new instance of `Disk`.
    pub fn load(environment: &Environment) -> Result<Self, CoordinatorError>
    where
        Self: Sized,
    {
        trace!("Loading disk storage");

        // Check the base directory exists.
        if !Path::new(environment.local_base_directory()).exists() {
            // Create the base directory if it does not exist.
            fs::create_dir_all(environment.local_base_directory()).expect("unable to create the base directory");
        }

        // Create a new `Storage` instance, and set the `Environment`.
        let mut storage = Self {
            environment: environment.clone(),
            resolver: DiskResolver::new(environment.local_base_directory()),
        };

        // Create the coordinator state locator if it does not exist yet.
        if !storage.exists(&Locator::CoordinatorState) {
            storage.insert(
                Locator::CoordinatorState,
                Object::CoordinatorState(CoordinatorState::new(environment.clone())),
            )?;
        }

        trace!("Loaded disk storage");
        Ok(storage)
    }

    /// Initializes the location corresponding to the given locator.
    pub fn initialize(&mut self, locator: Locator, size: u64) -> Result<(), CoordinatorError> {
        let locator_path = self.to_path(&locator)?;
        trace!("Initializing {:?}", locator_path);

        // Check that the locator does not already exist in storage.
        if self.exists(&locator) {
            error!(
                "Locator {:?} in call to initialize() already exists in storage.",
                locator_path
            );
            return Err(CoordinatorError::StorageLocatorAlreadyExists);
        }

        // If the locator is a contribution file, initialize its directory.
        if let Locator::ContributionFile(contribution_locator) = locator {
            self.resolver
                .chunk_directory_init(contribution_locator.round_height(), contribution_locator.chunk_id());
        }

        // Open the file.
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(locator_path)?;

        // Set the initial file size.
        file.set_len(size)?;

        trace!("Initialized {}", self.to_path(&locator)?);
        Ok(())
    }

    /// Checks whether the given locator exists in the storage or not.
    pub fn exists(&self, locator: &Locator) -> bool {
        let path = match self.to_path(locator) {
            Ok(path) => path,
            Err(e) => {
                error!("Could not convert locator to path - {}", e);
                return false;
            }
        };

        trace!("Ensuring that {} exists in storage", path);
        match fs::metadata(path) {
            Ok(metadata) => metadata.is_file(),
            Err(_) => false,
        }
    }

    /// Returns a copy of an object at the given locator in storage, if it exists.
    pub fn get(&self, locator: &Locator) -> Result<Object, CoordinatorError> {
        let path = self.to_path(locator)?;
        trace!("Fetching {}", path);

        // Check that the given locator exists in storage.
        if !self.exists(locator) {
            error!("Locator missing in call to get() in storage - {:?}", locator);
            return Err(CoordinatorError::StorageLocatorMissing);
        }

        // read the file to a byte array
        let file_bytes = fs::read(path)?;

        let object = match locator {
            Locator::CoordinatorState => {
                let coordinator_state: CoordinatorState = serde_json::from_slice(&file_bytes)?;
                Ok(Object::CoordinatorState(coordinator_state))
            }
            Locator::RoundHeight => {
                let round_height: u64 = serde_json::from_slice(&file_bytes)?;
                Ok(Object::RoundHeight(round_height))
            }
            Locator::RoundState { round_height: _ } => {
                let round: Round = serde_json::from_slice(&file_bytes)?;
                Ok(Object::RoundState(round))
            }
            Locator::RoundFile { round_height } => {
                // Check that the round size is correct.
                let expected_size = Object::round_file_size(&self.environment);
                let found_size = file_bytes.len() as u64;
                debug!("Round {} filesize is {}", round_height, found_size);
                if found_size == 0 || expected_size != found_size {
                    error!("Round file size should be {} but found {}", expected_size, found_size);
                    return Err(CoordinatorError::RoundFileSizeMismatch.into());
                }

                Ok(Object::RoundFile(file_bytes))
            }
            Locator::ContributionFile(contribution_locator) => {
                // Check that the contribution size is correct.
                let expected_size = Object::contribution_file_size(
                    &self.environment,
                    contribution_locator.chunk_id(),
                    contribution_locator.is_verified(),
                );
                let found_size = file_bytes.len() as u64;
                debug!(
                    "Round {} chunk {} filesize is {}",
                    contribution_locator.round_height(),
                    contribution_locator.chunk_id(),
                    found_size
                );
                // if found_size == 0 || expected_size != found_size {
                //     error!(
                //         "Contribution file size should be {} but found {}",
                //         expected_size, found_size
                //     );
                //     return Err(CoordinatorError::ContributionFileSizeMismatch.into());
                // }

                let mut contribution_file: Vec<u8> = Vec::with_capacity(expected_size as usize);
                contribution_file.write_all(&file_bytes)?;
                Ok(Object::ContributionFile(contribution_file))
            }
            Locator::ContributionFileSignature(contribution_locator) => {
                // Check that the contribution file signature size is correct.
                let expected_size = Object::contribution_file_signature_size(contribution_locator.is_verified());
                let found_size = file_bytes.len() as u64;
                debug!(
                    "Round {} chunk {} contribution {} signature filesize is {}",
                    contribution_locator.round_height(),
                    contribution_locator.chunk_id(),
                    contribution_locator.contribution_id(),
                    found_size
                );
                if found_size == 0 || expected_size != found_size {
                    error!(
                        "Contribution signature file size should be {} but found {}",
                        expected_size, found_size
                    );
                    return Err(CoordinatorError::ContributionSignatureFileSizeMismatch.into());
                }

                let contribution_file_signature: ContributionFileSignature = serde_json::from_slice(&file_bytes)?;
                Ok(Object::ContributionFileSignature(contribution_file_signature))
            }
        };

        trace!("Fetched {}", self.to_path(locator)?);
        object
    }

    /// Inserts a new object at the given locator into storage, if it does not exist.
    pub fn insert(&mut self, locator: Locator, object: Object) -> Result<(), CoordinatorError> {
        trace!("Inserting {}", self.to_path(&locator)?);

        // Check that the given locator does not exist in storage.
        if self.exists(&locator) {
            error!("Locator in call to insert() already exists in storage.");
            return Err(CoordinatorError::StorageLocatorAlreadyExists);
        }

        // Initialize the new file with the object size.
        self.initialize(locator.clone(), object.size())?;

        // Insert the object at the given locator.
        self.update(&locator, object)?;

        trace!("Inserted {}", self.to_path(&locator)?);
        Ok(())
    }

    /// Updates an existing object for the given locator in storage, if it exists.
    pub fn update(&mut self, locator: &Locator, object: Object) -> Result<(), CoordinatorError> {
        let path = self.to_path(locator)?;
        trace!("Updating {}", path);

        // Check that the given locator exists in storage.
        if !self.exists(locator) {
            error!("Locator missing in call to update() in storage.");
            return Err(CoordinatorError::StorageLocatorMissing);
        }

        let mut file = OpenOptions::new().write(true).open(path)?;
        file.set_len(object.size())?;
        file.write_all(&object.to_bytes())?;
        // Sync all in-memory data to disk.
        file.flush()?;

        trace!("Updated {}", self.to_path(&locator)?);
        Ok(())
    }

    /// Copies an object from the given source locator to the given destination locator.
    pub fn copy(&mut self, source_locator: &Locator, destination_locator: &Locator) -> Result<(), CoordinatorError> {
        trace!(
            "Copying from A to B\n\n\tA: {}\n\tB: {}\n",
            self.to_path(source_locator)?,
            self.to_path(destination_locator)?
        );

        // Check that the given source locator exists in storage.
        if !self.exists(source_locator) {
            error!("Source locator missing in call to copy() in storage.");
            return Err(CoordinatorError::StorageLocatorMissing);
        }

        // Check that the given destination locator does NOT exist in storage.
        if self.exists(destination_locator) {
            error!("Destination locator in call to copy() already exists in storage.");
            return Err(CoordinatorError::StorageLocatorAlreadyExists);
        }

        // Fetch the source object.
        let source_object = self.get(source_locator)?;

        // Initialize the destination file with the source object size.initializeinitialize
        self.initialize(destination_locator.clone(), source_object.size())?;

        // Update the destination locator with the copied source object.
        self.update(destination_locator, source_object)?;

        trace!("Copied to {}", self.to_path(destination_locator)?);
        Ok(())
    }

    /// Removes the object corresponding to the given locator from storage.
    pub fn remove(&mut self, locator: &Locator) -> Result<(), CoordinatorError> {
        let path = self.to_path(locator)?;
        trace!("Removing {}", path);

        // Check that the locator exists in storage.
        if !self.exists(&locator) {
            error!("Locator in call to remove() doesn't exist in storage.");
            return Err(CoordinatorError::StorageLocatorMissing);
        }

        // TODO: if any of the locators are directories, make this
        // detect whether the path is a directory of a file and call
        // the appropriate function.
        fs::remove_file(path.clone())?;

        trace!("Removed {}", path);
        Ok(())
    }

    /// Returns the size of the object stored at the given locator.
    pub fn size(&self, locator: &Locator) -> Result<u64, CoordinatorError> {
        let path = self.to_path(locator)?;
        trace!("Fetching size of {}", path);

        // Check that the given locator exists in storage.
        if !self.exists(locator) {
            error!("Locator missing in call to size() in storage.");
            return Err(CoordinatorError::StorageLocatorMissing);
        }

        // Open the file.
        let file = OpenOptions::new().read(true).write(true).open(path.clone())?;

        trace!("Fetched size of {}", path);
        Ok(file.metadata()?.len())
    }

    /// Process a [StorageAction] which mutates the storage.
    pub fn process(&mut self, action: StorageAction) -> Result<()> {
        match action {
            StorageAction::Remove(remove_action) => {
                let locator = remove_action.try_into_locator(self)?;
                Ok(self.remove(&locator)?)
            }
            StorageAction::Update(update_action) => Ok(self.update(&update_action.locator, update_action.object)?),
            StorageAction::ClearRoundFiles(round_height) => Ok(self.clear_round_files(round_height)),
        }
    }

    /// Clears all files related to a round - used for round reset purposes.
    fn clear_round_files(&mut self, round_height: u64) {
        // Let's first fully clear any files in the next round - these will be
        // verifications and represent the initial challenges.
        let next_round_dir = self.resolver.round_directory(round_height + 1);
        self.clear_dir_files(next_round_dir.into(), true);

        // Now, let's clear all the contributions made on this round.
        let round_dir = self.resolver.round_directory(round_height);
        self.clear_dir_files(round_dir.into(), false);
    }

    fn clear_dir_files(&mut self, path: PathBuf, delete_initial_contribution: bool) {
        let entries = match fs::read_dir(path.as_path()) {
            Ok(entries) => entries,
            Err(e) => {
                tracing::warn!("Could not read directory at {:?} - {:?}", path, e);
                return;
            }
        };

        for entry in entries {
            if let Err(e) = entry {
                tracing::error!("Found erroneous entry - {:?}", e);
                continue;
            }

            let entry = entry.unwrap();

            match entry.path().is_dir() {
                true => self.clear_dir_files(entry.path(), delete_initial_contribution),
                false => {
                    let file_path = match entry.path().to_str() {
                        Some(file_path) => file_path.to_owned(),
                        None => {
                            tracing::error!("Could not turn fs entry into file path");
                            continue;
                        }
                    };

                    if !delete_initial_contribution && file_path.contains("contribution_0")
                        || file_path.contains("state.json")
                    {
                        continue;
                    }

                    let locator = match self.resolver.to_locator(&LocatorPath::new(file_path)) {
                        Ok(locator) => locator,
                        Err(e) => {
                            tracing::error!("Could not turn file path into locator - {:?}", e);
                            continue;
                        }
                    };

                    if let Err(e) = self.remove(&locator) {
                        tracing::error!("Could not remove locator - {:?}", e);
                    }
                }
            };
        }
    }
}

impl StorageLocator for Disk {
    #[inline]
    fn to_path(&self, locator: &Locator) -> Result<LocatorPath, CoordinatorError> {
        self.resolver.to_path(locator)
    }

    #[inline]
    fn to_locator(&self, path: &LocatorPath) -> Result<Locator, CoordinatorError> {
        self.resolver.to_locator(path)
    }
}

pub struct DiskObjectReader {
    data: Vec<u8>,
}

impl Deref for DiskObjectReader {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &*self.data
    }
}

impl AsRef<[u8]> for DiskObjectReader {
    fn as_ref(&self) -> &[u8] {
        self.data.as_ref()
    }
}

impl ObjectReader for DiskObjectReader {}

pub struct DiskObjectWriter {
    _file: File,
    memmap: memmap::MmapMut,
}

impl Deref for DiskObjectWriter {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &*self.memmap
    }
}

impl DerefMut for DiskObjectWriter {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut *self.memmap
    }
}

impl AsMut<[u8]> for DiskObjectWriter {
    fn as_mut(&mut self) -> &mut [u8] {
        self.memmap.as_mut()
    }
}

impl ObjectWriter for DiskObjectWriter {
    fn flush(&self) -> std::io::Result<()> {
        self.memmap.flush()
    }
}

impl StorageObject for Disk {
    type Reader = DiskObjectReader;
    type Writer = DiskObjectWriter;

    /// Returns an object reader for the given locator.
    #[inline]
    fn reader<'a>(&self, locator: &Locator) -> Result<Self::Reader, CoordinatorError> {
        let path = self.to_path(&locator)?;

        // Check that the locator exists in storage.
        if !self.exists(&locator) {
            error!("Locator {} missing in call to reader() in storage.", path);
            return Err(CoordinatorError::StorageLocatorMissing);
        }

        let file = OpenOptions::new().read(true).open(path)?;

        // Load the file into memory.
        let mut data = vec![];
        file.file()
            .read_to_end(&mut data)
            .map_err(|e| CoordinatorError::IOError(e))?;

        // match locator {
        //     Locator::RoundFile { round_height } => {
        //         // Check that the round size is correct.
        //         let expected_size = Object::round_file_size(&self.environment);
        //         let found_size = data.len() as u64;
        //         debug!("Round {} filesize is {}", round_height, found_size);
        //         if found_size != expected_size {
        //             error!(
        //                 "Contribution file size should be {} but found {}",
        //                 expected_size, found_size
        //             );
        //             return Err(CoordinatorError::RoundFileSizeMismatch.into());
        //         }
        //     }
        //     Locator::ContributionFile(contribution_locator) => {
        //         // Check that the contribution size is correct.
        //         let expected_size = Object::contribution_file_size(
        //             &self.environment,
        //             contribution_locator.chunk_id(),
        //             contribution_locator.is_verified(),
        //         );
        //         let found_size = data.len() as u64;
        //         debug!(
        //             "Round {} chunk {} filesize is {}",
        //             contribution_locator.round_height(),
        //             contribution_locator.chunk_id(),
        //             found_size
        //         );
        //         if found_size != expected_size {
        //             error!(
        //                 "Contribution file size should be {} but found {}",
        //                 expected_size, found_size
        //             );
        //             return Err(CoordinatorError::ContributionFileSizeMismatch.into());
        //         }
        //     }
        //     _ => {}
        // }

        Ok(DiskObjectReader { data })
    }

    /// Returns an object writer for the given locator.
    #[inline]
    fn writer(&self, locator: &Locator) -> Result<Self::Writer, CoordinatorError> {
        let path = self.to_path(&locator)?;

        // Check that the locator exists in storage.
        if !self.exists(&locator) {
            error!("Locator {} missing in call to writer() in storage.", path);
            return Err(CoordinatorError::StorageLocatorMissing);
        }

        let file = OpenOptions::new().read(true).write(true).open(path)?;

        // Load the file into memory.
        let memmap = unsafe { MmapOptions::new().map_mut(&file.file())? };
/*
        match locator {
            Locator::RoundFile { round_height: _ } => {
                // Check that the round size is correct.
                let expected_size = Object::round_file_size(&self.environment);
                let found_size = memmap.len() as u64;
                debug!("File size of {} is {}", self.to_path(locator)?, found_size);
                if found_size != expected_size {
                    error!(
                        "Contribution file size should be {} but found {}",
                        expected_size, found_size
                    );
                    return Err(CoordinatorError::RoundFileSizeMismatch.into());
                }
            }
            Locator::ContributionFile(contribution_locator) => {
                // Check that the contribution size is correct.
                let expected_size = Object::contribution_file_size(
                    &self.environment,
                    contribution_locator.chunk_id(),
                    contribution_locator.is_verified(),
                );
                let found_size = memmap.len() as u64;
                debug!("File size of {} is {}", self.to_path(locator)?, found_size);
                if found_size != expected_size {
                    error!(
                        "Contribution file size should be {} but found {}",
                        expected_size, found_size
                    );
                    return Err(CoordinatorError::ContributionFileSizeMismatch.into());
                }
            }
            _ => {}
        }
        */

        Ok(DiskObjectWriter { _file: file, memmap })
    }
}

#[derive(Debug)]
struct DiskResolver {
    base: String,
}

impl DiskResolver {
    #[inline]
    fn new(base: &str) -> Self {
        Self { base: base.to_string() }
    }
}

impl StorageLocator for DiskResolver {
    #[inline]
    fn to_path(&self, locator: &Locator) -> Result<LocatorPath, CoordinatorError> {
        let path = match locator {
            Locator::CoordinatorState => format!("{}/coordinator.json", self.base),
            Locator::RoundHeight => format!("{}/round_height", self.base),
            Locator::RoundState { round_height } => format!("{}/state.json", self.round_directory(*round_height)),
            Locator::RoundFile { round_height } => {
                let round_directory = self.round_directory(*round_height);
                format!("{}/round_{}.verified", round_directory, *round_height)
            }
            Locator::ContributionFile(contribution_locator) => {
                // Fetch the chunk directory path.
                let path = self.chunk_directory(contribution_locator.round_height(), contribution_locator.chunk_id());
                match contribution_locator.is_verified() {
                    // Set the contribution locator as `{chunk_directory}/contribution_{contribution_id}.verified`.
                    true => format!(
                        "{}/contribution_{}.verified",
                        path,
                        contribution_locator.contribution_id()
                    ),
                    // Set the contribution locator as `{chunk_directory}/contribution_{contribution_id}.unverified`.
                    false => format!(
                        "{}/contribution_{}.unverified",
                        path,
                        contribution_locator.contribution_id()
                    ),
                }
            }
            Locator::ContributionFileSignature(contribution_signature_locator) => {
                // Fetch the chunk directory path.
                let path = self.chunk_directory(
                    contribution_signature_locator.round_height(),
                    contribution_signature_locator.chunk_id(),
                );
                match contribution_signature_locator.is_verified() {
                    // Set the contribution locator as `{chunk_directory}/contribution_{contribution_id}.verified.signature`.
                    true => format!(
                        "{}/contribution_{}.verified.signature",
                        path,
                        contribution_signature_locator.contribution_id()
                    ),
                    // Set the contribution locator as `{chunk_directory}/contribution_{contribution_id}.unverified.signature`.
                    false => format!(
                        "{}/contribution_{}.unverified.signature",
                        path,
                        contribution_signature_locator.contribution_id()
                    ),
                }
            }
        };
        // Sanitize the path.
        LocatorPath::try_from(Path::new(&path))
    }

    #[inline]
    fn to_locator(&self, path: &LocatorPath) -> Result<Locator, CoordinatorError> {
        // Sanitize the given path and base to the local OS.
        let mut path = path.to_string();
        let path = {
            // TODO (howardwu): Change this to support absolute paths and OS specific path
            //   conventions that may be non-standard. For now, restrict this to relative paths.
            if !path.starts_with("./") {
                path = format!("./{}", path);
            }

            let path = Path::new(path.as_str());
            let base = Path::new(&self.base);

            // Check that the path matches the expected base.
            if !path.starts_with(base) {
                error!("{:?} does not start with {:?}", path, base);
                return Err(CoordinatorError::StorageLocatorFormatIncorrect);
            }

            path
        };

        // Strip the base prefix.
        let key = path
            .strip_prefix(&format!("{}/", self.base))
            .map_err(|_| CoordinatorError::StorageLocatorFormatIncorrect)?;

        let key = key.to_str().ok_or(CoordinatorError::StorageLocatorFormatIncorrect)?;

        // Check if it matches the coordinator state file.
        if key == "coordinator.json" {
            return Ok(Locator::CoordinatorState);
        }

        // Check if it matches the round height.
        if key == "round_height" {
            return Ok(Locator::RoundHeight);
        }

        // Parse the key into its components.
        if let Some((round, remainder)) = key.splitn(2, "/").collect_tuple() {
            // Check if it resembles the round directory.
            if round.starts_with("round_") {
                // Attempt to parse the round string for the round height.
                let round_height = u64::from_str(
                    round
                        .strip_prefix("round_")
                        .ok_or(CoordinatorError::StorageLocatorFormatIncorrect)?,
                )?;

                // Check if it matches the round directory.
                if round == &format!("round_{}", round_height) {
                    /* In round directory */

                    // Check if it matches the round state.
                    if remainder == "state.json" {
                        return Ok(Locator::RoundState { round_height });
                    }

                    // Check if it matches the round file.
                    if remainder == format!("round_{}.verified", round_height) {
                        return Ok(Locator::RoundFile { round_height });
                    }

                    // Parse the path into its components.
                    if let Some((chunk, path)) = remainder.splitn(2, "/").collect_tuple() {
                        // Check if it resembles the chunk directory.
                        if chunk.starts_with("chunk_") {
                            // Attempt to parse the path string for the chunk ID.
                            let chunk_id = u64::from_str(
                                chunk
                                    .strip_prefix("chunk_")
                                    .ok_or(CoordinatorError::StorageLocatorFormatIncorrect)?,
                            )?;

                            // Check if it matches the chunk directory.
                            if chunk == &format!("chunk_{}", chunk_id) {
                                /* In chunk directory */

                                // Check if it matches the contribution file.
                                if path.starts_with("contribution_") {
                                    let (id, extension) = path
                                        .strip_prefix("contribution_")
                                        .ok_or(CoordinatorError::StorageLocatorFormatIncorrect)?
                                        .splitn(2, '.')
                                        .collect_tuple()
                                        .ok_or(CoordinatorError::StorageLocatorFormatIncorrect)?;
                                    let contribution_id = u64::from_str(id)?;

                                    // Check if it matches a contribution file signature for an unverified contribution.
                                    if extension == "unverified.signature" {
                                        return Ok(Locator::ContributionFileSignature(
                                            ContributionSignatureLocator::new(
                                                round_height,
                                                chunk_id,
                                                contribution_id,
                                                false,
                                            ),
                                        ));
                                    }

                                    // Check if it matches a contribution file signature for a verified contribution.
                                    if extension == "verified.signature" {
                                        return Ok(Locator::ContributionFileSignature(
                                            ContributionSignatureLocator::new(
                                                round_height,
                                                chunk_id,
                                                contribution_id,
                                                true,
                                            ),
                                        ));
                                    }

                                    // Check if it matches a unverified contribution file.
                                    if extension == "unverified" {
                                        return Ok(Locator::ContributionFile(ContributionLocator::new(
                                            round_height,
                                            chunk_id,
                                            contribution_id,
                                            false,
                                        )));
                                    }

                                    // Check if it matches a unverified contribution file.
                                    if extension == "verified" {
                                        return Ok(Locator::ContributionFile(ContributionLocator::new(
                                            round_height,
                                            chunk_id,
                                            contribution_id,
                                            true,
                                        )));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Err(CoordinatorError::StorageLocatorFormatIncorrect)
    }
}

impl DiskResolver {
    /// Returns the round directory for a given round height from the coordinator.
    #[inline]
    fn round_directory(&self, round_height: u64) -> String {
        format!("{}/round_{}", self.base, round_height)
    }

    /// Returns the chunk directory for a given round height and chunk ID from the coordinator.
    #[inline]
    fn chunk_directory(&self, round_height: u64, chunk_id: u64) -> String {
        // Fetch the transcript directory path.
        let path = self.round_directory(round_height);

        // Format the chunk directory as `{round_directory}/chunk_{chunk_id}`.
        format!("{}/chunk_{}", path, chunk_id)
    }

    /// Initializes the chunk directory for a given  round height, and chunk ID.
    #[inline]
    fn chunk_directory_init(&self, round_height: u64, chunk_id: u64) {
        // If the round directory does not exist, attempt to initialize the directory path.
        let path = self.round_directory(round_height);
        if !Path::new(&path).exists() {
            fs::create_dir_all(&path).expect("unable to create the round directory");
        }

        // If the chunk directory does not exist, attempt to initialize the directory path.
        let path = self.chunk_directory(round_height, chunk_id);
        if !Path::new(&path).exists() {
            fs::create_dir_all(&path).expect("unable to create the chunk directory");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // use crate::testing::prelude::*;

    #[test]
    fn test_to_path_coordinator_state() {
        let locator = DiskResolver::new("./transcript/test");

        assert_eq!(
            LocatorPath::from("./transcript/test/coordinator.json"),
            locator.to_path(&Locator::CoordinatorState).unwrap()
        );
    }

    #[test]
    fn test_to_locator_coordinator_state() {
        let locator = DiskResolver::new("./transcript/test");

        assert_eq!(
            Locator::CoordinatorState,
            locator
                .to_locator(&"./transcript/test/coordinator.json".into())
                .unwrap(),
        );
    }

    #[test]
    fn test_to_path_round_height() {
        let locator = DiskResolver::new("./transcript/test");

        assert_eq!(
            LocatorPath::from("./transcript/test/round_height"),
            locator.to_path(&Locator::RoundHeight).unwrap()
        );
    }

    #[test]
    fn test_to_locator_round_height() {
        let locator = DiskResolver::new("./transcript/test");

        assert_eq!(
            Locator::RoundHeight,
            locator.to_locator(&"./transcript/test/round_height".into()).unwrap(),
        );
    }

    #[test]
    fn test_to_path_round_state() {
        let locator = DiskResolver::new("./transcript/test");

        assert_eq!(
            LocatorPath::from("./transcript/test/round_0/state.json"),
            locator.to_path(&Locator::RoundState { round_height: 0 }).unwrap()
        );
        assert_eq!(
            LocatorPath::from("./transcript/test/round_1/state.json"),
            locator.to_path(&Locator::RoundState { round_height: 1 }).unwrap()
        );
        assert_eq!(
            LocatorPath::from("./transcript/test/round_2/state.json"),
            locator.to_path(&Locator::RoundState { round_height: 2 }).unwrap()
        );
    }

    #[test]
    fn test_to_locator_round_state() {
        let locator = DiskResolver::new("./transcript/test");

        assert_eq!(
            Locator::RoundState { round_height: 0 },
            locator
                .to_locator(&"./transcript/test/round_0/state.json".into())
                .unwrap(),
        );
        assert_eq!(
            Locator::RoundState { round_height: 1 },
            locator
                .to_locator(&"./transcript/test/round_1/state.json".into())
                .unwrap(),
        );
        assert_eq!(
            Locator::RoundState { round_height: 2 },
            locator
                .to_locator(&"./transcript/test/round_2/state.json".into())
                .unwrap(),
        );
    }

    #[test]
    fn test_to_path_round_file() {
        let locator = DiskResolver::new("./transcript/test");

        assert_eq!(
            LocatorPath::from("./transcript/test/round_0/round_0.verified"),
            locator.to_path(&Locator::RoundFile { round_height: 0 }).unwrap()
        );
        assert_eq!(
            LocatorPath::from("./transcript/test/round_1/round_1.verified"),
            locator.to_path(&Locator::RoundFile { round_height: 1 }).unwrap()
        );
        assert_eq!(
            LocatorPath::from("./transcript/test/round_2/round_2.verified"),
            locator.to_path(&Locator::RoundFile { round_height: 2 }).unwrap()
        );
    }

    #[test]
    fn test_to_locator_round_file() {
        let locator = DiskResolver::new("./transcript/test");

        assert_eq!(
            Locator::RoundFile { round_height: 0 },
            locator
                .to_locator(&"./transcript/test/round_0/round_0.verified".into())
                .unwrap(),
        );
        assert_eq!(
            Locator::RoundFile { round_height: 1 },
            locator
                .to_locator(&"./transcript/test/round_1/round_1.verified".into())
                .unwrap(),
        );
        assert_eq!(
            Locator::RoundFile { round_height: 2 },
            locator
                .to_locator(&"./transcript/test/round_2/round_2.verified".into())
                .unwrap(),
        );
    }

    #[test]
    fn test_to_path_contribution_file() {
        let locator = DiskResolver::new("./transcript/test");

        assert_eq!(
            LocatorPath::from("./transcript/test/round_0/chunk_0/contribution_0.unverified"),
            locator
                .to_path(&Locator::ContributionFile(ContributionLocator::new(0, 0, 0, false)))
                .unwrap()
        );
        assert_eq!(
            LocatorPath::from("./transcript/test/round_0/chunk_0/contribution_0.verified"),
            locator
                .to_path(&Locator::ContributionFile(ContributionLocator::new(0, 0, 0, true)))
                .unwrap()
        );

        assert_eq!(
            LocatorPath::from("./transcript/test/round_1/chunk_0/contribution_0.unverified"),
            locator
                .to_path(&Locator::ContributionFile(ContributionLocator::new(1, 0, 0, false)))
                .unwrap()
        );
        assert_eq!(
            LocatorPath::from("./transcript/test/round_1/chunk_0/contribution_0.verified"),
            locator
                .to_path(&Locator::ContributionFile(ContributionLocator::new(1, 0, 0, true)))
                .unwrap()
        );

        assert_eq!(
            LocatorPath::from("./transcript/test/round_0/chunk_1/contribution_0.unverified"),
            locator
                .to_path(&Locator::ContributionFile(ContributionLocator::new(0, 1, 0, false)))
                .unwrap()
        );
        assert_eq!(
            LocatorPath::from("./transcript/test/round_0/chunk_1/contribution_0.verified"),
            locator
                .to_path(&Locator::ContributionFile(ContributionLocator::new(0, 1, 0, true)))
                .unwrap()
        );

        assert_eq!(
            LocatorPath::from("./transcript/test/round_0/chunk_0/contribution_1.unverified"),
            locator
                .to_path(&Locator::ContributionFile(ContributionLocator::new(0, 0, 1, false)))
                .unwrap()
        );
        assert_eq!(
            LocatorPath::from("./transcript/test/round_0/chunk_0/contribution_1.verified"),
            locator
                .to_path(&Locator::ContributionFile(ContributionLocator::new(0, 0, 1, true)))
                .unwrap()
        );

        assert_eq!(
            LocatorPath::from("./transcript/test/round_1/chunk_1/contribution_0.unverified"),
            locator
                .to_path(&Locator::ContributionFile(ContributionLocator::new(1, 1, 0, false)))
                .unwrap()
        );
        assert_eq!(
            LocatorPath::from("./transcript/test/round_1/chunk_1/contribution_0.verified"),
            locator
                .to_path(&Locator::ContributionFile(ContributionLocator::new(1, 1, 0, true)))
                .unwrap()
        );

        assert_eq!(
            LocatorPath::from("./transcript/test/round_1/chunk_0/contribution_1.unverified"),
            locator
                .to_path(&Locator::ContributionFile(ContributionLocator::new(1, 0, 1, false)))
                .unwrap()
        );
        assert_eq!(
            LocatorPath::from("./transcript/test/round_1/chunk_0/contribution_1.verified"),
            locator
                .to_path(&Locator::ContributionFile(ContributionLocator::new(1, 0, 1, true)))
                .unwrap()
        );

        assert_eq!(
            LocatorPath::from("./transcript/test/round_0/chunk_1/contribution_1.unverified"),
            locator
                .to_path(&Locator::ContributionFile(ContributionLocator::new(0, 1, 1, false)))
                .unwrap()
        );
        assert_eq!(
            LocatorPath::from("./transcript/test/round_0/chunk_1/contribution_1.verified"),
            locator
                .to_path(&Locator::ContributionFile(ContributionLocator::new(0, 1, 1, true)))
                .unwrap()
        );

        assert_eq!(
            LocatorPath::from("./transcript/test/round_1/chunk_1/contribution_1.unverified"),
            locator
                .to_path(&Locator::ContributionFile(ContributionLocator::new(1, 1, 1, false)))
                .unwrap()
        );
        assert_eq!(
            LocatorPath::from("./transcript/test/round_1/chunk_1/contribution_1.verified"),
            locator
                .to_path(&Locator::ContributionFile(ContributionLocator::new(1, 1, 1, true)))
                .unwrap()
        );
    }

    #[test]
    fn test_to_locator_contribution_file() {
        let locator = DiskResolver::new("./transcript/test");

        assert_eq!(
            locator
                .to_locator(&"./transcript/test/round_0/chunk_0/contribution_0.unverified".into())
                .unwrap(),
            Locator::ContributionFile(ContributionLocator::new(0, 0, 0, false))
        );
        assert_eq!(
            locator
                .to_locator(&"./transcript/test/round_0/chunk_0/contribution_0.verified".into())
                .unwrap(),
            Locator::ContributionFile(ContributionLocator::new(0, 0, 0, true))
        );

        assert_eq!(
            locator
                .to_locator(&"./transcript/test/round_1/chunk_0/contribution_0.unverified".into())
                .unwrap(),
            Locator::ContributionFile(ContributionLocator::new(1, 0, 0, false))
        );
        assert_eq!(
            locator
                .to_locator(&"./transcript/test/round_1/chunk_0/contribution_0.verified".into())
                .unwrap(),
            Locator::ContributionFile(ContributionLocator::new(1, 0, 0, true))
        );

        assert_eq!(
            locator
                .to_locator(&"./transcript/test/round_0/chunk_1/contribution_0.unverified".into())
                .unwrap(),
            Locator::ContributionFile(ContributionLocator::new(0, 1, 0, false))
        );
        assert_eq!(
            locator
                .to_locator(&"./transcript/test/round_0/chunk_1/contribution_0.verified".into())
                .unwrap(),
            Locator::ContributionFile(ContributionLocator::new(0, 1, 0, true))
        );

        assert_eq!(
            locator
                .to_locator(&"./transcript/test/round_0/chunk_0/contribution_1.unverified".into())
                .unwrap(),
            Locator::ContributionFile(ContributionLocator::new(0, 0, 1, false))
        );
        assert_eq!(
            locator
                .to_locator(&"./transcript/test/round_0/chunk_0/contribution_1.verified".into())
                .unwrap(),
            Locator::ContributionFile(ContributionLocator::new(0, 0, 1, true))
        );

        assert_eq!(
            locator
                .to_locator(&"./transcript/test/round_1/chunk_1/contribution_0.unverified".into())
                .unwrap(),
            Locator::ContributionFile(ContributionLocator::new(1, 1, 0, false))
        );
        assert_eq!(
            locator
                .to_locator(&"./transcript/test/round_1/chunk_1/contribution_0.verified".into())
                .unwrap(),
            Locator::ContributionFile(ContributionLocator::new(1, 1, 0, true))
        );

        assert_eq!(
            locator
                .to_locator(&"./transcript/test/round_1/chunk_0/contribution_1.unverified".into())
                .unwrap(),
            Locator::ContributionFile(ContributionLocator::new(1, 0, 1, false))
        );
        assert_eq!(
            locator
                .to_locator(&"./transcript/test/round_1/chunk_0/contribution_1.verified".into())
                .unwrap(),
            Locator::ContributionFile(ContributionLocator::new(1, 0, 1, true))
        );

        assert_eq!(
            locator
                .to_locator(&"./transcript/test/round_0/chunk_1/contribution_1.unverified".into())
                .unwrap(),
            Locator::ContributionFile(ContributionLocator::new(0, 1, 1, false))
        );
        assert_eq!(
            locator
                .to_locator(&"./transcript/test/round_0/chunk_1/contribution_1.verified".into())
                .unwrap(),
            Locator::ContributionFile(ContributionLocator::new(0, 1, 1, true))
        );

        assert_eq!(
            locator
                .to_locator(&"./transcript/test/round_1/chunk_1/contribution_1.unverified".into())
                .unwrap(),
            Locator::ContributionFile(ContributionLocator::new(1, 1, 1, false))
        );
        assert_eq!(
            locator
                .to_locator(&"./transcript/test/round_1/chunk_1/contribution_1.verified".into())
                .unwrap(),
            Locator::ContributionFile(ContributionLocator::new(1, 1, 1, true))
        );
    }

    #[test]
    fn test_to_locator_contribution_file_signature() {
        let locator = DiskResolver::new("./transcript/test");

        assert_eq!(
            locator
                .to_locator(&"./transcript/test/round_0/chunk_0/contribution_0.unverified.signature".into())
                .unwrap(),
            Locator::ContributionFileSignature(ContributionSignatureLocator::new(0, 0, 0, false))
        );
        assert_eq!(
            locator
                .to_locator(&"./transcript/test/round_0/chunk_0/contribution_0.verified.signature".into())
                .unwrap(),
            Locator::ContributionFileSignature(ContributionSignatureLocator::new(0, 0, 0, true))
        );

        assert_eq!(
            locator
                .to_locator(&"./transcript/test/round_1/chunk_0/contribution_0.unverified.signature".into())
                .unwrap(),
            Locator::ContributionFileSignature(ContributionSignatureLocator::new(1, 0, 0, false))
        );
        assert_eq!(
            locator
                .to_locator(&"./transcript/test/round_1/chunk_0/contribution_0.verified.signature".into())
                .unwrap(),
            Locator::ContributionFileSignature(ContributionSignatureLocator::new(1, 0, 0, true))
        );

        assert_eq!(
            locator
                .to_locator(&"./transcript/test/round_0/chunk_1/contribution_0.unverified.signature".into())
                .unwrap(),
            Locator::ContributionFileSignature(ContributionSignatureLocator::new(0, 1, 0, false))
        );
        assert_eq!(
            locator
                .to_locator(&"./transcript/test/round_0/chunk_1/contribution_0.verified.signature".into())
                .unwrap(),
            Locator::ContributionFileSignature(ContributionSignatureLocator::new(0, 1, 0, true))
        );

        assert_eq!(
            locator
                .to_locator(&"./transcript/test/round_0/chunk_0/contribution_1.unverified.signature".into())
                .unwrap(),
            Locator::ContributionFileSignature(ContributionSignatureLocator::new(0, 0, 1, false))
        );
        assert_eq!(
            locator
                .to_locator(&"./transcript/test/round_0/chunk_0/contribution_1.verified.signature".into())
                .unwrap(),
            Locator::ContributionFileSignature(ContributionSignatureLocator::new(0, 0, 1, true))
        );

        assert_eq!(
            locator
                .to_locator(&"./transcript/test/round_1/chunk_1/contribution_0.unverified.signature".into())
                .unwrap(),
            Locator::ContributionFileSignature(ContributionSignatureLocator::new(1, 1, 0, false))
        );
        assert_eq!(
            locator
                .to_locator(&"./transcript/test/round_1/chunk_1/contribution_0.verified.signature".into())
                .unwrap(),
            Locator::ContributionFileSignature(ContributionSignatureLocator::new(1, 1, 0, true))
        );

        assert_eq!(
            locator
                .to_locator(&"./transcript/test/round_1/chunk_0/contribution_1.unverified.signature".into())
                .unwrap(),
            Locator::ContributionFileSignature(ContributionSignatureLocator::new(1, 0, 1, false))
        );
        assert_eq!(
            locator
                .to_locator(&"./transcript/test/round_1/chunk_0/contribution_1.verified.signature".into())
                .unwrap(),
            Locator::ContributionFileSignature(ContributionSignatureLocator::new(1, 0, 1, true))
        );

        assert_eq!(
            locator
                .to_locator(&"./transcript/test/round_0/chunk_1/contribution_1.unverified.signature".into())
                .unwrap(),
            Locator::ContributionFileSignature(ContributionSignatureLocator::new(0, 1, 1, false))
        );
        assert_eq!(
            locator
                .to_locator(&"./transcript/test/round_0/chunk_1/contribution_1.verified.signature".into())
                .unwrap(),
            Locator::ContributionFileSignature(ContributionSignatureLocator::new(0, 1, 1, true))
        );

        assert_eq!(
            locator
                .to_locator(&"./transcript/test/round_1/chunk_1/contribution_1.unverified.signature".into())
                .unwrap(),
            Locator::ContributionFileSignature(ContributionSignatureLocator::new(1, 1, 1, false))
        );
        assert_eq!(
            locator
                .to_locator(&"./transcript/test/round_1/chunk_1/contribution_1.verified.signature".into())
                .unwrap(),
            Locator::ContributionFileSignature(ContributionSignatureLocator::new(1, 1, 1, true))
        );
    }
}
