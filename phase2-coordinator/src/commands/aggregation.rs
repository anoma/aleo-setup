use crate::{
    environment::Environment,
    objects::Round,
    storage::{ContributionLocator, Disk, DiskObjectReader, Locator, Object, StorageLocator, StorageObject},
    CoordinatorError,
};

use std::time::Instant;
use tracing::{debug, error, trace};

pub(crate) struct Aggregation;

impl Aggregation {
    /// Runs aggregation for a given environment, storage, and round.
    /// NOTE: The Namada Trusted Setup runs only phase 2 and doesn't chunk the contributions. The aggregation has been left as a placeholder to avoid breaking the whole library logic. In practice, there is no aggregation.
    #[inline]
    pub(crate) fn run(environment: &Environment, storage: &mut Disk, round: &Round) -> anyhow::Result<()> {
        let start = Instant::now();

        // Fetch the round height.
        let round_height = round.round_height();
        debug!("Starting aggregation on round {}", round_height);

        // Fetch the compressed input setting for the final round file.
        let _compressed_input = environment.compressed_inputs();
        // Fetch the compressed output setting based on the round height.
        let compressed_output = environment.compressed_outputs();

        // Fetch the round locator for the given round.
        let round_locator = Locator::RoundFile { round_height };

        // Check that the round locator does not already exist.
        if storage.exists(&round_locator) {
            return Err(CoordinatorError::RoundLocatorAlreadyExists.into());
        }

        // Initialize the round locator.
        storage.initialize(round_locator.clone(), Object::round_file_size(environment))?;

        // Load the contribution files.
        let readers = Self::readers(environment, storage, round)?;
        let _contribution_readers: Vec<_> = readers.iter().map(|r| (r.as_ref(), compressed_output)).collect();

        let elapsed = Instant::now().duration_since(start);
        debug!("Completed aggregation on round {} in {:?}", round_height, elapsed);
        Ok(())
    }

    /// Attempts to open every contribution for the given round and
    /// returns readers to each chunk contribution file.
    #[inline]
    fn readers<'a>(
        environment: &Environment,
        storage: &'a Disk,
        round: &Round,
    ) -> anyhow::Result<Vec<DiskObjectReader>> {
        let mut readers = vec![];

        // Fetch the round height.
        let round_height = round.round_height();

        // Fetch the expected current contribution ID for each chunk in the given round.
        let expected_id = round.expected_number_of_contributions() - 1;

        for chunk_id in 0..environment.number_of_chunks() {
            trace!("Loading contribution for round {} chunk {}", round_height, chunk_id);

            // Fetch the contribution ID.
            let contribution_id = round.chunk(chunk_id)?.current_contribution_id();

            // Sanity check that all chunks have all contributions present.
            if expected_id != contribution_id {
                error!("Expects {} contributions, found {}", expected_id, contribution_id);
                return Err(CoordinatorError::NumberOfContributionsDiffer.into());
            }

            // Fetch the contribution locator.
            let contribution_locator =
                Locator::ContributionFile(ContributionLocator::new(round_height, chunk_id, contribution_id, false));
            trace!("Loading contribution from {}", storage.to_path(&contribution_locator)?);

            // Check the corresponding verified contribution locator exists.
            let verified_contribution =
                Locator::ContributionFile(ContributionLocator::new(round_height + 1, chunk_id, 0, true));
            if !storage.exists(&verified_contribution) {
                error!("{} is missing", storage.to_path(&verified_contribution)?);
                return Err(CoordinatorError::ContributionMissingVerifiedLocator.into());
            }

            // Fetch and save a reader for the contribution locator.
            readers.push(storage.reader(&contribution_locator)?);

            trace!("Loaded contribution for round {} chunk {}", round_height, chunk_id);
        }

        Ok(readers)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        authentication::Dummy,
        commands::{Aggregation, Seed, SigningKey, SEED_LENGTH},
        objects::Task,
        storage::Locator,
        testing::prelude::*,
        Coordinator,
    };

    use once_cell::sync::Lazy;
    use rand::RngCore;
    use time::OffsetDateTime;
    use tracing::*;

    #[test]
    #[serial]
    fn test_aggregation_run() {
        initialize_test_environment(&TEST_ENVIRONMENT_3);

        let mut coordinator = Coordinator::new(TEST_ENVIRONMENT_3.clone(), Arc::new(Dummy)).unwrap();

        let contributor = Lazy::force(&TEST_CONTRIBUTOR_ID).clone();
        let contributor_signing_key: SigningKey = "secret_key".to_string();

        let verifier = Lazy::force(&TEST_VERIFIER_ID).clone();
        let verifier_signing_key: SigningKey = "secret_key".to_string();

        {
            // Run initialization.
            info!("Initializing ceremony");
            let round_height = coordinator.run_initialization(OffsetDateTime::now_utc()).unwrap();
            info!("Initialized ceremony");

            // Check current round height is now 0.
            assert_eq!(0, round_height);

            let contributors = vec![contributor.clone()];
            coordinator.next_round(*TEST_STARTED_AT, contributors).unwrap();
        }

        // Check current round height is now 1.
        assert_eq!(1, coordinator.current_round_height().unwrap());

        // Define test parameters.
        let round_height = coordinator.current_round_height().unwrap();
        let number_of_chunks = TEST_ENVIRONMENT_3.number_of_chunks();

        let mut seed: Seed = [0; SEED_LENGTH];
        rand::thread_rng().fill_bytes(&mut seed[..]);
        // Iterate over all chunk IDs.
        for chunk_id in 0..number_of_chunks {
            {
                // Acquire the lock as contributor.
                let try_lock = coordinator.try_lock_chunk(chunk_id, &contributor.clone());
                if try_lock.is_err() {
                    error!(
                        "Failed to acquire lock as contributor {:?}\n{}",
                        &contributor,
                        serde_json::to_string_pretty(&coordinator.current_round().unwrap()).unwrap()
                    );
                    try_lock.unwrap();
                }
            }
            {
                // Run computation as contributor.
                let contribute = coordinator.run_computation(
                    round_height,
                    chunk_id,
                    1,
                    &contributor.clone(),
                    &contributor_signing_key,
                    &seed,
                );
                if contribute.is_err() {
                    error!(
                        "Failed to run computation as contributor {:?}\n{}",
                        &contributor,
                        serde_json::to_string_pretty(&coordinator.current_round().unwrap()).unwrap()
                    );
                    contribute.unwrap();
                }

                // Add the contribution as the contributor.
                let contribute = coordinator.add_contribution(chunk_id, &contributor.clone());
                if contribute.is_err() {
                    error!(
                        "Failed to add contribution as contributor {:?}\n{}",
                        &contributor,
                        serde_json::to_string_pretty(&coordinator.current_round().unwrap()).unwrap()
                    );
                    contribute.unwrap();
                }
            }
            {
                // Run verification as verifier.
                let task = Task::new(chunk_id, 1);
                let verify = coordinator.run_verification(round_height, &task, &verifier, &verifier_signing_key);
                if verify.is_err() {
                    error!(
                        "Failed to run verification as verifier {:?}\n{}",
                        &verifier,
                        serde_json::to_string_pretty(&coordinator.current_round().unwrap()).unwrap()
                    );
                    verify.unwrap();
                }

                // Run verification as the verifier.
                let verify = coordinator.verify_contribution(&task, &verifier.clone());
                if verify.is_err() {
                    error!(
                        "Failed to run verification as verifier {:?}\n{}",
                        &verifier,
                        serde_json::to_string_pretty(&coordinator.current_round().unwrap()).unwrap()
                    );
                    verify.unwrap();
                }
            }
        }

        // Fetch the current round state.
        let round = coordinator.get_round(round_height).unwrap();

        // Aggregate.
        {
            let storage = coordinator.storage_mut();

            // Run aggregation on the round.
            Aggregation::run(&TEST_ENVIRONMENT_3, storage, &round).unwrap();

            // Fetch the round locator for the given round.
            let round_locator = Locator::RoundFile { round_height };

            assert!(storage.exists(&round_locator));
        }
    }
}
