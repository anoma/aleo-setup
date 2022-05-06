use crate::{
    authentication::Signature,
    commands::SigningKey,
    environment::Environment,
    storage::{
        ContributionLocator,
        ContributionSignatureLocator,
        Disk,
        Locator,
        Object,
        StorageLocator,
        StorageObject,
    },
    CoordinatorError,
};
use phase1::{helpers::CurveKind, PublicKey};
use setup_utils::{calculate_hash, GenericArray, U64};

use std::{io::Write, sync::Arc, time::Instant};
use tracing::{debug, error, info, trace};

use blake2::{Blake2b512, Digest};
use itertools::Itertools;
use masp_phase2::{verify_contribution, MPCParameters};

pub(crate) struct Verification;

impl Verification {
    ///
    /// Runs verification for a given environment, storage,
    /// round height, chunk ID, and contribution ID of the
    /// unverified response file.
    ///
    #[inline]
    pub(crate) fn run(
        environment: &Environment,
        storage: &mut Disk,
        signature: Arc<dyn Signature>,
        signing_key: &SigningKey,
        round_height: u64,
        chunk_id: u64,
        current_contribution_id: u64,
        is_final_contribution: bool,
    ) -> Result<(), CoordinatorError> {
        info!(
            "Starting verification of round {} chunk {} contribution {}",
            round_height, chunk_id, current_contribution_id
        );
        let start = Instant::now();

        // Check that this is not the initial contribution.
        if (round_height == 0 || round_height == 1) && current_contribution_id == 0 {
            return Err(CoordinatorError::VerificationOnContributionIdZero);
        }

        // Check that the chunk ID is valid.
        if chunk_id > environment.number_of_chunks() {
            return Err(CoordinatorError::ChunkIdInvalid);
        }

        // Fetch the locators for `Verification`.
        let challenge_locator = Locator::ContributionFile(ContributionLocator::new(
            round_height,
            chunk_id,
            current_contribution_id - 1,
            true,
        ));
        let response_locator = Locator::ContributionFile(ContributionLocator::new(
            round_height,
            chunk_id,
            current_contribution_id,
            false,
        ));
        let (next_challenge_locator, contribution_file_signature_locator) = match is_final_contribution {
            true => (
                Locator::ContributionFile(ContributionLocator::new(round_height + 1, chunk_id, 0, true)),
                Locator::ContributionFileSignature(ContributionSignatureLocator::new(
                    round_height + 1,
                    chunk_id,
                    0,
                    true,
                )),
            ),
            false => (
                Locator::ContributionFile(ContributionLocator::new(
                    round_height,
                    chunk_id,
                    current_contribution_id,
                    true,
                )),
                Locator::ContributionFileSignature(ContributionSignatureLocator::new(
                    round_height,
                    chunk_id,
                    current_contribution_id,
                    true,
                )),
            ),
        };

        trace!("Challenge locator is {}", storage.to_path(&challenge_locator)?);
        trace!("Response locator is {}", storage.to_path(&response_locator)?);
        trace!(
            "Next challenge locator is {}",
            storage.to_path(&next_challenge_locator)?
        );
        trace!(
            "Contribution file signature locator is {}",
            storage.to_path(&contribution_file_signature_locator)?
        );

        if let Err(error) = Self::verification(
            environment,
            storage,
            chunk_id,
            challenge_locator.clone(),
            response_locator.clone(),
            next_challenge_locator.clone(),
        ) {
            error!("Verification failed with {}", error);
            return Err(error);
        }

        debug!(
            "Writing contribution file signature for round {} chunk {} verified contribution {}",
            round_height, chunk_id, current_contribution_id
        );

        // Initialize the contribution file signature locator, if it does not exist.
        if !storage.exists(&contribution_file_signature_locator) {
            let expected_filesize = Object::contribution_file_signature_size(true);
            storage.initialize(contribution_file_signature_locator.clone(), expected_filesize)?;
        }

        // TODO (raychu86): Move the implementation of this helper function.
        // Write the contribution file signature to disk.
        crate::commands::write_contribution_file_signature(
            storage,
            signature,
            signing_key,
            &challenge_locator,
            &response_locator,
            Some(&next_challenge_locator),
            &contribution_file_signature_locator,
        )?;

        debug!(
            "Successfully wrote contribution file signature for round {} chunk {} verified contribution {}",
            round_height, chunk_id, current_contribution_id
        );

        let elapsed = Instant::now().duration_since(start);
        info!(
            "Completed verification of round {} chunk {} contribution {} in {:?}",
            round_height, chunk_id, current_contribution_id, elapsed
        );
        Ok(())
    }

    #[inline]
    fn verification(
        environment: &Environment,
        storage: &mut Disk,
        chunk_id: u64,
        challenge_locator: Locator,
        response_locator: Locator,
        next_challenge_locator: Locator,
    ) -> Result<(), CoordinatorError> {
        // Check that the previous and current locators exist in storage.
        if !storage.exists(&challenge_locator) || !storage.exists(&response_locator) {
            return Err(CoordinatorError::ContributionLocatorMissing);
        }

        // Execute ceremony verification on chunk.
        let settings = environment.parameters();
        let result = match settings.curve() {
            CurveKind::Bls12_381 => Self::transform_pok_and_correctness(
                storage.reader(&challenge_locator)?.as_ref(),
                storage.reader(&response_locator)?.as_ref(),
            ),
            CurveKind::Bls12_377 => Self::transform_pok_and_correctness(
                storage.reader(&challenge_locator)?.as_ref(),
                storage.reader(&response_locator)?.as_ref(),
            ),
            CurveKind::BW6 => Self::transform_pok_and_correctness(
                storage.reader(&challenge_locator)?.as_ref(),
                storage.reader(&response_locator)?.as_ref(),
            ),
        };
        let response_hash = match result {
            Ok(response_hash) => response_hash,
            Err(error) => {
                error!("Verification failed with {}", error);
                return Err(CoordinatorError::VerificationFailed.into());
            }
        };

        trace!("Verification succeeded! Writing the next challenge file");

        // Fetch the compression settings.
        // FIXME: removed the compression/decompression part, since we don't implement it
        let response_is_compressed = environment.compressed_outputs();
        let next_challenge_is_compressed = environment.compressed_inputs();

        // Create the next challenge file.
        let next_challenge_hash = if response_is_compressed == next_challenge_is_compressed {
            // TODO (howardwu): Update this.
            trace!("Copying decompressed response file without the public key");
            storage.copy(&response_locator, &next_challenge_locator)?;

            calculate_hash(&storage.reader(&next_challenge_locator)?)
        } else {
            trace!("Starting decompression of the response file for the next challenge file");

            // Initialize the next contribution locator, if it does not exist.
            if !storage.exists(&next_challenge_locator) {
                storage.initialize(
                    next_challenge_locator.clone(),
                    Object::contribution_file_size(environment, chunk_id, true),
                )?;
            }

            match settings.curve() {
                CurveKind::Bls12_381 => Self::decompress(
                    storage.reader(&response_locator)?.as_ref(),
                    storage.writer(&next_challenge_locator)?.as_mut(),
                    response_hash.as_ref(),
                )?,
                CurveKind::Bls12_377 => Self::decompress(
                    storage.reader(&response_locator)?.as_ref(),
                    storage.writer(&next_challenge_locator)?.as_mut(),
                    response_hash.as_ref(),
                )?,
                CurveKind::BW6 => Self::decompress(
                    storage.reader(&response_locator)?.as_ref(),
                    storage.writer(&next_challenge_locator)?.as_mut(),
                    response_hash.as_ref(),
                )?,
            };

            calculate_hash(storage.reader(&next_challenge_locator)?.as_ref())
        };

        debug!("The next challenge hash is {}", pretty_hash!(&next_challenge_hash));

        {
            // Fetch the saved response hash in the next challenge file.
            let saved_response_hash = storage
                .reader(&next_challenge_locator)?
                .as_ref()
                .chunks(64)
                .next()
                .unwrap()
                .to_vec();

            // Check that the response hash matches the next challenge hash.
            debug!("The response hash is {}", pretty_hash!(&response_hash));
            debug!("The saved response hash is {}", pretty_hash!(&saved_response_hash));
            if response_hash.as_slice() != saved_response_hash {
                error!("Response hash does not match the saved response hash.");
                return Err(CoordinatorError::ContributionHashMismatch);
            }
        }

        Ok(())
    }

    #[inline]
    fn transform_pok_and_correctness(
        challenge_reader: &[u8],
        response_reader: &[u8],
    ) -> Result<GenericArray<u8, U64>, CoordinatorError> {
        debug!("Verifying challenges");

        // Check that the challenge hashes match.
        let challenge_hash = {
            // Compute the challenge hash using the challenge file.
            let challenge_hash = calculate_hash(challenge_reader.as_ref());

            // Fetch the challenge hash from the response file.
            let saved_challenge_hash = &response_reader
                .get(0..64)
                .ok_or(CoordinatorError::StorageReaderFailed)?[..];

            // Check that the challenge hashes match.
            debug!("The challenge hash is {}", pretty_hash!(&challenge_hash));
            debug!("The saved challenge hash is {}", pretty_hash!(&saved_challenge_hash));
            match challenge_hash.as_slice() == saved_challenge_hash {
                true => challenge_hash,
                false => {
                    error!("Challenge hash does not match saved challenge hash.");
                    return Err(CoordinatorError::ContributionHashMismatch);
                }
            }
        };

        // Compute the response hash using the response file.
        let response_hash = calculate_hash(response_reader);
        debug!("Response Reader hash is {}", pretty_hash!(&response_hash));
        debug!("Challenge Reader is {}", pretty_hash!(&challenge_reader[0..256]));
        debug!("Response Reader is {}", pretty_hash!(&response_reader[0..256]));

        // Fetch the public key of the contributor.
        // let public_key = PublicKey::read(response_reader, compressed_response, &parameters)?;
        // trace!("Public key of the contributor is {:#?}", public_key);

        trace!("Starting verification");

        // Self::verify_masp(&challenge_reader, &response_reader);
        Self::verify_test_masp(&challenge_reader, &response_reader);

        trace!("Completed verification");

        Ok(response_hash)
    }

    #[inline]
    fn verify_masp(challenge_reader: &[u8], response_reader: &[u8]) {
        let masp_spend =
            MPCParameters::read(&challenge_reader[64..], false).expect("couldn't deserialize MASP Spend params");

        let masp_output =
            MPCParameters::read(&challenge_reader[64..], false).expect("couldn't deserialize MASP Output params");

        let masp_convert =
            MPCParameters::read(&challenge_reader[64..], false).expect("couldn't deserialize MASP Convert params");

        let new_masp_spend =
            MPCParameters::read(&response_reader[64..], true).expect("couldn't deserialize MASP Spend new_params");

        let new_masp_output =
            MPCParameters::read(&response_reader[64..], true).expect("couldn't deserialize MASP Output new_params");

        let new_masp_convert =
            MPCParameters::read(&response_reader[64..], true).expect("couldn't deserialize MASP Convert new_params");

        let spend_hash = match verify_contribution(&masp_spend, &new_masp_spend) {
            Ok(hash) => hash,
            Err(_) => panic!("invalid MASP Spend transformation!"),
        };

        let output_hash = match verify_contribution(&masp_output, &new_masp_output) {
            Ok(hash) => hash,
            Err(_) => panic!("invalid MASP Output transformation!"),
        };

        let convert_hash = match verify_contribution(&masp_convert, &new_masp_convert) {
            Ok(hash) => hash,
            Err(_) => panic!("invalid MASP Convert transformation!"),
        };

        let mut h = Blake2b512::new();
        h.update(&spend_hash);
        h.update(&output_hash);
        h.update(&convert_hash);
        let h = h.finalize();

        debug!("Verification hash: 0x{:02x}", h.iter().format(""));
    }

    #[inline]
    fn verify_test_masp(challenge_reader: &[u8], response_reader: &[u8]) {
        let masp_test =
            MPCParameters::read(&challenge_reader[64..], false).expect("couldn't deserialize MASP Test params");

        let new_masp_test =
            MPCParameters::read(&response_reader[64..], true).expect("couldn't deserialize MASP Spend new_params");

        let test_hash = match verify_contribution(&masp_test, &new_masp_test) {
            Ok(hash) => hash,
            Err(_) => panic!("invalid MASP Spend transformation!"),
        };

        let mut h = Blake2b512::new();
        h.update(&test_hash);
        let h = h.finalize();

        debug!("Verification hash: 0x{:02x}", h.iter().format(""));
    }

    #[inline]
    fn decompress(
        response_reader: &[u8],
        mut next_challenge_writer: &mut [u8],
        response_hash: &[u8],
    ) -> Result<(), CoordinatorError> {
        // Copies hash of previous response to the new challenge locator, then adds the parameters
        (&mut next_challenge_writer[0..]).write_all(response_hash)?;
        (&mut next_challenge_writer[64..]).write_all(&response_reader[64..])?;

        Ok(next_challenge_writer.flush()?)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        authentication::Dummy,
        commands::{Computation, Seed, Verification, SEED_LENGTH},
        storage::{ContributionLocator, ContributionSignatureLocator, Locator, Object},
        testing::prelude::*,
        Coordinator,
    };

    use once_cell::sync::Lazy;
    use rand::RngCore;
    use time::OffsetDateTime;

    #[test]
    #[serial]
    fn test_verification_run() {
        initialize_test_environment(&TEST_ENVIRONMENT_ANOMA);

        let mut coordinator = Coordinator::new(TEST_ENVIRONMENT_ANOMA.clone(), Arc::new(Dummy)).unwrap();

        let contributor = Lazy::force(&TEST_CONTRIBUTOR_ID).clone();
        let contributor_signing_key = "secret_key".to_string();

        let _verifier = Lazy::force(&TEST_VERIFIER_ID).clone();
        let verifier_signing_key = "secret_key".to_string();

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
        let number_of_chunks = TEST_ENVIRONMENT_ANOMA.number_of_chunks();
        let is_final = true;

        for chunk_id in 0..number_of_chunks {
            // Fetch the challenge locator.
            let challenge_locator =
                &Locator::ContributionFile(ContributionLocator::new(round_height, chunk_id, 0, true));
            // Fetch the response locator.
            let response_locator =
                &Locator::ContributionFile(ContributionLocator::new(round_height, chunk_id, 1, false));
            // Fetch the contribution file signature locator.
            let contribution_file_signature_locator = &Locator::ContributionFileSignature(
                ContributionSignatureLocator::new(round_height, chunk_id, 1, false),
            );

            let signature = coordinator.signature();
            let storage = coordinator.storage_mut();

            if !storage.exists(response_locator) {
                let expected_filesize = Object::contribution_file_size(&TEST_ENVIRONMENT_ANOMA, chunk_id, false);
                storage.initialize(response_locator.clone(), expected_filesize).unwrap();
            }
            if !storage.exists(contribution_file_signature_locator) {
                let expected_filesize = Object::contribution_file_signature_size(false);
                storage
                    .initialize(contribution_file_signature_locator.clone(), expected_filesize)
                    .unwrap();
            }

            // Run computation on chunk.
            let mut seed: Seed = [0; SEED_LENGTH];
            rand::thread_rng().fill_bytes(&mut seed[..]);
            Computation::run(
                &TEST_ENVIRONMENT_ANOMA,
                storage,
                signature.clone(),
                &contributor_signing_key,
                challenge_locator,
                response_locator,
                contribution_file_signature_locator,
                &seed,
            )
            .unwrap();

            // Run verification on chunk.
            Verification::run(
                &TEST_ENVIRONMENT_ANOMA,
                storage,
                signature,
                &verifier_signing_key,
                round_height,
                chunk_id,
                1,
                is_final,
            )
            .unwrap();

            // Fetch the next contribution locator.
            let next = match is_final {
                true => Locator::ContributionFile(ContributionLocator::new(round_height + 1, chunk_id, 0, true)),
                false => Locator::ContributionFile(ContributionLocator::new(round_height, chunk_id, 1, true)),
            };

            // Check the next challenge file exists.
            assert!(storage.exists(&next));
        }
    }
}
