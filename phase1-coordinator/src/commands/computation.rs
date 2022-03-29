use crate::{
    authentication::Signature,
    commands::SigningKey,
    environment::Environment,
    storage::{Disk, Locator, StorageLocator, StorageObject},
    CoordinatorError,
};
use phase1::helpers::CurveKind;

use setup_utils::calculate_hash;

use std::{io::Write, sync::Arc, time::Instant};
use tracing::{debug, error, info, trace};

pub const SEED_LENGTH: usize = 32;
pub type Seed = [u8; SEED_LENGTH];

use blake2::{Blake2b512, Digest};
use itertools::Itertools;
use masp_phase2::MPCParameters;

pub struct Computation;

impl Computation {
    ///
    /// Runs computation for a given environment, storage writer, challenge locator,
    /// response locator, and contribution file signature locator.
    ///
    /// This function assumes that the locator for the previous response file, challenge file,
    /// and response file have been initialized, typically as part of a call to
    /// `Coordinator::try_lock` to lock the contribution chunk.
    ///
    pub fn run(
        environment: &Environment,
        storage: &mut Disk,
        signature: Arc<dyn Signature>,
        contributor_signing_key: &SigningKey,
        challenge_locator: &Locator,
        response_locator: &Locator,
        contribution_file_signature_locator: &Locator,
        seed: &Seed,
    ) -> anyhow::Result<()> {
        let start = Instant::now();
        info!(
            "Starting computation for\n\n\tChallenge: {}\n\tResponse : {}\n",
            storage.to_path(challenge_locator)?,
            storage.to_path(response_locator)?
        );

        // Fetch the chunk ID from the response locator.
        let (round_height, chunk_id, contribution_id) = match response_locator {
            Locator::ContributionFile(contribution_locator) => (
                contribution_locator.round_height(),
                contribution_locator.chunk_id() as usize,
                contribution_locator.contribution_id(),
            ),
            _ => return Err(CoordinatorError::ContributionLocatorIncorrect.into()),
        };

        // Run computation on chunk.
        let settings = environment.parameters();
        let curve = settings.curve();
        if let Err(error) = match curve {
            CurveKind::Bls12_381 => Self::contribute(
                storage.reader(challenge_locator)?.as_ref(),
                storage.writer(response_locator)?.as_mut(),
            ),
            CurveKind::Bls12_377 => Self::contribute(
                storage.reader(challenge_locator)?.as_ref(),
                storage.writer(response_locator)?.as_mut(),
            ),
            CurveKind::BW6 => Self::contribute(
                storage.reader(challenge_locator)?.as_ref(),
                storage.writer(response_locator)?.as_mut(),
            ),
        } {
            error!("Computation failed with {}", error);
            return Err(CoordinatorError::ComputationFailed.into());
        }

        // Load a contribution response reader.
        let reader = storage.reader(response_locator)?;
        let contribution_hash = calculate_hash(reader.as_ref());
        drop(reader);
        debug!("Response hash is {}", pretty_hash!(&contribution_hash));

        debug!(
            "Writing contribution file signature for round {} chunk {} unverified contribution {}",
            round_height, chunk_id, contribution_id
        );

        // TODO (raychu86): Move the implementation of this helper function.
        // Write the contribution file signature to disk.
        crate::commands::write_contribution_file_signature(
            storage,
            signature,
            contributor_signing_key,
            challenge_locator,
            response_locator,
            None,
            contribution_file_signature_locator,
        )?;

        debug!(
            "Successfully wrote contribution file signature for round {} chunk {} unverified contribution {}",
            round_height, chunk_id, contribution_id
        );

        let elapsed = Instant::now().duration_since(start);
        info!(
            "Completed computation on {} in {:?}",
            storage.to_path(response_locator)?,
            elapsed
        );
        Ok(())
    }

    fn contribute(challenge_reader: &[u8], mut response_writer: &mut [u8]) -> Result<(), CoordinatorError> {
        trace!("Calculating previous contribution hash and writing it to the response");

        let challenge_hash = calculate_hash(&challenge_reader);
        debug!("Challenge hash is {}", pretty_hash!(&challenge_hash));

        response_writer.write_all(&challenge_hash.as_slice())?;
        response_writer.flush()?;

        // The hash of the previous contribution is contained in the first 64 bytes of the current challenge file.
        // The response writer is initialised empty, then the hash of the previous challenge is appended to it.
        // The new contribution calculation should be appended after this hash.
        let previous_hash = &challenge_reader
            .get(0..64)
            .ok_or(CoordinatorError::StorageReaderFailed)?;
        debug!("Challenge file claims previous hash is {}", pretty_hash!(previous_hash));
        debug!("Please double check this yourself! Do not trust it blindly!");

        // TODO: do we want to include the public key to the contribution file?
        // Construct our keypair using the RNG we created above.
        // let (public_key, private_key) =
        //     Phase1::key_generation(&mut rng, challenge_hash.as_ref()).expect("could not generate keypair");

        // Perform the transformation
        trace!("Computing and writing your contribution, this could take a while");

        //
        // NOTE: Add your MPC Parameters contribute function below
        //

        // Self::contribute_masp(&challenge_reader, &mut response_writer);
        Self::contribute_test_masp(&challenge_reader, &mut response_writer);

        trace!("Finishing writing your contribution to response file");

        // Write the public key.
        // public_key.write(response_writer, compressed_outputs, &parameters)?;

        Ok(())
    }

    #[inline]
    fn contribute_masp(challenge_reader: &[u8], mut response_writer: &mut [u8]) {
        let entropy = "entropy";
        // Create an RNG based on a mixture of system randomness and user provided randomness
        let mut rng = {
            use rand::{Rng, SeedableRng};
            use rand_chacha::ChaChaRng;
            use std::convert::TryInto;

            let h = {
                let mut system_rng = rand::rngs::OsRng;
                let mut h = Blake2b512::new();

                // Gather 1024 bytes of entropy from the system
                for _ in 0..1024 {
                    let r: u8 = system_rng.gen();
                    h.update(&[r]);
                }

                // Hash it all up to make a seed
                h.update(&entropy.as_bytes());
                h.finalize()
            };

            ChaChaRng::from_seed(h[0..32].try_into().unwrap())
        };

        // MASP Spend circuit
        // trace!("Reading MASP Spend...");
        // let mut spend_params = MPCParameters::read(&challenge_reader[64..], false).expect("unable to read MASP Spend params");

        // trace!("Contributing to MASP Spend...");
        // let mut progress_update_interval: u32 = 0;

        // let spend_hash = spend_params.contribute(&mut rng, &progress_update_interval);
        trace!("Contributed to MASP Spend!");

        // MASP Output circuit
        trace!("Reading MASP Output...");
        let mut output_params =
            MPCParameters::read(&challenge_reader[64..], false).expect("unable to read MASP Output params");

        trace!("Contributing to MASP Output...");
        let mut progress_update_interval: u32 = 0;

        let output_hash = output_params.contribute(&mut rng, &progress_update_interval);
        trace!("Contributed to MASP Output!");

        // MASP Convert circuit
        // trace!("Reading MASP Convert...");
        // let mut convert_params =
        //     MPCParameters::read(&challenge_reader[64..], false).expect("unable to read MASP Convert params");

        // trace!("Contributing to MASP Convert...");
        // let mut progress_update_interval: u32 = 0;
        // let convert_hash = convert_params.contribute(&mut rng, &progress_update_interval);
        trace!("Contributed to MASP Convert!");

        let mut h = Blake2b512::new();
        // h.update(&spend_hash);
        h.update(&output_hash);
        // h.update(&convert_hash);
        let h = h.finalize();

        debug!("Contribution hash: 0x{:02x}", h.iter().format(""));

        // trace!("Writing MASP Spend parameters to file...");
        // spend_params
        //     .write(&mut response_writer)
        //     .expect("failed to write updated MASP Spend parameters");

        trace!("Writing MASP Output parameters to file...");
        output_params
            .write(&mut response_writer)
            .expect("failed to write updated MASP Output parameters");

        // trace!("Writing MASP Convert parameters to file...");
        // convert_params
        //     .write(&mut response_writer)
        //     .expect("failed to write updated MASP Convert parameters");

        response_writer.flush().unwrap();
    }

    pub fn contribute_test_masp(challenge_reader: &[u8], mut response_writer: &mut [u8]) {
        let entropy = "entropy";
        // Create an RNG based on a mixture of system randomness and user provided randomness
        let mut rng = {
            use rand::{Rng, SeedableRng};
            use rand_chacha::ChaChaRng;
            use std::convert::TryInto;

            let h = {
                let mut system_rng = rand::rngs::OsRng;
                let mut h = Blake2b512::new();

                // Gather 1024 bytes of entropy from the system
                for _ in 0..1024 {
                    let r: u8 = system_rng.gen();
                    h.update(&[r]);
                }

                // Hash it all up to make a seed
                h.update(&entropy.as_bytes());
                h.finalize()
            };

            ChaChaRng::from_seed(h[0..32].try_into().unwrap())
        };

        let mut test_params =
            MPCParameters::read(&challenge_reader[64..], false).expect("unable to read MASP Test params");

        trace!("Contributing to Masp Test...");
        let progress_update_interval: u32 = 0;

        let test_hash = test_params.contribute(&mut rng, &progress_update_interval);

        let mut h = Blake2b512::new();
        h.update(&test_hash);
        let h = h.finalize();

        debug!("Contribution hash: 0x{:02x}", h.iter().format(""));

        trace!("Writing MASP Test parameters to file...");

        test_params
            .write(&mut response_writer)
            .expect("failed to write updated MASP Test parameters");

        response_writer.flush().unwrap();
    }

    pub fn contribute_test_masp_cli<W: Write>(challenge_reader: &[u8], mut response_writer: W) {
        let entropy = "entropy";
        // Create an RNG based on a mixture of system randomness and user provided randomness
        let mut rng = {
            use rand::{Rng, SeedableRng};
            use rand_chacha::ChaChaRng;
            use std::convert::TryInto;

            let h = {
                let mut system_rng = rand::rngs::OsRng;
                let mut h = Blake2b512::new();

                // Gather 1024 bytes of entropy from the system
                for _ in 0..1024 {
                    let r: u8 = system_rng.gen();
                    h.update(&[r]);
                }

                // Hash it all up to make a seed
                h.update(&entropy.as_bytes());
                h.finalize()
            };

            ChaChaRng::from_seed(h[0..32].try_into().unwrap())
        };

        let mut test_params =
            MPCParameters::read(&challenge_reader[64..], false).expect("unable to read MASP Test params");

        trace!("Contributing to Masp Test...");
        let progress_update_interval: u32 = 0;

        let test_hash = test_params.contribute(&mut rng, &progress_update_interval);

        let mut h = Blake2b512::new();
        h.update(&test_hash);
        let h = h.finalize();

        debug!("Contribution hash: 0x{:02x}", h.iter().format(""));

        trace!("Writing MASP Test parameters to file...");
        test_params
            .write(&mut response_writer)
            .expect("failed to write updated MASP Test parameters");

        response_writer.flush().unwrap();
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        authentication::{Dummy, Signature},
        commands::{Computation, Initialization, Seed, SEED_LENGTH},
        storage::{ContributionLocator, ContributionSignatureLocator, Locator, Object, StorageObject},
        testing::prelude::*,
    };
    use setup_utils::calculate_hash;

    use rand::RngCore;
    use std::sync::Arc;
    use tracing::{debug, trace};

    use itertools::Itertools;

    #[test]
    #[serial]
    fn test_computation_run() {
        initialize_test_environment(&TEST_ENVIRONMENT_ANOMA);

        // Define signature scheme.
        let signature: Arc<dyn Signature> = Arc::new(Dummy);

        // Define test parameters.
        let number_of_chunks = TEST_ENVIRONMENT_ANOMA.number_of_chunks();

        // Define test storage.
        let mut storage = test_storage(&TEST_ENVIRONMENT_ANOMA);

        // Generate a new challenge for the given parameters.
        let round_height = 0;
        for chunk_id in 0..number_of_chunks {
            debug!("Initializing test chunk {}", chunk_id);

            // Run initialization on chunk.
            Initialization::run(&TEST_ENVIRONMENT_ANOMA, &mut storage, round_height, chunk_id).unwrap();
        }

        // Generate a new challenge for the given parameters.
        let round_height = 1;
        for chunk_id in 0..number_of_chunks {
            trace!("Running computation on test chunk {}", chunk_id);

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
            let contributor_signing_key = "secret_key".to_string();
            let mut seed: Seed = [0; SEED_LENGTH];
            rand::thread_rng().fill_bytes(&mut seed[..]);
            Computation::run(
                &TEST_ENVIRONMENT_ANOMA,
                &mut storage,
                signature.clone(),
                &contributor_signing_key,
                challenge_locator,
                response_locator,
                contribution_file_signature_locator,
                &seed,
            )
            .unwrap();

            // Check that the current contribution was generated based on the previous contribution hash.
            let challenge_hash = calculate_hash(&storage.reader(&challenge_locator).unwrap());
            let saved_challenge_hash = storage
                .reader(&response_locator)
                .unwrap()
                .chunks(64)
                .next()
                .unwrap()
                .to_vec();

            trace!("challenge_hash: 0x{:02x}", challenge_hash.iter().format(""));
            trace!("saved_challenge_hash: 0x{:02x}", saved_challenge_hash.iter().format(""));

            for (i, (expected, candidate)) in (challenge_hash.iter().zip(&saved_challenge_hash)).enumerate() {
                trace!("Checking byte {} of expected hash", i);
                assert_eq!(expected, candidate);
            }
        }
    }

    #[test]
    #[serial]
    fn test_computation_run_anoma() {
        initialize_test_environment(&TEST_ENVIRONMENT_ANOMA);

        // Define signature scheme.
        let signature: Arc<dyn Signature> = Arc::new(Dummy);

        // Define test parameters.
        let number_of_chunks = TEST_ENVIRONMENT_ANOMA.number_of_chunks();

        // Define test storage.
        let mut storage = test_storage(&TEST_ENVIRONMENT_ANOMA);

        // Generate a new challenge for the given parameters.
        let round_height = 0;
        for chunk_id in 0..number_of_chunks {
            debug!("Initializing test chunk {}", chunk_id);

            // Run initialization on chunk.
            Initialization::run(&TEST_ENVIRONMENT_ANOMA, &mut storage, round_height, chunk_id).unwrap();
        }

        // Generate a new challenge for the given parameters.
        /*
        let round_height = 1;
        for chunk_id in 0..number_of_chunks {
            trace!("Running computation on test chunk {}", chunk_id);

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
            let contributor_signing_key = "secret_key".to_string();
            let mut seed: Seed = [0; SEED_LENGTH];
            rand::thread_rng().fill_bytes(&mut seed[..]);
            Computation::run(
                &TEST_ENVIRONMENT_ANOMA,
                &mut storage,
                signature.clone(),
                &contributor_signing_key,
                challenge_locator,
                response_locator,
                contribution_file_signature_locator,
                &seed,
            )
            .unwrap();

            // Check that the current contribution was generated based on the previous contribution hash.
            let challenge_hash = calculate_hash(&storage.reader(&challenge_locator).unwrap());
            let saved_challenge_hash = storage
                .reader(&response_locator)
                .unwrap()
                .chunks(64)
                .next()
                .unwrap()
                .to_vec();
            for (i, (expected, candidate)) in (challenge_hash.iter().zip(&saved_challenge_hash)).enumerate() {
                trace!("Checking byte {} of expected hash", i);
                assert_eq!(expected, candidate);
            }
        }
        */
    }
}
