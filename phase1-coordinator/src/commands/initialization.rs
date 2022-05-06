use crate::{
    environment::Environment,
    storage::{ContributionLocator, Disk, Locator, Object, StorageObject},
    CoordinatorError,
};

use phase1::helpers::CurveKind;
use setup_utils::{blank_hash, calculate_hash};

use std::io::Cursor;
use std::{io::Write, time::Instant, io::BufWriter};
use tracing::{debug, error, info, trace};
use std::fs::File;


use masp_phase2::MPCParameters;

use bellman::{Circuit, ConstraintSystem, SynthesisError};
use bls12_381::Scalar;

struct TestCircuit {
    x: Option<Scalar>,
}
impl Circuit<Scalar> for TestCircuit {
    fn synthesize<CS: ConstraintSystem<Scalar>>(self, cs: &mut CS) -> Result<(), SynthesisError> {
        let mut x_value = self.x;
        let mut x = cs.alloc(|| "x", || x_value.ok_or(SynthesisError::AssignmentMissing))?;

        cs.enforce(|| "x = x^2", |lc| lc + x, |lc| lc + x, |lc| lc + x);

        Ok(())
    }
}

pub(crate) struct Initialization;

impl Initialization {
    ///
    /// Runs chunk initialization for a given environment, round height, and chunk ID.
    ///
    /// Executes the round initialization on a given chunk ID.
    ///
    #[inline]
    pub(crate) fn run(
        environment: &Environment,
        storage: &mut Disk,
        round_height: u64,
        chunk_id: u64,
    ) -> anyhow::Result<Vec<u8>> {
        info!("Starting initialization on round {} chunk {}", round_height, chunk_id);
        let start = Instant::now();

        // Determine the expected challenge size.
        // For Anoma Phase 2, the expected challenge size is hardcoded under phase1-coordinator/storage/storage.rs at the const ANOMA_FILE_SIZE
        // TODO: refactor this parameter to the environment file and find a way to calculate the expected size
        let expected_challenge_size = Object::contribution_file_size(environment, chunk_id, true);
        // TODO: implement calculate size macro for our curve
        trace!("Expected challenge file size is {}", expected_challenge_size);

        // Initialize and fetch a writer for the contribution locator so the output is saved.
        let contribution_locator = Locator::ContributionFile(ContributionLocator::new(round_height, chunk_id, 0, true));
        storage.initialize(contribution_locator.clone(), expected_challenge_size as u64)?;

        // Run ceremony initialization on chunk.
        let settings = environment.parameters();

        if let Err(error) = match settings.curve() {
            CurveKind::Bls12_381 => Self::initialization(storage.writer(&contribution_locator)?.as_mut()),
            CurveKind::Bls12_377 => Self::initialization(storage.writer(&contribution_locator)?.as_mut()),
            CurveKind::BW6 => Self::initialization(storage.writer(&contribution_locator)?.as_mut()),
        } {
            error!("Initialization failed with {}", error);
            return Err(CoordinatorError::InitializationFailed.into());
        }

        // Copy the current transcript to the next transcript.
        // This operation will *overwrite* the contents of `next_transcript`.
        let next_contribution_locator =
            Locator::ContributionFile(ContributionLocator::new(round_height + 1, chunk_id, 0, true));
        if let Err(error) = storage.copy(&contribution_locator, &next_contribution_locator) {
            return Err(error.into());
        }

        // Check that the current and next contribution hash match.
        let hash = Self::check_hash(storage, &contribution_locator, &next_contribution_locator)?;
        debug!("The challenge hash of Chunk {} is {}", chunk_id, pretty_hash!(&hash));

        let elapsed = Instant::now().duration_since(start);
        info!("Completed initialization on chunk {} in {:?}", chunk_id, elapsed);
        Ok(hash)
    }

    /// Runs Phase 2 initialization on the given parameters.
    #[inline]
    fn initialization(mut writer: &mut [u8]) -> Result<(), CoordinatorError> {
        info!("Initializing Phase 2 Initialization");
        // The initialization contribution file contains [blank_hash, init.params]
        // The circuit parameters are appended to the blank_hash
        let hash = blank_hash();

        writer.write_all(&hash.as_slice())?;
        writer.flush()?;

        debug!("Empty challenge hash is {}", pretty_hash!(&hash));

        info!("Starting Phase 2 initialization operation");

        //
        // NOTE: Add your MPC Parameters initialization function below
        //

        // Self::initialize_masp(&mut writer);
        Self::initialize_test_masp(&mut writer);

        trace!("Completed Phase 2 initialization operation");

        Ok(())
    }

    /// Compute both contribution hashes and check for equivalence.
    #[inline]
    fn check_hash(
        storage: &Disk,
        contribution_locator: &Locator,
        next_contribution_locator: &Locator,
    ) -> anyhow::Result<Vec<u8>> {
        let current = storage.reader(contribution_locator)?;
        let next = storage.reader(next_contribution_locator)?;

        // Compare the contribution hashes of both files to ensure the copy succeeded.
        let contribution_hash_0 = calculate_hash(current.as_ref());
        let contribution_hash_1 = calculate_hash(next.as_ref());
        if contribution_hash_0 != contribution_hash_1 {
            return Err(CoordinatorError::InitializationTranscriptsDiffer.into());
        }

        Ok(contribution_hash_1.to_vec())
    }

    #[inline]
    fn initialize_masp(mut writer: &mut [u8]) {
        //
        // MASP spend circuit
        //
        trace!("Creating initial parameters for MASP Spend...");
        let spend_params = MPCParameters::new(
            masp_proofs::circuit::sapling::Spend {
                value_commitment: None,
                proof_generation_key: None,
                payment_address: None,
                commitment_randomness: None,
                ar: None,
                auth_path: vec![None; 32], // Tree depth is 32 for sapling
                anchor: None,
            },
            //should_filter_points_at_infinity,
            //radix_directory,
        )
        .unwrap();
        trace!("Writing initial MASP Spend parameters to file...",);

        spend_params
            .write(&mut writer)
            .expect("unable to write MASP Spend params");
        //
        // MASP output circuit
        //
        trace!("Creating initial parameters for MASP Output...");
        let output_params = MPCParameters::new(
            masp_proofs::circuit::sapling::Output {
                value_commitment: None,
                payment_address: None,
                commitment_randomness: None,
                esk: None,
                asset_identifier: vec![None; 256],
            },
            //should_filter_points_at_infinity,
            //radix_directory,
        )
        .unwrap();

        trace!("Writing initial MASP Output parameters to file...",);

        output_params
            .write(&mut writer)
            .expect("unable to write MASP Output params");

        //
        // MASP Convert circuit
        //
        trace!("Creating initial parameters for MASP Convert...");
        let convert_params = MPCParameters::new(
            masp_proofs::circuit::convert::Convert {
                value_commitment: None,
                auth_path: vec![None; 32], // Tree depth is 32 for sapling
                anchor: None,
            },
            //should_filter_points_at_infinity,
            //radix_directory,
        )
        .unwrap();

        trace!("Writing initial MASP Convert parameters to file...",);

        convert_params
            .write(&mut writer)
            .expect("unable to write MASP Convert params");

        writer.flush().unwrap();
    }

    #[inline]
    fn initialize_test_masp(mut writer: &mut [u8]) {
        // MASP Test circuit
        trace!("Creating initial parameters for MASP Test Circuit...");
        let instance = TestCircuit { x: Some(Scalar::one()) };
        let test_params = MPCParameters::new(instance).unwrap();
        trace!("Writing initial MASP Test Circuit parameters to file...",);

        test_params
            .write(&mut writer)
            .expect("unable to write MASP Test Circuit params");

        writer.flush().unwrap();
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        commands::Initialization,
        storage::{ContributionLocator, Locator, StorageObject},
        testing::prelude::*,
    };
    use setup_utils::{blank_hash, calculate_hash, GenericArray};

    use tracing::{debug, trace};

    #[test]
    #[serial]
    fn test_initialization_run() {
        initialize_test_environment(&TEST_ENVIRONMENT_ANOMA);

        // Define test parameters.
        let round_height = 0;
        let number_of_chunks = TEST_ENVIRONMENT_ANOMA.number_of_chunks();

        // Define test storage.
        let mut storage = test_storage(&TEST_ENVIRONMENT_ANOMA);

        // Initialize the previous contribution hash with a no-op value.
        let mut previous_contribution_hash: GenericArray<u8, _> =
            GenericArray::from_slice(vec![0; 64].as_slice()).clone();

        // Generate a new challenge for the given parameters.
        for chunk_id in 0..number_of_chunks {
            debug!("Initializing test chunk {}", chunk_id);

            // Execute the ceremony initialization
            let candidate_hash =
                Initialization::run(&TEST_ENVIRONMENT_ANOMA, &mut storage, round_height, chunk_id).unwrap();

            // Open the contribution locator file.
            let locator = Locator::ContributionFile(ContributionLocator::new(round_height, chunk_id, 0, true));
            let reader = storage.reader(&locator).unwrap();

            // Check that the contribution chunk was generated based on the blank hash.
            let hash = blank_hash();

            debug!("blank hash is {}", pretty_hash!(&hash));
            let challenge_hash = calculate_hash(&reader);
            debug!("reader hash is {}", pretty_hash!(challenge_hash));
            debug!("reader is {}", pretty_hash!(&reader[0..255]));
            for (i, (expected, candidate)) in hash.iter().zip(reader.as_ref().chunks(64).next().unwrap()).enumerate() {
                trace!(
                    "Checking byte {} of expected hash: {:02x} =? {:02x}",
                    i,
                    expected,
                    candidate
                );
                assert_eq!(expected, candidate);
            }

            // If chunk ID is under (number_of_chunks / 2), the contribution hash
            // of each iteration will match with Groth16 and Marlin.
            if chunk_id < (number_of_chunks / 2) as u64 {
                // Sanity only - Check that the current contribution hash matches the previous one.
                let contribution_hash = calculate_hash(reader.as_ref());
                assert_eq!(contribution_hash.to_vec(), candidate_hash);
                match chunk_id == 0 {
                    true => previous_contribution_hash = contribution_hash,
                    false => {
                        assert_eq!(previous_contribution_hash, contribution_hash);
                        previous_contribution_hash = contribution_hash;
                    }
                }
                trace!(
                    "The contribution hash of chunk {} is {}",
                    chunk_id,
                    pretty_hash!(&contribution_hash)
                );
            }
        }
    }
}
