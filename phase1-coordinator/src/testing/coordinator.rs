use crate::{
    authentication::Dummy,
    environment::{Environment, Parameters, Testing},
    objects::{Participant, Round},
    storage::Disk,
    Coordinator,
    CoordinatorError,
};

use once_cell::sync::Lazy;
use serde_diff::{Diff, SerdeDiff};
#[cfg(test)]
use serial_test::serial;
use std::{path::Path, sync::Arc};
use time::{macros::datetime, OffsetDateTime};
use tracing::*;

use fs_err as fs;
use once_cell::sync::OnceCell;

static INSTANCE: OnceCell<()> = OnceCell::new();

/// Environment for testing purposes only.
pub static TEST_ENVIRONMENT: Lazy<Environment> = Lazy::new(|| Testing::from(Parameters::Test8Chunks).into());

/// Environment for testing purposes only.
pub static TEST_ENVIRONMENT_3: Lazy<Environment> = Lazy::new(|| Testing::from(Parameters::Test3Chunks).into());

/// Environment for testing purposes only.
pub static TEST_ENVIRONMENT_ANOMA: Lazy<Environment> = Lazy::new(|| {
    Testing::from(Parameters::TestCustom {
        number_of_chunks: 1,
        power: 8,
        batch_size: 128,
    })
    .into()
});

/// Round start datetime for testing purposes only.
pub static TEST_STARTED_AT: Lazy<OffsetDateTime> = Lazy::new(|| datetime!(1970-01-01 00:01:01 UTC));

/// Contributor ID for testing purposes only.
pub static TEST_CONTRIBUTOR_ID: Lazy<Participant> =
    Lazy::new(|| test_coordinator_contributor(&TEST_ENVIRONMENT).unwrap());

/// Contributor ID 2 for testing purposes only.
pub static TEST_CONTRIBUTOR_ID_2: Lazy<Participant> =
    Lazy::new(|| Participant::Contributor("testing-coordinator-contributor-2".to_string()));

/// Contributor ID 3 for testing purposes only.
pub static TEST_CONTRIBUTOR_ID_3: Lazy<Participant> =
    Lazy::new(|| Participant::Contributor("testing-coordinator-contributor-3".to_string()));

/// Verifier ID for testing purposes only.
pub static TEST_VERIFIER_ID: Lazy<Participant> = Lazy::new(|| test_coordinator_verifier(&TEST_ENVIRONMENT).unwrap());

/// Verifier ID 2 for testing purposes only.
pub static TEST_VERIFIER_ID_2: Lazy<Participant> =
    Lazy::new(|| Participant::Verifier("testing-coordinator-verifier-2".to_string()));

/// Verifier ID 2 for testing purposes only.
pub static TEST_VERIFIER_ID_3: Lazy<Participant> =
    Lazy::new(|| Participant::Verifier("testing-coordinator-verifier-3".to_string()));

/// Contributor IDs for testing purposes only.
pub static TEST_CONTRIBUTOR_IDS: Lazy<Vec<Participant>> = Lazy::new(|| vec![Lazy::force(&TEST_CONTRIBUTOR_ID).clone()]);

/// Verifier IDs for testing purposes only.
pub static TEST_VERIFIER_IDS: Lazy<Vec<Participant>> = Lazy::new(|| vec![Lazy::force(&TEST_VERIFIER_ID).clone()]);

pub fn test_coordinator(environment: &Environment) -> anyhow::Result<Coordinator> {
    info!("Starting coordinator");
    let coordinator = Coordinator::new(environment.clone(), Arc::new(Dummy))?;
    info!("Coordinator is ready");
    Ok(coordinator)
}

pub fn test_coordinator_contributor(environment: &Environment) -> anyhow::Result<Participant> {
    Ok(environment
        .coordinator_contributors()
        .first()
        .ok_or(CoordinatorError::ContributorsMissing)?
        .clone())
}

pub fn test_coordinator_verifier(environment: &Environment) -> anyhow::Result<Participant> {
    Ok(environment
        .coordinator_verifiers()
        .first()
        .ok_or(CoordinatorError::VerifierMissing)?
        .clone())
}

pub fn initialize_test_environment(environment: &Environment) -> Environment {
    test_logger();

    clear_test_storage(environment);
    environment.clone()
}

pub(crate) fn test_logger() {
    INSTANCE.get_or_init(|| {
        tracing_subscriber::fmt::init();
    });
}

/// Clears the transcript directory for testing purposes only.
pub fn clear_test_storage(environment: &Environment) {
    let path = environment.local_base_directory();
    if Path::new(path).exists() {
        warn!("Coordinator is clearing {:?}", &path);
        match fs::remove_dir_all(&path) {
            Ok(_) => (),
            Err(error) => error!(
                "The testing framework tried to clear the test transcript and failed. {}",
                error
            ),
        }
        warn!("Coordinator cleared {:?}", &path);
    }
}

/// Initializes a test storage object.
pub fn test_storage(environment: &Environment) -> Disk {
    environment.storage().unwrap()
}

/// Loads the reference JSON object with a serialized round for testing purposes only.
pub fn test_round_0_json() -> anyhow::Result<Round> {
    Ok(serde_json::from_str(include_str!("resources/test_round_0.json"))?)
}

/// Loads the reference JSON object with a serialized round for testing purposes only.
pub fn test_round_1_initial_json() -> anyhow::Result<Round> {
    Ok(serde_json::from_str(include_str!(
        "resources/test_round_1_initial.json"
    ))?)
}

/// Loads the reference JSON object with a serialized round for testing purposes only.
pub fn test_round_1_partial_json() -> anyhow::Result<Round> {
    Ok(serde_json::from_str(include_str!(
        "resources/test_round_1_partial.json"
    ))?)
}

/// Creates the initial round for testing purposes only.
pub fn test_round_0() -> anyhow::Result<Round> {
    // Define test storage.
    let mut test_storage = test_storage(&TEST_ENVIRONMENT);

    Ok(Round::new(
        &TEST_ENVIRONMENT,
        &mut test_storage,
        0, /* height */
        *TEST_STARTED_AT,
        vec![],
    )?)
}

/// Prints the difference in JSON objects between `a` and `b`.
pub fn print_diff<S: SerdeDiff>(a: &S, b: &S) {
    println!(
        "\nDifference(s) between left and right values\n-------------------------------------------\n{}\n",
        serde_json::to_string_pretty(&Diff::serializable(a, b)).unwrap()
    );
}

#[test]
#[serial]
fn test_round_0_matches() {
    initialize_test_environment(&TEST_ENVIRONMENT);

    let expected = test_round_0_json().unwrap();
    let candidate = test_round_0().unwrap();

    // Print the differences in JSON if they do not match.
    if candidate != expected {
        print_diff(&expected, &candidate);
    }
    assert_eq!(candidate, expected);
}
