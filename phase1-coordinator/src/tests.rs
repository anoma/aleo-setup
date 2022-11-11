use crate::{
    authentication::Dummy,
    commands::{Seed, SigningKey, SEED_LENGTH},
    environment::{Environment, Parameters, Settings, Testing},
    objects::Task,
    storage::{Disk, StorageLocator},
    testing::prelude::*,
    Coordinator,
    CoordinatorError,
    MockTimeSource,
    Participant,
    Round,
};
use phase1::{helpers::CurveKind, ContributionMode, ProvingSystem};
use time::OffsetDateTime;

use fs_err as fs;
use rand::RngCore;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use std::{
    collections::{HashSet, LinkedList},
    iter::FromIterator,
    net::{IpAddr, Ipv4Addr},
    sync::Arc,
};

fn create_contributor(id: &str) -> (Participant, SigningKey, Seed) {
    let contributor = Participant::Contributor(format!("test-contributor-{}", id));
    let contributor_signing_key: SigningKey = "secret_key".to_string();

    let mut seed: Seed = [0; SEED_LENGTH];
    rand::thread_rng().fill_bytes(&mut seed[..]);

    (contributor, contributor_signing_key, seed)
}

fn create_verifier(id: &str) -> (Participant, SigningKey) {
    let verifier = Participant::Verifier(format!("test-verifier-{}", id));
    let verifier_signing_key: SigningKey = "secret_key".to_string();

    (verifier, verifier_signing_key)
}

fn make_tasks(items: &[(u64, u64)]) -> LinkedList<Task> {
    let iterator = items
        .iter()
        .map(|&(chunk_id, contribution_id)| Task::new(chunk_id, contribution_id));
    LinkedList::from_iter(iterator)
}

struct ContributorTestDetails {
    participant: Participant,
    signing_key: SigningKey,
    seed: Seed,
}

impl ContributorTestDetails {
    fn contribute_to(&self, coordinator: &mut Coordinator) -> Result<(), CoordinatorError> {
        coordinator.contribute(&self.participant, &self.signing_key, &self.seed)
    }
}

fn create_contributor_test_details(id: &str) -> ContributorTestDetails {
    let (participant, signing_key, seed) = create_contributor(id);
    ContributorTestDetails {
        participant,
        signing_key,
        seed,
    }
}

struct VerifierTestDetails {
    participant: Participant,
    signing_key: SigningKey,
}

impl VerifierTestDetails {
    /// If there are pending verifications, grab one and verify it.
    /// Otherwise do nothing
    fn verify_if_available(&self, coordinator: &mut Coordinator) -> anyhow::Result<()> {
        verify_task_if_available(coordinator, &self.participant, &self.signing_key)
    }
}

fn create_verifier_test_details(id: &str) -> VerifierTestDetails {
    let (participant, signing_key) = create_verifier(id);
    VerifierTestDetails {
        participant,
        signing_key,
    }
}

fn execute_round(proving_system: ProvingSystem, curve: CurveKind) -> anyhow::Result<()> {
    let parameters = Parameters::Custom(Settings::new(
        ContributionMode::Chunked,
        proving_system,
        curve,
        7,  /* power */
        32, /* batch_size */
        32, /* chunk_size */
    ));
    let environment = initialize_test_environment(&Testing::from(parameters).into());
    let number_of_chunks = environment.number_of_chunks() as usize;

    // Instantiate a coordinator.
    let mut coordinator = Coordinator::new(environment, Arc::new(Dummy))?;

    // Initialize the ceremony to round 0.
    coordinator.initialize()?;
    assert_eq!(0, coordinator.current_round_height()?);

    // Meanwhile, add a contributor and verifier to the queue.
    let (contributor, contributor_signing_key, seed) = create_contributor("1");
    let contributor_ip = IpAddr::V4(Ipv4Addr::UNSPECIFIED);
    let token = String::from("test_token");
    let (verifier, verifier_signing_key) = create_verifier("1");
    coordinator.add_to_queue(contributor.clone(), Some(contributor_ip), token.clone(), 10)?;
    assert_eq!(1, coordinator.number_of_queue_contributors());

    // Advance the ceremony from round 0 to round 1.
    coordinator.update()?;
    assert_eq!(1, coordinator.current_round_height()?);
    assert_eq!(0, coordinator.number_of_queue_contributors());

    // Run contribution and verification for round 1.
    for _ in 0..number_of_chunks {
        coordinator.contribute(&contributor, &contributor_signing_key, &seed)?;
        verify_task_if_available(&mut coordinator, &verifier, &verifier_signing_key)?;
    }

    //
    // Meanwhile, add a contributor and verifier to the queue.
    //
    // Note: This logic for adding to the queue works because
    // `Environment::allow_current_contributors_in_queue`
    // and `Environment::allow_current_verifiers_in_queue`
    // are set to `true`. This section can be removed without
    // changing the outcome of this test, if necessary.
    //
    let (contributor, _, _) = create_contributor("1");
    coordinator.add_to_queue(contributor.clone(), Some(contributor_ip), token, 10)?;
    assert_eq!(1, coordinator.number_of_queue_contributors());

    // Update the ceremony from round 1 to round 2.
    coordinator.update()?;
    assert_eq!(2, coordinator.current_round_height()?);
    assert_eq!(0, coordinator.number_of_queue_contributors());

    Ok(())
}

/*
    Drop Participant Tests

    1. Basic drop - `coordinator_drop_contributor_basic`
        Drop a contributor that does not affect other contributors/verifiers.

    2. Given 3 contributors, drop middle contributor - `coordinator_drop_contributor_in_between_two_contributors`
        Given contributors 1, 2, and 3, drop contributor 2 and ensure that the tasks are present.

    3. Drop contributor with pending tasks - `coordinator_drop_contributor_with_contributors_in_pending_tasks`
        Drops a contributor with other contributors in pending tasks.

    4. Drop contributor with a locked chunk - `coordinator_drop_contributor_with_locked_chunk`
        Test that dropping a contributor releases the locks held by the dropped contributor.

    5. Dropping a contributor removes provided contributions - `coordinator_drop_contributor_removes_contributions`
        Test that dropping a contributor will remove all the contributions that the dropped contributor has provided.

    6. Dropping a participant clears lock for subsequent contributors/verifiers - `coordinator_drop_contributor_clear_locks`
        Test that if a contribution is dropped from a chunk, while a  contributor/verifier is performing their contribution,
        the lock should be released after the task has been disposed. The disposed task should also be reassigned correctly.
        Currently, the lock is release and the task is disposed after the contributor/verifier calls `try_contribute` or `try_verify`.

    7. Dropping a contributor removes all subsequent contributions  - `coordinator_drop_contributor_removes_subsequent_contributions`
        If a contributor is dropped, all contributions built on top of the dropped contributions must also
        be dropped.

    8. Dropping multiple contributors allocates tasks to the coordinator contributor correctly - `coordinator_drop_multiple_contributors`
        Pick contributor with least load in `add_replacement_contributor_unsafe`.

    9. Current contributor/verifier `completed_tasks` should be removed/moved when a participant is dropped
       and tasks need to be recomputed - UNTESTED
        The tasks declared in the state file should be updated correctly when a participant is dropped.

    10. The coordinator contributor should replace all dropped participants and complete the round correctly. - `drop_all_contributors_and_complete_round`

    11. Drop one contributor and check that completed tasks are reassigned properly, - `drop_contributor_and_reassign_tasks`
        as well as a replacement contributor has the right amount of tasks assigned

*/

/// If there are pending verifications, grab one and verify it.
/// Otherwise do nothing
fn verify_task_if_available(
    coordinator: &mut Coordinator,
    verifier: &Participant,
    signing_key: &SigningKey,
) -> anyhow::Result<()> {
    let pending_tasks = coordinator.get_pending_verifications();
    if let Some(task) = pending_tasks.keys().next().cloned() {
        coordinator.verify(&verifier, signing_key, &task)?;
    }
    Ok(())
}

fn fetch_task_for_verifier(coordinator: &Coordinator) -> Option<Task> {
    coordinator.get_pending_verifications().keys().next().cloned()
}

#[test]
#[serial]
/// Drops a contributor who does not affect other contributors or verifiers.
fn coordinator_drop_contributor_basic() {
    let parameters = Parameters::Custom(Settings::new(
        ContributionMode::Chunked,
        ProvingSystem::Groth16,
        CurveKind::Bls12_377,
        6,  /* power */
        16, /* batch_size */
        16, /* chunk_size */
    ));
    let environment = initialize_test_environment(&Testing::from(parameters).into());
    let number_of_chunks = environment.number_of_chunks() as usize;

    // Instantiate a coordinator.
    let mut coordinator = Coordinator::new(environment, Arc::new(Dummy)).unwrap();

    // Initialize the ceremony to round 0.
    coordinator.initialize().unwrap();
    assert_eq!(0, coordinator.current_round_height().unwrap());

    // Add a contributor and verifier to the queue.
    let token1 = String::from("test_token");
    let token2 = String::from("test_token_2");
    let (contributor1, contributor_signing_key1, seed1) = create_contributor("1");
    let contributor_1_ip = IpAddr::V4("0.0.0.1".parse().unwrap());
    let (contributor2, contributor_signing_key2, seed2) = create_contributor("2");
    let contributor_2_ip = IpAddr::V4("0.0.0.2".parse().unwrap());
    let (verifier, verifier_signing_key) = create_verifier("1");
    coordinator
        .add_to_queue(contributor1.clone(), Some(contributor_1_ip), token1, 10)
        .unwrap();
    coordinator
        .add_to_queue(contributor2.clone(), Some(contributor_2_ip), token2, 9)
        .unwrap();
    assert_eq!(2, coordinator.number_of_queue_contributors());
    assert!(coordinator.is_queue_contributor(&contributor1));
    assert!(coordinator.is_queue_contributor(&contributor2));
    assert!(!coordinator.is_current_contributor(&contributor1));
    assert!(!coordinator.is_current_contributor(&contributor2));
    assert!(!coordinator.is_finished_contributor(&contributor1));
    assert!(!coordinator.is_finished_contributor(&contributor2));

    // Update the ceremony to round 1.
    coordinator.update().unwrap();
    assert_eq!(1, coordinator.current_round_height().unwrap());
    assert_eq!(0, coordinator.number_of_queue_contributors());
    assert!(!coordinator.is_queue_contributor(&contributor1));
    assert!(!coordinator.is_queue_contributor(&contributor2));
    assert!(coordinator.is_current_contributor(&contributor1));
    assert!(coordinator.is_current_contributor(&contributor2));
    assert!(!coordinator.is_finished_contributor(&contributor1));
    assert!(!coordinator.is_finished_contributor(&contributor2));

    // Contribute and verify up to the penultimate chunk.
    for _ in 0..(number_of_chunks - 1) as u64 {
        coordinator
            .contribute(&contributor1, &contributor_signing_key1, &seed1)
            .unwrap();
        coordinator
            .contribute(&contributor2, &contributor_signing_key2, &seed2)
            .unwrap();
        verify_task_if_available(&mut coordinator, &verifier, &verifier_signing_key).unwrap();
        verify_task_if_available(&mut coordinator, &verifier, &verifier_signing_key).unwrap();
    }
    assert!(!coordinator.is_queue_contributor(&contributor1));
    assert!(!coordinator.is_queue_contributor(&contributor2));
    assert!(coordinator.is_current_contributor(&contributor1));
    assert!(coordinator.is_current_contributor(&contributor2));
    assert!(!coordinator.is_finished_contributor(&contributor1));
    assert!(!coordinator.is_finished_contributor(&contributor2));

    // Drop the contributor from the current round.
    coordinator.drop_participant(&contributor1).unwrap();

    assert!(!coordinator.is_queue_contributor(&contributor1));
    assert!(!coordinator.is_queue_contributor(&contributor2));
    assert!(!coordinator.is_current_contributor(&contributor1));
    assert!(coordinator.is_current_contributor(&contributor2));
    assert!(!coordinator.is_finished_contributor(&contributor1));
    assert!(!coordinator.is_finished_contributor(&contributor2));

    // Check that contributor 1 was dropped and coordinator state was updated.
    let contributors = coordinator.current_contributors();
    assert_eq!(2, contributors.len());
    assert_eq!(0, contributors.par_iter().filter(|(p, _)| *p == contributor1).count());
    for (contributor, contributor_info) in contributors {
        if contributor == contributor2 {
            assert_eq!(0, contributor_info.locked_chunks().len());
            assert_eq!(4, contributor_info.assigned_tasks().len());
            assert_eq!(0, contributor_info.pending_tasks().len());
            assert_eq!(4, contributor_info.completed_tasks().len());
            assert_eq!(0, contributor_info.disposing_tasks().len());
            assert_eq!(3, contributor_info.disposed_tasks().len());
        } else {
            assert_eq!(0, contributor_info.locked_chunks().len());
            assert_eq!(8, contributor_info.assigned_tasks().len());
            assert_eq!(0, contributor_info.pending_tasks().len());
            assert_eq!(0, contributor_info.completed_tasks().len());
            assert_eq!(0, contributor_info.disposing_tasks().len());
            assert_eq!(0, contributor_info.disposed_tasks().len());
        }
    }

    // Print the coordinator state.
    let state = coordinator.state();
    debug!("{}", serde_json::to_string_pretty(&state).unwrap());
    assert_eq!(1, state.current_round_height());

    debug!(
        "{}",
        serde_json::to_string_pretty(&coordinator.current_round().unwrap()).unwrap()
    );
}

#[test]
#[serial]
/// Drops a contributor in between two contributors.
fn coordinator_drop_contributor_in_between_two_contributors() -> anyhow::Result<()> {
    let parameters = Parameters::Custom(Settings::new(
        ContributionMode::Chunked,
        ProvingSystem::Groth16,
        CurveKind::Bls12_377,
        6,  /* power */
        16, /* batch_size */
        16, /* chunk_size */
    ));
    let environment = initialize_test_environment(&Testing::from(parameters).into());
    let number_of_chunks = environment.number_of_chunks() as usize;

    // Instantiate a coordinator.
    let mut coordinator = Coordinator::new(environment.clone(), Arc::new(Dummy))?;

    // Initialize the ceremony to round 0.
    coordinator.initialize()?;
    assert_eq!(0, coordinator.current_round_height()?);

    // Add a contributor and verifier to the queue.
    let token = String::from("test_token");
    let token2 = String::from("test_token_2");
    let token3 = String::from("test_token_3");
    let (contributor1, contributor_signing_key1, seed1) = create_contributor("1");
    let contributor_1_ip = IpAddr::V4("0.0.0.1".parse().unwrap());
    let (contributor2, contributor_signing_key2, seed2) = create_contributor("2");
    let contributor_2_ip = IpAddr::V4("0.0.0.2".parse().unwrap());
    let (contributor3, contributor_signing_key3, seed3) = create_contributor("3");
    let contributor_3_ip = IpAddr::V4("0.0.0.3".parse().unwrap());
    let (verifier, verifier_signing_key) = create_verifier("1");
    coordinator.add_to_queue(contributor1.clone(), Some(contributor_1_ip), token, 10)?;
    coordinator.add_to_queue(contributor2.clone(), Some(contributor_2_ip), token2, 9)?;
    coordinator.add_to_queue(contributor3.clone(), Some(contributor_3_ip), token3, 8)?;
    assert_eq!(3, coordinator.number_of_queue_contributors());

    // Update the ceremony to round 1.
    coordinator.update()?;
    assert_eq!(1, coordinator.current_round_height()?);
    assert_eq!(0, coordinator.number_of_queue_contributors());

    // Contribute and verify up to the penultimate chunk.
    for _ in 0..(number_of_chunks - 1) {
        coordinator.contribute(&contributor1, &contributor_signing_key1, &seed1)?;
        coordinator.contribute(&contributor2, &contributor_signing_key2, &seed2)?;
        coordinator.contribute(&contributor3, &contributor_signing_key3, &seed3)?;
        verify_task_if_available(&mut coordinator, &verifier, &verifier_signing_key)?;
        verify_task_if_available(&mut coordinator, &verifier, &verifier_signing_key)?;
        verify_task_if_available(&mut coordinator, &verifier, &verifier_signing_key)?;
    }
    assert!(!coordinator.is_queue_contributor(&contributor1));
    assert!(!coordinator.is_queue_contributor(&contributor2));
    assert!(!coordinator.is_queue_contributor(&contributor3));
    assert!(coordinator.is_current_contributor(&contributor1));
    assert!(coordinator.is_current_contributor(&contributor2));
    assert!(coordinator.is_current_contributor(&contributor3));
    assert!(!coordinator.is_finished_contributor(&contributor1));
    assert!(!coordinator.is_finished_contributor(&contributor2));
    assert!(!coordinator.is_finished_contributor(&contributor3));

    // Drop the contributor from the current round.
    coordinator.drop_participant(&contributor2)?;
    assert!(!coordinator.is_queue_contributor(&contributor1));
    assert!(!coordinator.is_queue_contributor(&contributor2));
    assert!(!coordinator.is_queue_contributor(&contributor3));
    assert!(coordinator.is_current_contributor(&contributor1));
    assert!(!coordinator.is_current_contributor(&contributor2));
    assert!(coordinator.is_current_contributor(&contributor3));
    assert!(!coordinator.is_finished_contributor(&contributor1));
    assert!(!coordinator.is_finished_contributor(&contributor2));
    assert!(!coordinator.is_finished_contributor(&contributor3));

    // Print the coordinator state.
    let state = coordinator.state();
    debug!("{}", serde_json::to_string_pretty(&state)?);
    assert_eq!(1, state.current_round_height());

    // Check that contributor 2 was dropped and coordinator state was updated.
    let contributors = coordinator.current_contributors();
    assert_eq!(3, contributors.len());
    assert_eq!(0, contributors.par_iter().filter(|(p, _)| *p == contributor2).count());
    let mut tasks: HashSet<Task> = HashSet::new();
    for (contributor, contributor_info) in contributors {
        if contributor == contributor1 {
            tasks.extend(contributor_info.assigned_tasks().iter());
            tasks.extend(contributor_info.completed_tasks().iter());
            assert_eq!(0, contributor_info.locked_chunks().len());
            assert_eq!(6, contributor_info.assigned_tasks().len());
            assert_eq!(0, contributor_info.pending_tasks().len());
            assert_eq!(2, contributor_info.completed_tasks().len());
            assert_eq!(0, contributor_info.disposing_tasks().len());
            assert_eq!(5, contributor_info.disposed_tasks().len());
        } else if contributor == contributor3 {
            tasks.extend(contributor_info.assigned_tasks().iter());
            tasks.extend(contributor_info.completed_tasks().iter());
            assert_eq!(0, contributor_info.locked_chunks().len());
            assert_eq!(2, contributor_info.assigned_tasks().len());
            assert_eq!(0, contributor_info.pending_tasks().len());
            assert_eq!(6, contributor_info.completed_tasks().len());
            assert_eq!(0, contributor_info.disposing_tasks().len());
            assert_eq!(1, contributor_info.disposed_tasks().len());
        } else {
            tasks.extend(contributor_info.assigned_tasks().iter());
            assert_eq!(0, contributor_info.locked_chunks().len());
            assert_eq!(8, contributor_info.assigned_tasks().len());
            assert_eq!(0, contributor_info.pending_tasks().len());
            assert_eq!(0, contributor_info.completed_tasks().len());
            assert_eq!(0, contributor_info.disposing_tasks().len());
            assert_eq!(0, contributor_info.disposed_tasks().len());
        }
    }

    // Check that all tasks are present.
    assert_eq!(24, tasks.len());
    for chunk_id in 0..environment.number_of_chunks() {
        for contribution_id in 1..4 {
            debug!("Checking {:?}", Task::new(chunk_id, contribution_id));
            assert!(tasks.contains(&Task::new(chunk_id, contribution_id)));
        }
    }

    Ok(())
}

#[test]
#[serial]
/// Drops a contributor with other contributors in pending tasks.
fn coordinator_drop_contributor_with_contributors_in_pending_tasks() -> anyhow::Result<()> {
    let parameters = Parameters::Custom(Settings::new(
        ContributionMode::Chunked,
        ProvingSystem::Groth16,
        CurveKind::Bls12_377,
        6,  /* power */
        16, /* batch_size */
        16, /* chunk_size */
    ));
    let environment = initialize_test_environment(&Testing::from(parameters).into());
    let number_of_chunks = environment.number_of_chunks() as usize;

    // Instantiate a coordinator.
    let mut coordinator = Coordinator::new(environment.clone(), Arc::new(Dummy))?;

    // Initialize the ceremony to round 0.
    coordinator.initialize()?;
    assert_eq!(0, coordinator.current_round_height()?);

    // Add a contributor and verifier to the queue.
    let token = String::from("test_token");
    let token2 = String::from("test_token_2");
    let token3 = String::from("test_token_3");
    let (contributor1, contributor_signing_key1, seed1) = create_contributor("1");
    let contributor_1_ip = IpAddr::V4("0.0.0.1".parse().unwrap());
    let (contributor2, contributor_signing_key2, seed2) = create_contributor("2");
    let contributor_2_ip = IpAddr::V4("0.0.0.2".parse().unwrap());
    let (contributor3, contributor_signing_key3, seed3) = create_contributor("3");
    let contributor_3_ip = IpAddr::V4("0.0.0.3".parse().unwrap());
    let (verifier, verifier_signing_key) = create_verifier("1");
    coordinator.add_to_queue(contributor1.clone(), Some(contributor_1_ip), token, 10)?;
    coordinator.add_to_queue(contributor2.clone(), Some(contributor_2_ip), token2, 9)?;
    coordinator.add_to_queue(contributor3.clone(), Some(contributor_3_ip), token3, 8)?;
    assert_eq!(3, coordinator.number_of_queue_contributors());

    // Update the ceremony to round 1.
    coordinator.update()?;
    assert_eq!(1, coordinator.current_round_height()?);
    assert_eq!(0, coordinator.number_of_queue_contributors());

    // Contribute and verify up to 2 before the final chunk.
    for _ in 0..(number_of_chunks - 2) {
        coordinator.contribute(&contributor1, &contributor_signing_key1, &seed1)?;
        coordinator.contribute(&contributor2, &contributor_signing_key2, &seed2)?;
        coordinator.contribute(&contributor3, &contributor_signing_key3, &seed3)?;
        verify_task_if_available(&mut coordinator, &verifier, &verifier_signing_key)?;
        verify_task_if_available(&mut coordinator, &verifier, &verifier_signing_key)?;
        verify_task_if_available(&mut coordinator, &verifier, &verifier_signing_key)?;
    }

    // Lock the next task for contributor 1 and 3.
    coordinator.try_lock(&contributor1)?;
    coordinator.try_lock(&contributor3)?;

    // Check that coordinator state includes a pending task for contributor 1 and 3.
    let contributors = coordinator.current_contributors();
    assert_eq!(3, contributors.len());
    assert_eq!(1, contributors.par_iter().filter(|(p, _)| *p == contributor2).count());
    let mut tasks: HashSet<Task> = HashSet::new();
    for (contributor, contributor_info) in contributors {
        if contributor == contributor1 || contributor == contributor3 {
            tasks.extend(contributor_info.assigned_tasks().iter());
            tasks.extend(contributor_info.pending_tasks().iter());
            tasks.extend(contributor_info.completed_tasks().iter());
            assert_eq!(1, contributor_info.locked_chunks().len());
            assert_eq!(1, contributor_info.assigned_tasks().len());
            assert_eq!(1, contributor_info.pending_tasks().len());
            assert_eq!(6, contributor_info.completed_tasks().len());
            assert_eq!(0, contributor_info.disposing_tasks().len());
            assert_eq!(0, contributor_info.disposed_tasks().len());
        } else {
            tasks.extend(contributor_info.assigned_tasks().iter());
            tasks.extend(contributor_info.completed_tasks().iter());
            assert_eq!(0, contributor_info.locked_chunks().len());
            assert_eq!(2, contributor_info.assigned_tasks().len());
            assert_eq!(0, contributor_info.pending_tasks().len());
            assert_eq!(6, contributor_info.completed_tasks().len());
            assert_eq!(0, contributor_info.disposing_tasks().len());
            assert_eq!(0, contributor_info.disposed_tasks().len());
        }
    }

    // Check that all tasks are present.
    assert_eq!(24, tasks.len());
    for chunk_id in 0..environment.number_of_chunks() {
        for contribution_id in 1..4 {
            debug!("Checking {:?}", Task::new(chunk_id, contribution_id));
            assert!(tasks.contains(&Task::new(chunk_id, contribution_id)));
        }
    }

    // Drop the contributor from the current round.
    coordinator.drop_participant(&contributor2)?;
    assert!(!coordinator.is_queue_contributor(&contributor1));
    assert!(!coordinator.is_queue_contributor(&contributor2));
    assert!(!coordinator.is_queue_contributor(&contributor3));
    assert!(coordinator.is_current_contributor(&contributor1));
    assert!(!coordinator.is_current_contributor(&contributor2));
    assert!(coordinator.is_current_contributor(&contributor3));
    assert!(!coordinator.is_finished_contributor(&contributor1));
    assert!(!coordinator.is_finished_contributor(&contributor2));
    assert!(!coordinator.is_finished_contributor(&contributor3));

    // Print the coordinator state.
    let state = coordinator.state();
    debug!("{}", serde_json::to_string_pretty(&state)?);
    assert_eq!(1, state.current_round_height());

    // Check that contributor 2 was dropped and coordinator state was updated.
    let contributors = coordinator.current_contributors();
    assert_eq!(3, contributors.len());
    assert_eq!(0, contributors.par_iter().filter(|(p, _)| *p == contributor2).count());
    let mut tasks: HashSet<Task> = HashSet::new();
    for (contributor, contributor_info) in contributors {
        if contributor == contributor1 {
            tasks.extend(contributor_info.assigned_tasks().iter());
            tasks.extend(contributor_info.completed_tasks().iter());
            assert_eq!(1, contributor_info.locked_chunks().len());
            assert_eq!(6, contributor_info.assigned_tasks().len());
            assert_eq!(0, contributor_info.pending_tasks().len());
            assert_eq!(2, contributor_info.completed_tasks().len());
            assert_eq!(1, contributor_info.disposing_tasks().len());
            assert_eq!(4, contributor_info.disposed_tasks().len());
        } else if contributor == contributor3 {
            tasks.extend(contributor_info.assigned_tasks().iter());
            tasks.extend(contributor_info.pending_tasks().iter());
            tasks.extend(contributor_info.completed_tasks().iter());
            assert_eq!(1, contributor_info.locked_chunks().len());
            assert_eq!(2, contributor_info.assigned_tasks().len());
            assert_eq!(0, contributor_info.pending_tasks().len());
            assert_eq!(6, contributor_info.completed_tasks().len());
            assert_eq!(1, contributor_info.disposing_tasks().len());
            assert_eq!(0, contributor_info.disposed_tasks().len());
        } else {
            tasks.extend(contributor_info.assigned_tasks().iter());
            assert_eq!(0, contributor_info.locked_chunks().len());
            assert_eq!(8, contributor_info.assigned_tasks().len());
            assert_eq!(0, contributor_info.pending_tasks().len());
            assert_eq!(0, contributor_info.completed_tasks().len());
            assert_eq!(0, contributor_info.disposing_tasks().len());
            assert_eq!(0, contributor_info.disposed_tasks().len());
        }
    }

    // Check that all tasks are present.
    assert_eq!(24, tasks.len());
    for chunk_id in 0..environment.number_of_chunks() {
        for contribution_id in 1..4 {
            debug!("Checking {:?}", Task::new(chunk_id, contribution_id));
            assert!(tasks.contains(&Task::new(chunk_id, contribution_id)));
        }
    }

    Ok(())
}

#[test]
#[serial]
/// Drops a contributor with locked chunks and other contributors in pending tasks.
fn coordinator_drop_contributor_locked_chunks() -> anyhow::Result<()> {
    let parameters = Parameters::Custom(Settings::new(
        ContributionMode::Chunked,
        ProvingSystem::Groth16,
        CurveKind::Bls12_377,
        6,  /* power */
        16, /* batch_size */
        16, /* chunk_size */
    ));
    let environment = initialize_test_environment(&Testing::from(parameters).into());
    let number_of_chunks = environment.number_of_chunks() as usize;

    // Instantiate a coordinator.
    let mut coordinator = Coordinator::new(environment.clone(), Arc::new(Dummy))?;

    // Initialize the ceremony to round 0.
    coordinator.initialize()?;
    assert_eq!(0, coordinator.current_round_height()?);

    // Add a contributor and verifier to the queue.
    let token = String::from("test_token");
    let token2 = String::from("test_token_2");
    let token3 = String::from("test_token_3");
    let (contributor1, contributor_signing_key1, seed1) = create_contributor("1");
    let contributor_1_ip = IpAddr::V4("0.0.0.1".parse().unwrap());
    let (contributor2, contributor_signing_key2, seed2) = create_contributor("2");
    let contributor_2_ip = IpAddr::V4("0.0.0.2".parse().unwrap());
    let (contributor3, contributor_signing_key3, seed3) = create_contributor("3");
    let contributor_3_ip = IpAddr::V4("0.0.0.3".parse().unwrap());
    let (verifier, verifier_signing_key) = create_verifier("1");
    coordinator.add_to_queue(contributor1.clone(), Some(contributor_1_ip), token, 10)?;
    coordinator.add_to_queue(contributor2.clone(), Some(contributor_2_ip), token2, 9)?;
    coordinator.add_to_queue(contributor3.clone(), Some(contributor_3_ip), token3, 8)?;
    assert_eq!(3, coordinator.number_of_queue_contributors());

    // Update the ceremony to round 1.
    coordinator.update()?;
    assert_eq!(1, coordinator.current_round_height()?);
    assert_eq!(0, coordinator.number_of_queue_contributors());

    // Contribute and verify up to 2 before the final chunk.
    for _ in 0..(number_of_chunks - 2) {
        coordinator.contribute(&contributor1, &contributor_signing_key1, &seed1)?;
        coordinator.contribute(&contributor2, &contributor_signing_key2, &seed2)?;
        coordinator.contribute(&contributor3, &contributor_signing_key3, &seed3)?;
        verify_task_if_available(&mut coordinator, &verifier, &verifier_signing_key)?;
        verify_task_if_available(&mut coordinator, &verifier, &verifier_signing_key)?;
        verify_task_if_available(&mut coordinator, &verifier, &verifier_signing_key)?;
    }

    // Lock the next task for contributor 1 and 3.
    coordinator.try_lock(&contributor1)?;
    coordinator.try_lock(&contributor3)?;

    // Check that coordinator state includes a pending task for contributor 1 and 3.
    let contributors = coordinator.current_contributors();
    assert_eq!(3, contributors.len());
    assert_eq!(1, contributors.par_iter().filter(|(p, _)| *p == contributor2).count());
    let mut tasks: HashSet<Task> = HashSet::new();
    for (contributor, contributor_info) in contributors {
        if contributor == contributor1 || contributor == contributor3 {
            tasks.extend(contributor_info.assigned_tasks().iter());
            tasks.extend(contributor_info.pending_tasks().iter());
            tasks.extend(contributor_info.completed_tasks().iter());
            assert_eq!(1, contributor_info.locked_chunks().len());
            assert_eq!(1, contributor_info.assigned_tasks().len());
            assert_eq!(1, contributor_info.pending_tasks().len());
            assert_eq!(6, contributor_info.completed_tasks().len());
            assert_eq!(0, contributor_info.disposing_tasks().len());
            assert_eq!(0, contributor_info.disposed_tasks().len());
        } else {
            tasks.extend(contributor_info.assigned_tasks().iter());
            tasks.extend(contributor_info.completed_tasks().iter());
            assert_eq!(0, contributor_info.locked_chunks().len());
            assert_eq!(2, contributor_info.assigned_tasks().len());
            assert_eq!(0, contributor_info.pending_tasks().len());
            assert_eq!(6, contributor_info.completed_tasks().len());
            assert_eq!(0, contributor_info.disposing_tasks().len());
            assert_eq!(0, contributor_info.disposed_tasks().len());
        }
    }

    // Check that all tasks are present.
    assert_eq!(24, tasks.len());
    for chunk_id in 0..environment.number_of_chunks() {
        for contribution_id in 1..4 {
            debug!("Checking {:?}", Task::new(chunk_id, contribution_id));
            assert!(tasks.contains(&Task::new(chunk_id, contribution_id)));
        }
    }

    // Lock the next task for contributor 2.
    coordinator.try_lock(&contributor2)?;

    // Drop the contributor from the current round.
    coordinator.drop_participant(&contributor2)?;
    assert!(!coordinator.is_queue_contributor(&contributor1));
    assert!(!coordinator.is_queue_contributor(&contributor2));
    assert!(!coordinator.is_queue_contributor(&contributor3));
    assert!(coordinator.is_current_contributor(&contributor1));
    assert!(!coordinator.is_current_contributor(&contributor2));
    assert!(coordinator.is_current_contributor(&contributor3));
    assert!(!coordinator.is_finished_contributor(&contributor1));
    assert!(!coordinator.is_finished_contributor(&contributor2));
    assert!(!coordinator.is_finished_contributor(&contributor3));

    // Print the coordinator state.
    let state = coordinator.state();
    debug!("{}", serde_json::to_string_pretty(&state)?);
    assert_eq!(1, state.current_round_height());

    // Check that contributor 2 was dropped and coordinator state was updated.
    let contributors = coordinator.current_contributors();
    assert_eq!(3, contributors.len());
    assert_eq!(0, contributors.par_iter().filter(|(p, _)| *p == contributor2).count());
    let mut tasks: HashSet<Task> = HashSet::new();
    for (contributor, contributor_info) in contributors {
        if contributor == contributor1 {
            tasks.extend(contributor_info.assigned_tasks().iter());
            tasks.extend(contributor_info.completed_tasks().iter());
            assert_eq!(1, contributor_info.locked_chunks().len());
            assert_eq!(6, contributor_info.assigned_tasks().len());
            assert_eq!(0, contributor_info.pending_tasks().len());
            assert_eq!(2, contributor_info.completed_tasks().len());
            assert_eq!(1, contributor_info.disposing_tasks().len());
            assert_eq!(4, contributor_info.disposed_tasks().len());
        } else if contributor == contributor3 {
            tasks.extend(contributor_info.assigned_tasks().iter());
            tasks.extend(contributor_info.pending_tasks().iter());
            tasks.extend(contributor_info.completed_tasks().iter());
            assert_eq!(1, contributor_info.locked_chunks().len());
            assert_eq!(2, contributor_info.assigned_tasks().len());
            assert_eq!(0, contributor_info.pending_tasks().len());
            assert_eq!(6, contributor_info.completed_tasks().len());
            assert_eq!(1, contributor_info.disposing_tasks().len());
            assert_eq!(0, contributor_info.disposed_tasks().len());
        } else {
            tasks.extend(contributor_info.assigned_tasks().iter());
            assert_eq!(0, contributor_info.locked_chunks().len());
            assert_eq!(8, contributor_info.assigned_tasks().len());
            assert_eq!(0, contributor_info.pending_tasks().len());
            assert_eq!(0, contributor_info.completed_tasks().len());
            assert_eq!(0, contributor_info.disposing_tasks().len());
            assert_eq!(0, contributor_info.disposed_tasks().len());
        }
    }

    // Check that all tasks are present.
    assert_eq!(24, tasks.len());
    for chunk_id in 0..environment.number_of_chunks() {
        for contribution_id in 1..4 {
            debug!("Checking {:?}", Task::new(chunk_id, contribution_id));
            assert!(tasks.contains(&Task::new(chunk_id, contribution_id)));
        }
    }

    // Print the current round of the ceremony.
    debug!("{}", serde_json::to_string_pretty(&coordinator.current_round()?)?);

    Ok(())
}

#[test]
#[serial]
/// Drops a contributor and removes all contributions from the contributor.
fn coordinator_drop_contributor_removes_contributions() -> anyhow::Result<()> {
    let parameters = Parameters::Custom(Settings::new(
        ContributionMode::Chunked,
        ProvingSystem::Groth16,
        CurveKind::Bls12_377,
        6,  /* power */
        16, /* batch_size */
        16, /* chunk_size */
    ));
    let environment = initialize_test_environment(&Testing::from(parameters).into());
    let number_of_chunks = environment.number_of_chunks() as usize;

    // Instantiate a coordinator.
    let mut coordinator = Coordinator::new(environment, Arc::new(Dummy))?;

    // Initialize the ceremony to round 0.
    coordinator.initialize()?;
    assert_eq!(0, coordinator.current_round_height()?);

    // Add a contributor and verifier to the queue.
    let token = String::from("test_token");
    let token2 = String::from("test_token_2");
    let (contributor1, contributor_signing_key1, seed1) = create_contributor("1");
    let contributor_1_ip = IpAddr::V4("0.0.0.1".parse().unwrap());
    let (contributor2, contributor_signing_key2, seed2) = create_contributor("2");
    let contributor_2_ip = IpAddr::V4("0.0.0.2".parse().unwrap());
    let (verifier, verifier_signing_key) = create_verifier("1");
    coordinator.add_to_queue(contributor1.clone(), Some(contributor_1_ip), token, 10)?;
    coordinator.add_to_queue(contributor2.clone(), Some(contributor_2_ip), token2, 9)?;
    assert_eq!(2, coordinator.number_of_queue_contributors());
    assert!(coordinator.is_queue_contributor(&contributor1));
    assert!(coordinator.is_queue_contributor(&contributor2));
    assert!(!coordinator.is_current_contributor(&contributor1));
    assert!(!coordinator.is_current_contributor(&contributor2));
    assert!(!coordinator.is_finished_contributor(&contributor1));
    assert!(!coordinator.is_finished_contributor(&contributor2));

    // Update the ceremony to round 1.
    coordinator.update()?;
    assert_eq!(1, coordinator.current_round_height()?);
    assert_eq!(0, coordinator.number_of_queue_contributors());
    assert!(!coordinator.is_queue_contributor(&contributor1));
    assert!(!coordinator.is_queue_contributor(&contributor2));
    assert!(coordinator.is_current_contributor(&contributor1));
    assert!(coordinator.is_current_contributor(&contributor2));
    assert!(!coordinator.is_finished_contributor(&contributor1));
    assert!(!coordinator.is_finished_contributor(&contributor2));

    // Contribute and verify up to the penultimate chunk.
    for _ in 0..(number_of_chunks - 1) {
        coordinator.contribute(&contributor1, &contributor_signing_key1, &seed1)?;
        coordinator.contribute(&contributor2, &contributor_signing_key2, &seed2)?;
        verify_task_if_available(&mut coordinator, &verifier, &verifier_signing_key)?;
        verify_task_if_available(&mut coordinator, &verifier, &verifier_signing_key)?;
    }
    assert!(!coordinator.is_queue_contributor(&contributor1));
    assert!(!coordinator.is_queue_contributor(&contributor2));
    assert!(coordinator.is_current_contributor(&contributor1));
    assert!(coordinator.is_current_contributor(&contributor2));
    assert!(!coordinator.is_finished_contributor(&contributor1));
    assert!(!coordinator.is_finished_contributor(&contributor2));

    // Drop the contributor from the current round.
    coordinator.drop_participant(&contributor1)?;
    assert!(!coordinator.is_queue_contributor(&contributor1));
    assert!(!coordinator.is_queue_contributor(&contributor2));
    assert!(!coordinator.is_current_contributor(&contributor1));
    assert!(coordinator.is_current_contributor(&contributor2));
    assert!(!coordinator.is_finished_contributor(&contributor1));
    assert!(!coordinator.is_finished_contributor(&contributor2));

    // Check that contributor 1 was dropped and coordinator state was updated.
    let contributors = coordinator.current_contributors();
    assert_eq!(2, contributors.len());
    assert_eq!(0, contributors.par_iter().filter(|(p, _)| *p == contributor1).count());
    for (contributor, contributor_info) in contributors {
        if contributor == contributor2 {
            assert_eq!(0, contributor_info.locked_chunks().len());
            assert_eq!(4, contributor_info.assigned_tasks().len());
            assert_eq!(0, contributor_info.pending_tasks().len());
            assert_eq!(4, contributor_info.completed_tasks().len());
            assert_eq!(0, contributor_info.disposing_tasks().len());
            assert_eq!(3, contributor_info.disposed_tasks().len());
        } else {
            assert_eq!(0, contributor_info.locked_chunks().len());
            assert_eq!(8, contributor_info.assigned_tasks().len());
            assert_eq!(0, contributor_info.pending_tasks().len());
            assert_eq!(0, contributor_info.completed_tasks().len());
            assert_eq!(0, contributor_info.disposing_tasks().len());
            assert_eq!(0, contributor_info.disposed_tasks().len());
        }
    }

    for chunk in coordinator.current_round()?.chunks() {
        let num_contributor1_chunk_contributions = chunk
            .get_contributions()
            .par_iter()
            .filter(|(_, contribution)| contribution.get_contributor() == &Some(contributor1.clone()))
            .count();

        assert_eq!(num_contributor1_chunk_contributions, 0);
    }

    // Print the coordinator state.
    let state = coordinator.state();
    debug!("{}", serde_json::to_string_pretty(&state)?);
    assert_eq!(1, state.current_round_height());

    // Print the current round of the ceremony.
    debug!("{}", serde_json::to_string_pretty(&coordinator.current_round()?)?);

    Ok(())
}

#[test]
#[serial]
/// Drops a contributor and clears locks for contributors/verifiers working on disposed tasks.
fn coordinator_drop_contributor_clear_locks() -> anyhow::Result<()> {
    let parameters = Parameters::Custom(Settings::new(
        ContributionMode::Chunked,
        ProvingSystem::Groth16,
        CurveKind::Bls12_377,
        6,  /* power */
        16, /* batch_size */
        16, /* chunk_size */
    ));
    let environment = initialize_test_environment(&Testing::from(parameters).into());
    let number_of_chunks = environment.number_of_chunks() as usize;

    // Instantiate a coordinator.
    let mut coordinator = Coordinator::new(environment.clone(), Arc::new(Dummy)).unwrap();

    // Initialize the ceremony to round 0.
    coordinator.initialize().unwrap();
    assert_eq!(0, coordinator.current_round_height().unwrap());

    // Add a contributor and verifier to the queue.
    let token = String::from("test_token");
    let token2 = String::from("test_token_2");
    let token3 = String::from("test_token_3");
    let (contributor1, contributor_signing_key1, seed1) = create_contributor("1");
    let contributor_1_ip = IpAddr::V4("0.0.0.1".parse().unwrap());
    let (contributor2, contributor_signing_key2, seed2) = create_contributor("2");
    let contributor_2_ip = IpAddr::V4("0.0.0.2".parse().unwrap());
    let (contributor3, contributor_signing_key3, seed3) = create_contributor("3");
    let contributor_3_ip = IpAddr::V4("0.0.0.3".parse().unwrap());
    let (verifier, verifier_signing_key) = create_verifier("1");
    coordinator
        .add_to_queue(contributor1.clone(), Some(contributor_1_ip), token, 10)
        .unwrap();
    coordinator
        .add_to_queue(contributor2.clone(), Some(contributor_2_ip), token2, 9)
        .unwrap();
    coordinator
        .add_to_queue(contributor3.clone(), Some(contributor_3_ip), token3, 8)
        .unwrap();
    assert_eq!(3, coordinator.number_of_queue_contributors());

    // Update the ceremony to round 1.
    coordinator.update().unwrap();
    assert_eq!(1, coordinator.current_round_height().unwrap());
    assert_eq!(0, coordinator.number_of_queue_contributors());

    // Contribute and verify up to 2 before the final chunk.
    for _ in 0..(number_of_chunks - 2) {
        coordinator
            .contribute(&contributor1, &contributor_signing_key1, &seed1)
            .unwrap();
        coordinator
            .contribute(&contributor2, &contributor_signing_key2, &seed2)
            .unwrap();
        coordinator
            .contribute(&contributor3, &contributor_signing_key3, &seed3)
            .unwrap();
        verify_task_if_available(&mut coordinator, &verifier, &verifier_signing_key).unwrap();
        verify_task_if_available(&mut coordinator, &verifier, &verifier_signing_key).unwrap();
        verify_task_if_available(&mut coordinator, &verifier, &verifier_signing_key).unwrap();
    }

    // Contribute up to the final chunk.
    coordinator
        .contribute(&contributor1, &contributor_signing_key1, &seed1)
        .unwrap();
    coordinator
        .contribute(&contributor2, &contributor_signing_key2, &seed2)
        .unwrap();
    coordinator
        .contribute(&contributor3, &contributor_signing_key3, &seed3)
        .unwrap();

    // Lock the next task for the verifier and contributor 1 and 3.
    let (contributor1_locked_chunk_id, _) = coordinator.try_lock(&contributor1).unwrap();

    // Print the coordinator state.
    let state = coordinator.state();
    debug!("{}", serde_json::to_string_pretty(&state).unwrap());
    assert_eq!(1, state.current_round_height());

    // Check that coordinator state includes a pending task for contributor 1 and 3.
    let contributors = coordinator.current_contributors();
    assert_eq!(3, contributors.len());
    assert_eq!(1, contributors.par_iter().filter(|(p, _)| *p == contributor2).count());
    let mut tasks: HashSet<Task> = HashSet::new();
    for (contributor, contributor_info) in contributors {
        if contributor == contributor1 {
            tasks.extend(contributor_info.assigned_tasks().iter());
            tasks.extend(contributor_info.pending_tasks().iter());
            tasks.extend(contributor_info.completed_tasks().iter());
            assert_eq!(1, contributor_info.locked_chunks().len());
            assert_eq!(0, contributor_info.assigned_tasks().len());
            assert_eq!(1, contributor_info.pending_tasks().len());
            assert_eq!(7, contributor_info.completed_tasks().len());
            assert_eq!(0, contributor_info.disposing_tasks().len());
            assert_eq!(0, contributor_info.disposed_tasks().len());
        } else if contributor == contributor3 {
            tasks.extend(contributor_info.assigned_tasks().iter());
            tasks.extend(contributor_info.pending_tasks().iter());
            tasks.extend(contributor_info.completed_tasks().iter());
            assert_eq!(0, contributor_info.locked_chunks().len());
            assert_eq!(1, contributor_info.assigned_tasks().len());
            assert_eq!(0, contributor_info.pending_tasks().len());
            assert_eq!(7, contributor_info.completed_tasks().len());
            assert_eq!(0, contributor_info.disposing_tasks().len());
            assert_eq!(0, contributor_info.disposed_tasks().len());
        } else {
            tasks.extend(contributor_info.assigned_tasks().iter());
            tasks.extend(contributor_info.completed_tasks().iter());
            assert_eq!(0, contributor_info.locked_chunks().len());
            assert_eq!(1, contributor_info.assigned_tasks().len());
            assert_eq!(0, contributor_info.pending_tasks().len());
            assert_eq!(7, contributor_info.completed_tasks().len());
            assert_eq!(0, contributor_info.disposing_tasks().len());
            assert_eq!(0, contributor_info.disposed_tasks().len());
        }
    }

    // Check that all tasks are present.
    assert_eq!(24, tasks.len());
    for chunk_id in 0..environment.number_of_chunks() {
        for contribution_id in 1..4 {
            debug!("Checking {:?}", Task::new(chunk_id, contribution_id));
            assert!(tasks.contains(&Task::new(chunk_id, contribution_id)));
        }
    }

    // Lock the next task for contributor 2.
    coordinator.try_lock(&contributor2).unwrap();

    // Drop the contributor from the current round.
    coordinator.drop_participant(&contributor2).unwrap();
    assert!(!coordinator.is_queue_contributor(&contributor1));
    assert!(!coordinator.is_queue_contributor(&contributor2));
    assert!(!coordinator.is_queue_contributor(&contributor3));
    assert!(coordinator.is_current_contributor(&contributor1));
    assert!(!coordinator.is_current_contributor(&contributor2));
    assert!(coordinator.is_current_contributor(&contributor3));
    assert!(!coordinator.is_finished_contributor(&contributor1));
    assert!(!coordinator.is_finished_contributor(&contributor2));
    assert!(!coordinator.is_finished_contributor(&contributor3));

    // Run `try_contribute` to remove disposed tasks.
    coordinator
        .try_contribute(&contributor1, contributor1_locked_chunk_id)
        .unwrap();
    // No more disposed tasks for verifiers, so this call should return Err
    let task = fetch_task_for_verifier(&coordinator).unwrap();
    let result = coordinator.try_verify(&verifier, &task);
    assert!(result.is_err());

    // Print the coordinator state.
    let state = coordinator.state();
    debug!("{}", serde_json::to_string_pretty(&state).unwrap());
    assert_eq!(1, state.current_round_height());

    // Check that contributor 2 was dropped and coordinator state was updated.
    let contributors = coordinator.current_contributors();
    assert_eq!(3, contributors.len());
    assert_eq!(0, contributors.par_iter().filter(|(p, _)| *p == contributor2).count());
    let mut tasks: HashSet<Task> = HashSet::new();
    for (contributor, contributor_info) in contributors {
        if contributor == contributor1 {
            tasks.extend(contributor_info.assigned_tasks().iter());
            tasks.extend(contributor_info.completed_tasks().iter());
            assert_eq!(0, contributor_info.locked_chunks().len());
            assert_eq!(6, contributor_info.assigned_tasks().len());
            assert_eq!(0, contributor_info.pending_tasks().len());
            assert_eq!(2, contributor_info.completed_tasks().len());
            assert_eq!(0, contributor_info.disposing_tasks().len());
            assert_eq!(6, contributor_info.disposed_tasks().len());
        } else if contributor == contributor3 {
            tasks.extend(contributor_info.assigned_tasks().iter());
            tasks.extend(contributor_info.completed_tasks().iter());
            assert_eq!(0, contributor_info.locked_chunks().len());
            assert_eq!(2, contributor_info.assigned_tasks().len());
            assert_eq!(0, contributor_info.pending_tasks().len());
            assert_eq!(6, contributor_info.completed_tasks().len());
            assert_eq!(0, contributor_info.disposing_tasks().len());
            assert_eq!(1, contributor_info.disposed_tasks().len());
        } else {
            tasks.extend(contributor_info.assigned_tasks().iter());
            assert_eq!(0, contributor_info.locked_chunks().len());
            assert_eq!(8, contributor_info.assigned_tasks().len());
            assert_eq!(0, contributor_info.pending_tasks().len());
            assert_eq!(0, contributor_info.completed_tasks().len());
            assert_eq!(0, contributor_info.disposing_tasks().len());
            assert_eq!(0, contributor_info.disposed_tasks().len());
        }
    }

    // Check that all tasks are present.
    assert_eq!(24, tasks.len());
    for chunk_id in 0..environment.number_of_chunks() {
        for contribution_id in 1..4 {
            debug!("Checking {:?}", Task::new(chunk_id, contribution_id));
            assert!(tasks.contains(&Task::new(chunk_id, contribution_id)));
        }
    }

    Ok(())
}

/// Drops a contributor and removes all subsequent contributions.
#[test]
#[serial]
fn coordinator_drop_contributor_removes_subsequent_contributions() -> anyhow::Result<()> {
    let parameters = Parameters::Custom(Settings {
        contribution_mode: ContributionMode::Chunked,
        proving_system: ProvingSystem::Groth16,
        curve: CurveKind::Bls12_377,
        power: 1,
        batch_size: 2,
        chunk_size: 2,
    });
    let (replacement_contributor, ..) = create_contributor("replacement-1");
    let testing = Testing::from(parameters).coordinator_contributors(&[replacement_contributor.clone()]);
    let environment = initialize_test_environment(&testing.into());
    let number_of_chunks = environment.number_of_chunks() as usize;

    // Instantiate a coordinator.
    let mut coordinator = Coordinator::new(environment, Arc::new(Dummy))?;

    // Initialize the ceremony to round 0.
    coordinator.initialize()?;
    assert_eq!(0, coordinator.current_round_height()?);

    // Add a contributor and verifier to the queue.
    let token = String::from("test_token");
    let token2 = String::from("test_token_2");
    let (contributor1, contributor_signing_key1, seed1) = create_contributor("1");
    let contributor_1_ip = IpAddr::V4("0.0.0.1".parse().unwrap());
    let (contributor2, contributor_signing_key2, seed2) = create_contributor("2");
    let contributor_2_ip = IpAddr::V4("0.0.0.2".parse().unwrap());
    let (verifier, verifier_signing_key) = create_verifier("1");
    coordinator.add_to_queue(contributor1.clone(), Some(contributor_1_ip), token, 10)?;
    coordinator.add_to_queue(contributor2.clone(), Some(contributor_2_ip), token2, 9)?;

    // Update the ceremony to round 1.
    coordinator.update()?;

    // Make all contributions
    for _ in 0..number_of_chunks {
        coordinator.contribute(&contributor1, &contributor_signing_key1, &seed1)?;
        coordinator.contribute(&contributor2, &contributor_signing_key2, &seed2)?;
        verify_task_if_available(&mut coordinator, &verifier, &verifier_signing_key)?;
        verify_task_if_available(&mut coordinator, &verifier, &verifier_signing_key)?;
    }

    // Check that all contributors completed expected tasks
    for (contributor, contributor_info) in coordinator.current_contributors() {
        let expected_tasks = if contributor == contributor1 {
            make_tasks(&[(0, 1), (1, 2)])
        } else if contributor == contributor2 {
            make_tasks(&[(1, 1), (0, 2)])
        } else {
            panic!("Unexpected contributor: {:?}", contributor);
        };
        assert!(contributor_info.assigned_tasks().is_empty());
        assert_eq!(contributor_info.completed_tasks(), &expected_tasks);
    }

    // Drop one contributor
    coordinator.drop_participant(&contributor1)?;

    // Check that the tasks were reassigned properly
    for (contributor, contributor_info) in coordinator.current_contributors() {
        if contributor == contributor2 {
            assert_eq!(contributor_info.completed_tasks(), &make_tasks(&[(1, 1)]));
            assert_eq!(contributor_info.assigned_tasks(), &make_tasks(&[(0, 2)]));
            assert_eq!(contributor_info.disposed_tasks(), &make_tasks(&[(0, 2)]));
        } else if contributor == replacement_contributor {
            assert!(contributor_info.completed_tasks().is_empty());
            assert_eq!(contributor_info.assigned_tasks(), &make_tasks(&[(0, 1), (1, 2)]));
            assert!(contributor_info.disposed_tasks().is_empty());
        } else {
            panic!("Unexpected contributor: {:?}", contributor);
        }
    }

    Ok(())
}

/// Drops a contributor and release the locks
///
/// The key part of this test is that we lock a chunk
/// by a contributor and then immediately drop the contributor
/// without contributing
#[test]
#[serial]
fn coordinator_drop_contributor_and_release_locks() {
    // Unwraps are used to find out the exact line which produces the error
    // When the test returns Result with an Err, the line is unknown

    let parameters = Parameters::Custom(Settings {
        contribution_mode: ContributionMode::Chunked,
        proving_system: ProvingSystem::Groth16,
        curve: CurveKind::Bls12_377,
        power: 1,
        batch_size: 2,
        chunk_size: 2,
    });
    let replacement_contributor = create_contributor_test_details("replacement-1");
    let testing = Testing::from(parameters).coordinator_contributors(&[replacement_contributor.participant.clone()]);
    let environment = initialize_test_environment(&testing.into());
    let number_of_chunks = environment.number_of_chunks() as usize;

    // Instantiate a coordinator.
    let mut coordinator = Coordinator::new(environment, Arc::new(Dummy)).unwrap();

    // Initialize the ceremony to round 0.
    coordinator.initialize().unwrap();
    assert_eq!(0, coordinator.current_round_height().unwrap());

    // Add a contributor and verifier to the queue.
    let token = String::from("test_token");
    let token2 = String::from("test_token_2");
    let contributor_1 = create_contributor_test_details("1");
    let contributor_1_ip = IpAddr::V4("0.0.0.1".parse().unwrap());
    let contributor_2 = create_contributor_test_details("2");
    let contributor_2_ip = IpAddr::V4("0.0.0.2".parse().unwrap());
    let verifier_1 = create_verifier_test_details("1");
    coordinator
        .add_to_queue(contributor_1.participant.clone(), Some(contributor_1_ip), token, 10)
        .unwrap();
    coordinator
        .add_to_queue(contributor_2.participant.clone(), Some(contributor_2_ip), token2, 9)
        .unwrap();

    // Update the ceremony to round 1.
    coordinator.update().unwrap();

    // Lock a chunk by a contributor
    coordinator.try_lock(&contributor_1.participant).unwrap();

    // Drop the contributor which have locked the chunk
    coordinator.drop_participant(&contributor_1.participant).unwrap();

    // Contribute to the round 1
    for _ in 0..number_of_chunks {
        replacement_contributor.contribute_to(&mut coordinator).unwrap();
        contributor_2.contribute_to(&mut coordinator).unwrap();
        verifier_1.verify_if_available(&mut coordinator).unwrap();
        verifier_1.verify_if_available(&mut coordinator).unwrap();
    }

    // Add some more participants to proceed to the next round
    let token3 = String::from("test_token_3");
    let token4 = String::from("test_token_4");
    let test_contributor_3 = create_contributor_test_details("3");
    let contributor_3_ip = IpAddr::V4("0.0.0.3".parse().unwrap());
    let test_contributor_4 = create_contributor_test_details("4");
    let contributor_4_ip = IpAddr::V4("0.0.0.4".parse().unwrap());
    coordinator
        .add_to_queue(
            test_contributor_3.participant.clone(),
            Some(contributor_3_ip),
            token3,
            10,
        )
        .unwrap();
    coordinator
        .add_to_queue(
            test_contributor_4.participant.clone(),
            Some(contributor_4_ip),
            token4,
            10,
        )
        .unwrap();

    // Update the ceremony to round 2.
    coordinator.update().unwrap();
    assert_eq!(2, coordinator.current_round_height().unwrap());
    assert_eq!(0, coordinator.number_of_queue_contributors());
}

/// Drops a few contributors and see what happens
///
/// The goal of this test is to reproduce a specific error
/// which happens in the integration tests at the moment
#[test]
#[serial]
#[ignore]
fn coordinator_drop_several_contributors() {
    let parameters = Parameters::Custom(Settings {
        contribution_mode: ContributionMode::Chunked,
        proving_system: ProvingSystem::Groth16,
        curve: CurveKind::Bls12_377,
        power: 2,
        batch_size: 2,
        chunk_size: 2,
    });
    let replacement_contributor_1 = create_contributor_test_details("replacement-1");
    let replacement_contributor_2 = create_contributor_test_details("replacement-2");
    let testing = Testing::from(parameters).coordinator_contributors(&[
        replacement_contributor_1.participant.clone(),
        replacement_contributor_2.participant.clone(),
    ]);
    let environment = initialize_test_environment(&testing.into());

    // Instantiate a coordinator.
    let mut coordinator = Coordinator::new(environment, Arc::new(Dummy)).unwrap();

    // Initialize the ceremony to round 0.
    coordinator.initialize().unwrap();
    assert_eq!(0, coordinator.current_round_height().unwrap());

    // Add some contributors and one verifier to the queue.
    let token = String::from("test_token");
    let token2 = String::from("test_token_2");
    let token3 = String::from("test_token_3");
    let contributor_1 = create_contributor_test_details("1");
    let contributor_1_ip = IpAddr::V4("0.0.0.1".parse().unwrap());
    let contributor_2 = create_contributor_test_details("2");
    let contributor_2_ip = IpAddr::V4("0.0.0.2".parse().unwrap());
    let contributor_3 = create_contributor_test_details("3");
    let contributor_3_ip = IpAddr::V4("0.0.0.3".parse().unwrap());
    let verifier_1 = create_verifier_test_details("1");
    coordinator
        .add_to_queue(contributor_1.participant.clone(), Some(contributor_1_ip), token, 10)
        .unwrap();
    coordinator
        .add_to_queue(contributor_2.participant.clone(), Some(contributor_2_ip), token2, 10)
        .unwrap();
    coordinator
        .add_to_queue(contributor_3.participant.clone(), Some(contributor_3_ip), token3, 10)
        .unwrap();

    // Update the ceremony to round 1.
    coordinator.update().unwrap();
    assert_eq!(1, coordinator.current_round_height().unwrap());

    // Make some contributions
    let k = 3;
    for _ in 0..k {
        contributor_1.contribute_to(&mut coordinator).unwrap();
        contributor_2.contribute_to(&mut coordinator).unwrap();
        contributor_3.contribute_to(&mut coordinator).unwrap();

        verifier_1.verify_if_available(&mut coordinator).unwrap();
        verifier_1.verify_if_available(&mut coordinator).unwrap();
        verifier_1.verify_if_available(&mut coordinator).unwrap();
    }

    {
        let round = coordinator.current_round().unwrap();
        let storage = coordinator.storage();
        check_round_matches_storage_files(storage, &round);
    }

    let _locators = coordinator.drop_participant(&contributor_1.participant).unwrap();
    let _locators = coordinator.drop_participant(&contributor_2.participant).unwrap();

    coordinator.update().unwrap();

    {
        let round = coordinator.current_round().unwrap();
        let storage = coordinator.storage();
        check_round_matches_storage_files(storage, &round);
    }

    fn contribute_verify_until_no_tasks(
        contributor: &ContributorTestDetails,
        verifier: &VerifierTestDetails,
        coordinator: &mut Coordinator,
    ) -> anyhow::Result<bool> {
        match contributor.contribute_to(coordinator) {
            Err(CoordinatorError::ParticipantHasNoRemainingTasks) => Ok(true),
            Err(CoordinatorError::PreviousContributionMissing { current_task: _ }) => Ok(false),
            Ok(_) => {
                verifier.verify_if_available(coordinator)?;
                Ok(false)
            }
            Err(error) => return Err(error.into()),
        }
    }

    // Contribute to the round 1
    let mut all_complete = false;
    let mut count = 0;
    while !all_complete {
        let c3_complete = contribute_verify_until_no_tasks(&contributor_3, &verifier_1, &mut coordinator).unwrap();
        let rc1_complete =
            contribute_verify_until_no_tasks(&replacement_contributor_1, &verifier_1, &mut coordinator).unwrap();
        let rc2_complete =
            contribute_verify_until_no_tasks(&replacement_contributor_2, &verifier_1, &mut coordinator).unwrap();

        all_complete = c3_complete && rc1_complete && rc2_complete;
        count += 1;

        if count > 50 {
            panic!("There have been too many attempts to make contributions")
        }
    }

    // Add some more participants to proceed to the next round
    let token3 = String::from("test_token_3");
    let token4 = String::from("test_token_4");
    let test_contributor_3 = create_contributor_test_details("3");
    let contributor_3_ip = IpAddr::V4("0.0.0.3".parse().unwrap());
    let test_contributor_4 = create_contributor_test_details("4");
    let contributor_4_ip = IpAddr::V4("0.0.0.4".parse().unwrap());
    coordinator
        .add_to_queue(
            test_contributor_3.participant.clone(),
            Some(contributor_3_ip),
            token3,
            10,
        )
        .unwrap();
    coordinator
        .add_to_queue(
            test_contributor_4.participant.clone(),
            Some(contributor_4_ip),
            token4,
            10,
        )
        .unwrap();

    // Update the ceremony to round 2.
    coordinator.update().unwrap();

    assert_eq!(2, coordinator.current_round_height().unwrap());
    assert_eq!(0, coordinator.number_of_queue_contributors());
}

fn check_round_matches_storage_files(storage: &Disk, round: &Round) {
    debug!("Checking round {}", round.round_height());
    for chunk in round.chunks() {
        debug!("Checking chunk {}", chunk.chunk_id());
        let initial_challenge_location = if let Some(current_contributed_location) =
            chunk.get_contribution(0).unwrap().get_verified_location().as_ref()
        {
            current_contributed_location
        } else {
            tracing::warn!(
                "No initial challenge found for round {} chunk {}",
                round.round_height(),
                chunk.chunk_id()
            );
            continue;
        };
        let path = initial_challenge_location.as_path();
        let chunk_dir = path.parent().unwrap();

        let n_files = fs::read_dir(&chunk_dir).unwrap().count();

        let contributions_complete = chunk.only_contributions_complete(round.expected_number_of_contributions());

        let mut expected_n_files = 0;

        let contributions = chunk.get_contributions();
        let last_index = contributions.len() - 1;
        for (index, (contribution_id, contribution)) in contributions.iter().enumerate() {
            if let Some(path) = contribution.get_contributed_location() {
                let locator = storage.to_locator(&path).unwrap();
                assert!(storage.exists(&locator));
                expected_n_files += 1;
            }

            if let Some(path) = contribution.get_contributed_signature_location() {
                let locator = storage.to_locator(&path).unwrap();
                assert!(storage.exists(&locator));
                expected_n_files += 1;
            }

            if let Some(path) = contribution.get_verified_location() {
                let locator = storage.to_locator(&path).unwrap();
                assert!(storage.exists(&locator));

                // the final contribution's verification goes in the next round's directory
                if (!contributions_complete) || last_index != index {
                    expected_n_files += 1;
                }
            }

            if let Some(path) = contribution.get_verified_signature_location() {
                // TODO: for some reason contribution 0 for round 0
                // and round 1 is missing a signature file, this could
                // be a bug.
                if *contribution_id != 0 {
                    let locator = storage.to_locator(&path).unwrap();
                    assert!(storage.exists(&locator));

                    // the final contribution's verification goes in the next round's directory
                    if (!contributions_complete) || last_index != index {
                        expected_n_files += 1;
                    }
                }
            }
        }

        if expected_n_files != n_files {
            panic!(
                "Error: For round {} chunk {}, expected number of files according to round state ({}) \
                does not match the actual number of files ({}) in the chunk \
                directory {:?}",
                round.round_height(),
                chunk.chunk_id(),
                expected_n_files,
                n_files,
                chunk_dir
            )
        }
    }
}

/// Drops a contributor and updates verifier tasks
///
/// Make one contribution and verify it, then drop the
/// contributor. The tasks of a verifier should be updated
/// properly
#[test]
#[serial]
fn coordinator_drop_contributor_and_update_verifier_tasks() {
    // Unwraps are used to find out the exact line which produces the error
    // When the test returns Result with an Err, the line is unknown

    let parameters = Parameters::Custom(Settings {
        contribution_mode: ContributionMode::Chunked,
        proving_system: ProvingSystem::Groth16,
        curve: CurveKind::Bls12_377,
        power: 1,
        batch_size: 2,
        chunk_size: 2,
    });
    let replacement_contributor = create_contributor_test_details("replacement-1");
    let testing = Testing::from(parameters).coordinator_contributors(&[replacement_contributor.participant.clone()]);
    let environment = initialize_test_environment(&testing.into());
    let number_of_chunks = environment.number_of_chunks() as usize;

    // Instantiate a coordinator.
    let mut coordinator = Coordinator::new(environment, Arc::new(Dummy)).unwrap();

    // Initialize the ceremony to round 0.
    coordinator.initialize().unwrap();
    assert_eq!(0, coordinator.current_round_height().unwrap());

    // Add a contributor and verifier to the queue.
    let token = String::from("test_token");
    let token2 = String::from("test_token_2");
    let contributor_1 = create_contributor_test_details("1");
    let contributor_1_ip = IpAddr::V4("0.0.0.1".parse().unwrap());
    let contributor_2 = create_contributor_test_details("2");
    let contributor_2_ip = IpAddr::V4("0.0.0.2".parse().unwrap());
    let verifier_1 = create_verifier_test_details("1");
    coordinator
        .add_to_queue(contributor_1.participant.clone(), Some(contributor_1_ip), token, 10)
        .unwrap();
    coordinator
        .add_to_queue(contributor_2.participant.clone(), Some(contributor_2_ip), token2, 9)
        .unwrap();

    // Update the ceremony to round 1.
    coordinator.update().unwrap();

    contributor_1.contribute_to(&mut coordinator).unwrap();

    verifier_1.verify_if_available(&mut coordinator).unwrap();

    coordinator.drop_participant(&contributor_1.participant).unwrap();

    // Contribute to the round 1
    for _ in 0..number_of_chunks {
        replacement_contributor.contribute_to(&mut coordinator).unwrap();
        contributor_2.contribute_to(&mut coordinator).unwrap();
        verifier_1.verify_if_available(&mut coordinator).unwrap();
        verifier_1.verify_if_available(&mut coordinator).unwrap();
    }

    // Add some more participants to proceed to the next round
    let token3 = String::from("test_token_3");
    let token4 = String::from("test_token_4");
    let test_contributor_3 = create_contributor_test_details("3");
    let contributor_3_ip = IpAddr::V4("0.0.0.3".parse().unwrap());
    let test_contributor_4 = create_contributor_test_details("4");
    let contributor_4_ip = IpAddr::V4("0.0.0.4".parse().unwrap());
    coordinator
        .add_to_queue(
            test_contributor_3.participant.clone(),
            Some(contributor_3_ip),
            token3,
            10,
        )
        .unwrap();
    coordinator
        .add_to_queue(
            test_contributor_4.participant.clone(),
            Some(contributor_4_ip),
            token4,
            10,
        )
        .unwrap();

    // Update the ceremony to round 2.
    coordinator.update().unwrap();
    assert_eq!(2, coordinator.current_round_height().unwrap());
    assert_eq!(0, coordinator.number_of_queue_contributors());
}

#[test]
#[serial]
/// Drops a multiple contributors an replaces with the coordinator contributor.
fn coordinator_drop_multiple_contributors() -> anyhow::Result<()> {
    let parameters = Parameters::Custom(Settings::new(
        ContributionMode::Chunked,
        ProvingSystem::Groth16,
        CurveKind::Bls12_377,
        6,  /* power */
        16, /* batch_size */
        16, /* chunk_size */
    ));
    let testing = Testing::from(parameters).coordinator_contributors(&[
        Participant::new_contributor("testing-coordinator-contributor-1"),
        Participant::new_contributor("testing-coordinator-contributor-2"),
        Participant::new_contributor("testing-coordinator-contributor-3"),
    ]);
    let environment = initialize_test_environment(&testing.into());

    let number_of_chunks = environment.number_of_chunks() as usize;

    // Instantiate a coordinator.
    let mut coordinator = Coordinator::new(environment.clone(), Arc::new(Dummy))?;

    // Initialize the ceremony to round 0.
    coordinator.initialize()?;
    assert_eq!(0, coordinator.current_round_height()?);

    // Add a contributor and verifier to the queue.
    let token = String::from("test_token");
    let token2 = String::from("test_token_2");
    let token3 = String::from("test_token_3");
    let (contributor1, contributor_signing_key1, seed1) = create_contributor("1");
    let contributor_1_ip = IpAddr::V4("0.0.0.1".parse().unwrap());
    let (contributor2, contributor_signing_key2, seed2) = create_contributor("2");
    let contributor_2_ip = IpAddr::V4("0.0.0.2".parse().unwrap());
    let (contributor3, contributor_signing_key3, seed3) = create_contributor("3");
    let contributor_3_ip = IpAddr::V4("0.0.0.3".parse().unwrap());
    let (verifier, verifier_signing_key) = create_verifier("1");
    coordinator.add_to_queue(contributor1.clone(), Some(contributor_1_ip), token, 10)?;
    coordinator.add_to_queue(contributor2.clone(), Some(contributor_2_ip), token2, 9)?;
    coordinator.add_to_queue(contributor3.clone(), Some(contributor_3_ip), token3, 8)?;
    assert_eq!(3, coordinator.number_of_queue_contributors());

    // Update the ceremony to round 1.
    coordinator.update()?;
    assert_eq!(1, coordinator.current_round_height()?);
    assert_eq!(0, coordinator.number_of_queue_contributors());

    // Contribute and verify up to 2 before the final chunk.
    for _ in 0..(number_of_chunks - 2) {
        coordinator.contribute(&contributor1, &contributor_signing_key1, &seed1)?;
        coordinator.contribute(&contributor2, &contributor_signing_key2, &seed2)?;
        coordinator.contribute(&contributor3, &contributor_signing_key3, &seed3)?;
        verify_task_if_available(&mut coordinator, &verifier, &verifier_signing_key)?;
        verify_task_if_available(&mut coordinator, &verifier, &verifier_signing_key)?;
        verify_task_if_available(&mut coordinator, &verifier, &verifier_signing_key)?;
    }

    // Aggregate all of the tasks for each of the contributors into a HashSet.
    let contributors = coordinator.current_contributors();
    assert_eq!(3, contributors.len());
    let mut tasks: HashSet<Task> = HashSet::new();
    for (_, contributor_info) in contributors {
        tasks.extend(contributor_info.assigned_tasks().iter());
        tasks.extend(contributor_info.completed_tasks().iter());
        assert_eq!(0, contributor_info.locked_chunks().len());
        assert_eq!(2, contributor_info.assigned_tasks().len());
        assert_eq!(0, contributor_info.pending_tasks().len());
        assert_eq!(6, contributor_info.completed_tasks().len());
        assert_eq!(0, contributor_info.disposing_tasks().len());
        assert_eq!(0, contributor_info.disposed_tasks().len());
    }

    // Check that all tasks are present.
    assert_eq!(24, tasks.len());
    for chunk_id in 0..environment.number_of_chunks() {
        for contribution_id in 1..4 {
            debug!("Checking {:?}", Task::new(chunk_id, contribution_id));
            assert!(tasks.contains(&Task::new(chunk_id, contribution_id)));
        }
    }

    // Lock the next tasks for contributor 1, 2, and 3.
    coordinator.try_lock(&contributor1)?;
    coordinator.try_lock(&contributor2)?;
    coordinator.try_lock(&contributor3)?;

    // Drop the contributor 1 from the current round.
    coordinator.drop_participant(&contributor1)?;
    // Number of files affected by the drop.
    assert!(!coordinator.is_queue_contributor(&contributor1));
    assert!(!coordinator.is_queue_contributor(&contributor2));
    assert!(!coordinator.is_queue_contributor(&contributor3));
    assert!(!coordinator.is_current_contributor(&contributor1));
    assert!(coordinator.is_current_contributor(&contributor2));
    assert!(coordinator.is_current_contributor(&contributor3));
    assert!(!coordinator.is_finished_contributor(&contributor1));
    assert!(!coordinator.is_finished_contributor(&contributor2));
    assert!(!coordinator.is_finished_contributor(&contributor3));

    // Drop the contributor 2 from the current round.
    coordinator.drop_participant(&contributor2)?;
    assert!(!coordinator.is_queue_contributor(&contributor1));
    assert!(!coordinator.is_queue_contributor(&contributor2));
    assert!(!coordinator.is_queue_contributor(&contributor3));
    assert!(!coordinator.is_current_contributor(&contributor1));
    assert!(!coordinator.is_current_contributor(&contributor2));
    assert!(coordinator.is_current_contributor(&contributor3));
    assert!(!coordinator.is_finished_contributor(&contributor1));
    assert!(!coordinator.is_finished_contributor(&contributor2));
    assert!(!coordinator.is_finished_contributor(&contributor3));

    // Drop the contributor 3 from the current round.
    coordinator.drop_participant(&contributor3)?;
    assert!(!coordinator.is_queue_contributor(&contributor1));
    assert!(!coordinator.is_queue_contributor(&contributor2));
    assert!(!coordinator.is_queue_contributor(&contributor3));
    assert!(!coordinator.is_current_contributor(&contributor1));
    assert!(!coordinator.is_current_contributor(&contributor2));
    assert!(!coordinator.is_current_contributor(&contributor3));
    assert!(!coordinator.is_finished_contributor(&contributor1));
    assert!(!coordinator.is_finished_contributor(&contributor2));
    assert!(!coordinator.is_finished_contributor(&contributor3));

    // Print the coordinator state.
    let state = coordinator.state();
    debug!("{}", serde_json::to_string_pretty(&state)?);
    assert_eq!(1, state.current_round_height());

    let contributors = coordinator.current_contributors();
    assert_eq!(3, contributors.len());
    assert_eq!(0, contributors.par_iter().filter(|(p, _)| *p == contributor1).count());
    assert_eq!(0, contributors.par_iter().filter(|(p, _)| *p == contributor2).count());
    assert_eq!(0, contributors.par_iter().filter(|(p, _)| *p == contributor3).count());

    // Aggregate all of the tasks for each of the contributors into a HashSet.
    let mut tasks: HashSet<Task> = HashSet::new();
    for (_, contributor_info) in contributors {
        tasks.extend(contributor_info.assigned_tasks().iter());
        assert_eq!(0, contributor_info.locked_chunks().len());
        assert_eq!(8, contributor_info.assigned_tasks().len());
        assert_eq!(0, contributor_info.pending_tasks().len());
        assert_eq!(0, contributor_info.completed_tasks().len());
        assert_eq!(0, contributor_info.disposing_tasks().len());
        assert_eq!(0, contributor_info.disposed_tasks().len());
    }

    // Check that all tasks are present.
    assert_eq!(24, tasks.len());
    for chunk_id in 0..environment.number_of_chunks() {
        for contribution_id in 1..4 {
            debug!("Checking {:?}", Task::new(chunk_id, contribution_id));
            assert!(tasks.contains(&Task::new(chunk_id, contribution_id)));
        }
    }

    Ok(())
}

#[test]
#[serial]
fn try_lock_blocked() -> anyhow::Result<()> {
    let parameters = Parameters::Custom(Settings::new(
        ContributionMode::Chunked,
        ProvingSystem::Groth16,
        CurveKind::Bls12_377,
        7,  /* power */
        32, /* batch_size */
        32, /* chunk_size */
    ));
    let environment = initialize_test_environment(&Testing::from(parameters).into());
    let number_of_chunks = environment.number_of_chunks() as usize;

    // Instantiate a coordinator.
    let mut coordinator = Coordinator::new(environment, Arc::new(Dummy))?;

    // Initialize the ceremony to round 0.
    coordinator.initialize()?;
    assert_eq!(0, coordinator.current_round_height()?);

    // Meanwhile, add 2 contributors and 1 verifier to the queue.
    let token = String::from("test_token");
    let token2 = String::from("test_token_2");
    let (contributor1, contributor_signing_key1, seed1) = create_contributor("1");
    let contributor_1_ip = IpAddr::V4("0.0.0.1".parse().unwrap());
    let (contributor2, contributor_signing_key2, seed2) = create_contributor("2");
    let contributor_2_ip = IpAddr::V4("0.0.0.2".parse().unwrap());
    let (verifier, verifier_signing_key) = create_verifier("1");
    coordinator.add_to_queue(contributor1.clone(), Some(contributor_1_ip), token, 10)?;
    coordinator.add_to_queue(contributor2.clone(), Some(contributor_2_ip), token2, 10)?;
    assert_eq!(2, coordinator.number_of_queue_contributors());

    // Advance the ceremony from round 0 to round 1.
    coordinator.update()?;
    assert_eq!(1, coordinator.current_round_height()?);
    assert_eq!(0, coordinator.number_of_queue_contributors());

    // Fetch the bucket size.
    fn bucket_size(number_of_chunks: u64, number_of_contributors: u64) -> u64 {
        let number_of_buckets = number_of_contributors;
        let bucket_size = number_of_chunks / number_of_buckets;
        bucket_size
    }

    /*
     * |     BUCKET 0     |     BUCKET 1    |
     * |   0, 1, ...  m   |  m + 1, ... n   | <- Chunk IDs
     * |  ------------->  |  ------------>  |
     * |                  |  locked         | <- Contributor 2
     * |   done ... done  |  try_lock       | <- Contributor 1
     * |  ------------->  |  ------------>  |
     */

    // Lock first chunk for contributor 2.
    let (_, locked_locators) = coordinator.try_lock(&contributor2)?;
    let response_locator = locked_locators.next_contribution();

    // Run contributions for the first bucket as contributor 1.
    let bucket_size = bucket_size(number_of_chunks as u64, 2);
    for _ in 0..bucket_size {
        coordinator.contribute(&contributor1, &contributor_signing_key1, &seed1)?;
    }

    // Now try to lock the next chunk as contributor 1.
    //
    // This operation should be blocked by contributor 2,
    // who still holds the lock on this chunk.
    let result = coordinator.try_lock(&contributor1);
    assert!(result.is_err());

    // Run contribution on the locked chunk as contributor 2.
    {
        let round_height = response_locator.round_height();
        let chunk_id = response_locator.chunk_id();
        let contribution_id = response_locator.contribution_id();

        coordinator.run_computation(
            round_height,
            chunk_id,
            contribution_id,
            &contributor2,
            &contributor_signing_key2,
            &seed2,
        )?;
        coordinator.try_contribute(&contributor2, chunk_id)?;
    }

    // Now try to lock the next chunk as contributor 1 again.
    //
    // This operation should be blocked by the verifier,
    // who needs to verify this chunk in order for contributor 1 to acquire the lock.
    let result = coordinator.try_lock(&contributor1);
    match result {
        Err(CoordinatorError::ContributionMissingVerification) => {}
        _ => panic!("Unexpected result: {:#?}", result),
    }

    // Clear all pending verifications, so the locked chunk is released as well.
    loop {
        let pending_tasks = coordinator.get_pending_verifications();
        if let Some(task) = pending_tasks.keys().next().cloned() {
            coordinator.verify(&verifier, &verifier_signing_key, &task)?;
        } else {
            break;
        }
    }

    // Now try to lock the next chunk as contributor 1 again.
    //
    // This operation should no longer be blocked by contributor 2 or verifier,
    // who has released the lock on this chunk.
    let result = coordinator.try_lock(&contributor1);
    assert!(result.is_ok());

    Ok(())
}

#[test]
#[serial]
fn drop_all_contributors_and_complete_round() -> anyhow::Result<()> {
    let parameters = Parameters::Custom(Settings {
        contribution_mode: ContributionMode::Chunked,
        proving_system: ProvingSystem::Groth16,
        curve: CurveKind::Bls12_377,
        power: 6,
        batch_size: 16,
        chunk_size: 16,
    });

    // Create replacement contributors
    let replacement_contributor_1 = create_contributor_test_details("replacement-1");
    let replacement_contributor_2 = create_contributor_test_details("replacement-2");

    let testing = Testing::from(parameters).coordinator_contributors(&[
        replacement_contributor_1.participant.clone(),
        replacement_contributor_2.participant.clone(),
    ]);
    let environment = initialize_test_environment(&testing.into());

    let number_of_chunks = environment.number_of_chunks() as usize;

    // Instantiate a coordinator.
    let mut coordinator = Coordinator::new(environment.clone(), Arc::new(Dummy))?;

    // Initialize the ceremony to round 0.
    coordinator.initialize()?;
    assert_eq!(0, coordinator.current_round_height()?);

    // Add a contributor and verifier to the queue.
    let token = String::from("test_token");
    let token2 = String::from("test_token_2");
    let test_contributor_1 = create_contributor_test_details("1");
    let contributor_1_ip = IpAddr::V4("0.0.0.1".parse().unwrap());
    let test_contributor_2 = create_contributor_test_details("2");
    let contributor_2_ip = IpAddr::V4("0.0.0.2".parse().unwrap());
    let (verifier, verifier_signing_key) = create_verifier("1");
    coordinator.add_to_queue(
        test_contributor_1.participant.clone(),
        Some(contributor_1_ip),
        token,
        10,
    )?;
    coordinator.add_to_queue(
        test_contributor_2.participant.clone(),
        Some(contributor_2_ip),
        token2,
        9,
    )?;

    // Update the ceremony to round 1.
    coordinator.update()?;
    assert_eq!(1, coordinator.current_round_height()?);

    coordinator.drop_participant(&test_contributor_1.participant)?;
    coordinator.drop_participant(&test_contributor_2.participant)?;

    assert_eq!(false, coordinator.is_queue_contributor(&test_contributor_1.participant));
    assert_eq!(false, coordinator.is_queue_contributor(&test_contributor_2.participant));
    assert_eq!(
        false,
        coordinator.is_current_contributor(&test_contributor_1.participant),
    );
    assert_eq!(
        false,
        coordinator.is_current_contributor(&test_contributor_2.participant),
    );
    assert_eq!(
        true,
        coordinator.is_current_contributor(&replacement_contributor_1.participant),
    );
    assert_eq!(
        true,
        coordinator.is_current_contributor(&replacement_contributor_2.participant),
    );

    // Contribute to the round 1
    for _ in 0..number_of_chunks {
        replacement_contributor_1.contribute_to(&mut coordinator)?;
        replacement_contributor_2.contribute_to(&mut coordinator)?;
        verify_task_if_available(&mut coordinator, &verifier, &verifier_signing_key)?;
        verify_task_if_available(&mut coordinator, &verifier, &verifier_signing_key)?;
    }

    // Add some more participants to proceed to the next round
    let token3 = String::from("test_token_3");
    let token4 = String::from("test_token_4");
    let test_contributor_3 = create_contributor_test_details("3");
    let contributor_3_ip = IpAddr::V4("0.0.0.3".parse().unwrap());
    let test_contributor_4 = create_contributor_test_details("4");
    let contributor_4_ip = IpAddr::V4("0.0.0.4".parse().unwrap());
    coordinator.add_to_queue(
        test_contributor_3.participant.clone(),
        Some(contributor_3_ip),
        token3,
        10,
    )?;
    coordinator.add_to_queue(
        test_contributor_4.participant.clone(),
        Some(contributor_4_ip),
        token4,
        10,
    )?;

    // Update the ceremony to round 2.
    coordinator.update()?;
    assert_eq!(2, coordinator.current_round_height()?, "Should proceed to the round 2");
    assert_eq!(0, coordinator.number_of_queue_contributors());

    Ok(())
}

#[test]
#[serial]
fn drop_contributor_and_reassign_tasks() -> anyhow::Result<()> {
    let parameters = Parameters::Custom(Settings::new(
        ContributionMode::Chunked,
        ProvingSystem::Groth16,
        CurveKind::Bls12_377,
        6,  /* power */
        16, /* batch_size */
        16, /* chunk_size */
    ));
    let environment = initialize_test_environment(&Testing::from(parameters).into());
    let number_of_chunks = environment.number_of_chunks() as usize;

    // Instantiate a coordinator.
    let mut coordinator = Coordinator::new(environment, Arc::new(Dummy))?;

    // Initialize the ceremony to round 0.
    coordinator.initialize()?;
    assert_eq!(0, coordinator.current_round_height()?);

    // Add a contributor and verifier to the queue.
    let token = String::from("test_token");
    let token2 = String::from("test_token_2");
    let (contributor1, contributor_signing_key1, seed1) = create_contributor("1");
    let contributor_1_ip = IpAddr::V4("0.0.0.1".parse().unwrap());
    let (contributor2, contributor_signing_key2, seed2) = create_contributor("2");
    let contributor_2_ip = IpAddr::V4("0.0.0.2".parse().unwrap());
    let (verifier, verifier_signing_key) = create_verifier("1");
    coordinator.add_to_queue(contributor1.clone(), Some(contributor_1_ip), token, 10)?;
    coordinator.add_to_queue(contributor2.clone(), Some(contributor_2_ip), token2, 9)?;

    // Update the ceremony to round 1.
    coordinator.update()?;

    for _ in 0..number_of_chunks {
        coordinator.contribute(&contributor1, &contributor_signing_key1, &seed1)?;
        coordinator.contribute(&contributor2, &contributor_signing_key2, &seed2)?;
        verify_task_if_available(&mut coordinator, &verifier, &verifier_signing_key)?;
        verify_task_if_available(&mut coordinator, &verifier, &verifier_signing_key)?;
    }

    for (_participant, contributor_info) in coordinator.current_contributors() {
        assert_eq!(contributor_info.completed_tasks().len(), 8);
        assert_eq!(contributor_info.assigned_tasks().len(), 0);
        assert_eq!(contributor_info.disposing_tasks().len(), 0);
        assert_eq!(contributor_info.disposed_tasks().len(), 0);
    }

    // Drop the contributor from the current round.
    coordinator.drop_participant(&contributor1)?;
    assert_eq!(false, coordinator.is_queue_contributor(&contributor1));
    assert_eq!(false, coordinator.is_queue_contributor(&contributor2));
    assert_eq!(false, coordinator.is_current_contributor(&contributor1));
    assert_eq!(true, coordinator.is_current_contributor(&contributor2));
    assert_eq!(false, coordinator.is_finished_contributor(&contributor1));
    assert_eq!(false, coordinator.is_finished_contributor(&contributor2));

    for (participant, contributor_info) in coordinator.current_contributors() {
        if participant == contributor2 {
            assert_eq!(contributor_info.completed_tasks().len(), 4);
            assert_eq!(contributor_info.assigned_tasks().len(), 4);
            assert_eq!(contributor_info.disposing_tasks().len(), 0);
            assert_eq!(contributor_info.disposed_tasks().len(), 4);
        } else {
            // Replacement contributor
            assert_eq!(contributor_info.completed_tasks().len(), 0);
            assert_eq!(contributor_info.assigned_tasks().len(), 8);
            assert_eq!(contributor_info.disposing_tasks().len(), 0);
            assert_eq!(contributor_info.disposed_tasks().len(), 0);
        }
    }

    Ok(())
}

/// Test that participants who have not been seen for longer than the
/// [Environment::contributor_timeout_in_minutes] will be dropped.
#[test]
#[serial]
fn contributor_timeout_drop_test() -> anyhow::Result<()> {
    let time = Arc::new(MockTimeSource::new(OffsetDateTime::now_utc()));

    let parameters = Parameters::Custom(Settings::new(
        ContributionMode::Chunked,
        ProvingSystem::Groth16,
        CurveKind::Bls12_377,
        6,  /* power */
        16, /* batch_size */
        16, /* chunk_size */
    ));

    let testing_deployment: Testing = Testing::from(parameters)
        .contributor_seen_timeout(time::Duration::minutes(5))
        .participant_lock_timeout(time::Duration::minutes(10));

    let environment = initialize_test_environment(&Environment::from(testing_deployment));

    // Instantiate a coordinator.
    let mut coordinator = Coordinator::new_with_time(environment, Arc::new(Dummy), time.clone())?;

    // Initialize the ceremony to round 0.
    coordinator.initialize()?;

    let (contributor1, _contributor_signing_key1, _seed1) = create_contributor("1");
    let token = String::from("test_token");
    let contributor_1_ip = IpAddr::V4(Ipv4Addr::UNSPECIFIED);

    coordinator.add_to_queue(contributor1.clone(), Some(contributor_1_ip), token, 10)?;

    // Update the ceremony to round 1.
    coordinator.update()?;

    assert_eq!(1, coordinator.current_contributors().len());
    assert!(coordinator.dropped_participants().is_empty());

    // increment the time a little bit (but not enough for the
    // contributor to timeout)
    time.update(|prev| prev + time::Duration::minutes(1));
    coordinator.update()?;

    assert_eq!(1, coordinator.current_contributors().len());
    assert!(coordinator.dropped_participants().is_empty());

    // push the time past the timout
    time.update(|prev| prev + time::Duration::minutes(5));
    coordinator.update()?;

    // Check that replacement contributor has been added, and that the
    // contributor1 has been dropped.
    assert_eq!(1, coordinator.current_contributors().len());
    assert!(coordinator.current_contributors().get(0).unwrap().0 != contributor1);
    assert_eq!(1, coordinator.dropped_participants().len());
    assert_eq!(&contributor1, coordinator.dropped_participants().get(0).unwrap().id());

    Ok(())
}

/// Test that participant who is waiting for a verifier to verify
/// chunks that it depends on is not dropped from the round.
#[test]
#[serial]
fn contributor_wait_verifier_test() -> anyhow::Result<()> {
    let time = Arc::new(MockTimeSource::new(OffsetDateTime::now_utc()));

    let parameters = Parameters::Custom(Settings::new(
        ContributionMode::Chunked,
        ProvingSystem::Groth16,
        CurveKind::Bls12_377,
        6,  /* power */
        16, /* batch_size */
        16, /* chunk_size */
    ));
    let testing_deployment: Testing = Testing::from(parameters)
        .contributor_seen_timeout(time::Duration::minutes(5))
        .participant_lock_timeout(time::Duration::minutes(8));

    let environment = initialize_test_environment(&Environment::from(testing_deployment));
    let number_of_chunks = environment.number_of_chunks() as usize;

    // Instantiate a coordinator.
    let mut coordinator = Coordinator::new_with_time(environment, Arc::new(Dummy), time.clone())?;

    // Initialize the ceremony to round 0.
    coordinator.initialize()?;

    let token = String::from("test_token");
    let token2 = String::from("test_token_2");
    let (contributor1, contributor_signing_key1, seed1) = create_contributor("1");
    let contributor_1_ip = IpAddr::V4("0.0.0.1".parse().unwrap());
    let (contributor2, contributor_signing_key2, seed2) = create_contributor("2");
    let contributor_2_ip = IpAddr::V4("0.0.0.2".parse().unwrap());

    coordinator.add_to_queue(contributor1.clone(), Some(contributor_1_ip), token, 10)?;
    coordinator.add_to_queue(contributor2.clone(), Some(contributor_2_ip), token2, 10)?;

    // Update the ceremony to round 1.
    coordinator.update()?;

    for _ in 0..(number_of_chunks / 2) {
        time.update(|prev| prev + time::Duration::minutes(1));
        coordinator.contribute(&contributor1, &contributor_signing_key1, &seed1)?;
        coordinator.contribute(&contributor2, &contributor_signing_key2, &seed2)?;
    }

    // The next contribution cannot be made because it depends on
    // contributions that have not yet been made.
    assert!(coordinator.try_lock(&contributor1).is_err());

    coordinator.update()?;
    assert!(coordinator.dropped_participants().is_empty());

    // contributors are stuck waiting for 10 minutes, longer than the
    // contributor timeout duration.
    time.update(|prev| prev + time::Duration::minutes(10));

    // Emulate contributor querying the current round via the
    // `/v1/round/current` endpoint.
    let _round = coordinator.current_round().unwrap();

    // Only contributor1 performs a heartbeat
    coordinator.heartbeat(&contributor1).unwrap();

    coordinator.update()?;

    // contributor2 is dropped because it did not perform a heartbeat
    // while waiting.
    let dropped_participants = coordinator.dropped_participants();
    assert_eq!(1, dropped_participants.len());
    assert_eq!(&contributor2, dropped_participants.get(0).unwrap().id());

    Ok(())
}

/// Test that a participant who maintains a lock on a chunk for longer
/// than [Environment::participant_lock_timeout] is dropped from the
/// round by the coordinator.
#[test]
#[serial]
fn participant_lock_timeout_drop_test() -> anyhow::Result<()> {
    let time = Arc::new(MockTimeSource::new(OffsetDateTime::now_utc()));

    let parameters = Parameters::Custom(Settings::new(
        ContributionMode::Chunked,
        ProvingSystem::Groth16,
        CurveKind::Bls12_377,
        6,  /* power */
        16, /* batch_size */
        16, /* chunk_size */
    ));

    let testing_deployment: Testing = Testing::from(parameters)
        .contributor_seen_timeout(time::Duration::minutes(20))
        .participant_lock_timeout(time::Duration::minutes(10));

    let environment = initialize_test_environment(&Environment::from(testing_deployment));

    // Instantiate a coordinator.
    let mut coordinator = Coordinator::new_with_time(environment, Arc::new(Dummy), time.clone())?;

    // Initialize the ceremony to round 0.
    coordinator.initialize()?;

    let (contributor1, contributor_signing_key1, seed1) = create_contributor("1");
    let contributor_1_ip = IpAddr::V4(Ipv4Addr::UNSPECIFIED);
    let token = String::from("test_token");

    coordinator.add_to_queue(contributor1.clone(), Some(contributor_1_ip), token, 10)?;

    // Update the ceremony to round 1.
    coordinator.update()?;

    assert_eq!(1, coordinator.current_contributors().len());
    assert!(coordinator.dropped_participants().is_empty());

    coordinator.contribute(&contributor1, &contributor_signing_key1, &seed1)?;

    coordinator.try_lock(&contributor1)?;

    // increment the time a little bit (but not enough for the
    // lock to timeout)
    time.update(|prev| prev + time::Duration::minutes(1));
    coordinator.update()?;

    assert_eq!(1, coordinator.current_contributors().len());
    assert!(coordinator.dropped_participants().is_empty());

    // push the time past the timout
    time.update(|prev| prev + time::Duration::minutes(10));
    coordinator.update()?;

    // Check that replacement contributor has been added, and that the
    // contributor1 has been dropped.
    assert_eq!(1, coordinator.current_contributors().len());
    assert_eq!(1, coordinator.dropped_participants().len());
    assert!(coordinator.current_contributors().get(0).unwrap().0 != contributor1);
    assert_eq!(&contributor1, coordinator.dropped_participants().get(0).unwrap().id());

    Ok(())
}

/// Test that a participant who stays in the queue for more
/// than [Environment::queue_seen_timeout] is dropped from the
/// queue by the coordinator.
#[test]
#[serial]
fn queue_seen_timeout_drop_test() -> anyhow::Result<()> {
    let time = Arc::new(MockTimeSource::new(OffsetDateTime::now_utc()));

    let parameters = Parameters::Custom(Settings::new(
        ContributionMode::Chunked,
        ProvingSystem::Groth16,
        CurveKind::Bls12_377,
        6,  /* power */
        16, /* batch_size */
        16, /* chunk_size */
    ));

    let testing_deployment: Testing = Testing::from(parameters)
        .contributor_seen_timeout(time::Duration::days(20))
        .participant_lock_timeout(time::Duration::days(20));

    let environment = initialize_test_environment(&Environment::from(testing_deployment));

    // Instantiate a coordinator.
    let mut coordinator = Coordinator::new_with_time(environment, Arc::new(Dummy), time.clone())?;

    // Initialize the ceremony to round 0.
    coordinator.initialize()?;

    let (contributor1, _, _) = create_contributor("1");
    let contributor_1_ip = IpAddr::V4("0.0.0.1".parse().unwrap());
    let token = String::from("test_token");
    let token2 = String::from("test_token_2");

    coordinator.add_to_queue(contributor1.clone(), Some(contributor_1_ip), token, 10)?;

    // Update the ceremony to round 1.
    coordinator.update()?;

    // Add another contributor who we are gonna try to drop
    let (contributor2, _, _) = create_contributor("2");
    let contributor_2_ip = IpAddr::V4("0.0.0.2".parse().unwrap());
    coordinator.add_to_queue(contributor2.clone(), Some(contributor_2_ip), token2, 10)?;

    assert_eq!(1, coordinator.current_contributors().len());
    assert!(coordinator.is_queue_contributor(&contributor2));
    assert!(coordinator.dropped_participants().is_empty());

    // increment the time a little bit (but not enough for the
    // lock to timeout)
    time.update(|prev| prev + time::Duration::days(5));
    coordinator.update()?;

    assert_eq!(1, coordinator.current_contributors().len());
    assert!(coordinator.is_queue_contributor(&contributor2));
    assert!(coordinator.dropped_participants().is_empty());

    // push the time past the timout
    time.update(|prev| prev + time::Duration::days(10));
    coordinator.update()?;

    // Check that replacement contributor has been added, and that the
    // contributor1 has been dropped.
    assert_eq!(1, coordinator.current_contributors().len());
    assert!(!coordinator.is_queue_contributor(&contributor2));
    assert!(coordinator.dropped_participants().is_empty());

    Ok(())
}

/// Test that a participant can remain in the queue by sending heartbeats.
#[test]
#[serial]
fn queue_seen_timeout_heartbeat_test() -> anyhow::Result<()> {
    let time = Arc::new(MockTimeSource::new(OffsetDateTime::now_utc()));

    let parameters = Parameters::Custom(Settings::new(
        ContributionMode::Chunked,
        ProvingSystem::Groth16,
        CurveKind::Bls12_377,
        6,  /* power */
        16, /* batch_size */
        16, /* chunk_size */
    ));

    let testing_deployment: Testing = Testing::from(parameters)
        .contributor_seen_timeout(time::Duration::days(20))
        .participant_lock_timeout(time::Duration::days(20));

    let environment = initialize_test_environment(&Environment::from(testing_deployment));

    // Instantiate a coordinator.
    let mut coordinator = Coordinator::new_with_time(environment, Arc::new(Dummy), time.clone())?;

    // Initialize the ceremony to round 0.
    coordinator.initialize()?;

    let (contributor1, _, _) = create_contributor("1");
    let contributor_1_ip = IpAddr::V4("0.0.0.1".parse().unwrap());
    let token = String::from("test_token");
    let token2 = String::from("test_token_2");

    coordinator.add_to_queue(contributor1.clone(), Some(contributor_1_ip), token, 10)?;

    // Update the ceremony to round 1.
    coordinator.update()?;

    // Add another contributor who we are gonna try to drop
    let (contributor2, _, _) = create_contributor("2");
    let contributor_2_ip = IpAddr::V4("0.0.0.2".parse().unwrap());

    coordinator.add_to_queue(contributor2.clone(), Some(contributor_2_ip), token2, 10)?;

    assert_eq!(1, coordinator.current_contributors().len());
    assert!(coordinator.is_queue_contributor(&contributor2));
    assert!(coordinator.dropped_participants().is_empty());

    // increment the time a little bit (but not enough for the
    // lock to timeout)
    time.update(|prev| prev + time::Duration::days(5));
    coordinator.update()?;

    // Send heartbeat from contributor2
    coordinator.heartbeat(&contributor2).unwrap();

    assert_eq!(1, coordinator.current_contributors().len());
    assert!(coordinator.is_queue_contributor(&contributor2));
    assert!(coordinator.dropped_participants().is_empty());

    // push the time past the timout
    time.update(|prev| prev + time::Duration::days(5));
    coordinator.update()?;

    // Check that replacement contributor has been added, and that the
    // contributor1 has been dropped.
    assert_eq!(1, coordinator.current_contributors().len());
    assert!(coordinator.is_queue_contributor(&contributor2));
    assert!(coordinator.dropped_participants().is_empty());

    Ok(())
}

/// Test that a participant who maintains a lock on a chunk for longer
/// than [Environment::participant_lock_timeout] is dropped from the
/// round by the coordinator.
#[test]
#[serial]
fn rollback_locked_chunk() -> anyhow::Result<()> {
    let time = Arc::new(MockTimeSource::new(OffsetDateTime::now_utc()));

    let parameters = Parameters::Custom(Settings::new(
        ContributionMode::Chunked,
        ProvingSystem::Groth16,
        CurveKind::Bls12_377,
        6,  /* power */
        16, /* batch_size */
        16, /* chunk_size */
    ));

    let testing_deployment: Testing = Testing::from(parameters)
        .contributor_seen_timeout(time::Duration::minutes(20))
        .participant_lock_timeout(time::Duration::minutes(10));

    let environment = initialize_test_environment(&Environment::from(testing_deployment));

    // Instantiate a coordinator.
    let mut coordinator = Coordinator::new_with_time(environment, Arc::new(Dummy), time.clone())?;

    // Initialize the ceremony to round 0.
    coordinator.initialize()?;

    let (contributor1, contributor_signing_key1, seed1) = create_contributor("1");
    let contributor_1_ip = IpAddr::V4("0.0.0.1".parse().unwrap());
    let token = String::from("test_token");

    coordinator.add_to_queue(contributor1.clone(), Some(contributor_1_ip), token, 10)?;

    // Update the ceremony to round 1.
    coordinator.update()?;

    assert_eq!(1, coordinator.current_contributors().len());
    assert!(coordinator.dropped_participants().is_empty());

    coordinator.contribute(&contributor1, &contributor_signing_key1, &seed1)?;

    let (_, contributor_info) = &coordinator.current_contributors()[0];
    let num_locked = contributor_info.locked_chunks().len();
    let num_assigned = contributor_info.assigned_tasks().len();
    let num_pending = contributor_info.pending_tasks().len();

    let (chunk_id, _) = coordinator.try_lock(&contributor1)?;
    let task = Task::new(chunk_id, 1);
    coordinator.rollback_locked_task(&contributor1, task)?;

    assert_eq!(num_locked, contributor_info.locked_chunks().len());
    assert_eq!(num_assigned, contributor_info.assigned_tasks().len());
    assert_eq!(num_pending, contributor_info.pending_tasks().len());
    assert!(contributor_info.assigned_tasks().contains(&task));

    let current_round = coordinator.current_round()?;
    let chunk = current_round.chunk(task.chunk_id())?;
    assert_eq!(&None, chunk.lock_holder());

    Ok(())
}

#[test]
#[serial]
fn round_on_groth16_bls12_377() {
    execute_round(ProvingSystem::Groth16, CurveKind::Bls12_377).unwrap();
}

#[test]
#[serial]
fn round_on_groth16_bw6_761() {
    execute_round(ProvingSystem::Groth16, CurveKind::BW6).unwrap();
}

#[test]
#[serial]
fn round_on_marlin_bls12_377() {
    execute_round(ProvingSystem::Marlin, CurveKind::Bls12_377).unwrap();
}
