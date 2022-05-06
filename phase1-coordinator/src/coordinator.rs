//! This module contains the central piece of this crate, the
//! [Coordinator]. The coordinator's state is stored in a
//! [CoordinatorState] object.

use crate::{
    authentication::Signature,
    commands::{Aggregation, Initialization},
    coordinator_state::{
        CeremonyStorageAction,
        CoordinatorState,
        DropParticipant,
        ParticipantInfo,
        ResetCurrentRoundStorageAction,
        RoundMetrics,
    },
    environment::{Deployment, Environment},
    objects::{participant::*, task::TaskInitializationError, ContributionFileSignature, LockedLocators, Round, Task},
    storage::{
        ContributionLocator,
        ContributionSignatureLocator,
        Disk,
        Locator,
        LocatorPath,
        Object,
        StorageAction,
        StorageLocator,
        StorageObject,
        UpdateAction,
    },
};
use setup_utils::calculate_hash;

use std::{
    fmt,
    net::IpAddr,
    sync::{Arc, RwLock},
};
use time::OffsetDateTime;
use tracing::*;

#[cfg(any(test, feature = "operator"))]
use std::collections::HashMap;

#[derive(Debug)]
pub enum CoordinatorError {
    AggregateContributionFileSizeMismatch,
    ChallengeHashSizeInvalid,
    ChunkAlreadyComplete,
    ChunkAlreadyVerified,
    ChunkIdAlreadyAdded,
    ChunkIdInvalid,
    ChunkIdMismatch,
    ChunkIdMissing,
    ChunkLockAlreadyAcquired,
    ChunkLockLimitReached,
    ChunkMissing,
    ChunkMissingVerification,
    ChunkCannotLockZeroContributions { chunk_id: u64 },
    ChunkNotLockedOrByWrongParticipant,
    ComputationFailed,
    CompressedContributionHashingUnsupported,
    ContributorPendingTasksCannotBeEmpty(Participant),
    ContributionAlreadyAssignedVerifiedLocator,
    ContributionAlreadyAssignedVerifier,
    ContributionAlreadyVerified,
    ContributionFailed,
    ContributionFileSignatureLocatorAlreadyExists,
    ContributionFileSizeMismatch,
    ContributionHashMismatch,
    ContributionIdIsNonzero,
    ContributionIdMismatch,
    ContributionIdMustBeNonzero,
    ContributionLocatorAlreadyExists,
    ContributionLocatorIncorrect,
    ContributionLocatorMissing,
    ContributionMissing,
    ContributionMissingVerification,
    ContributionMissingVerifiedLocator,
    ContributionMissingVerifier,
    ContributionShouldNotExist,
    ContributionSignatureFileSizeMismatch,
    ContributionSignatureSizeMismatch,
    ContributionsComplete,
    ContributorAlreadyContributed,
    ContributorSignatureInvalid,
    ContributorsMissing,
    CoordinatorContributorMissing,
    CoordinatorStateNotInitialized,
    CurrentRoundAggregating,
    CurrentRoundAggregated,
    CurrentRoundFinished,
    CurrentRoundNotAggregated,
    CurrentRoundNotFinished,
    DropParticipantFailed,
    ExpectedContributor,
    ExpectedVerifier,
    Error(anyhow::Error),
    InitializationFailed,
    InitializationTranscriptsDiffer,
    Integer(std::num::ParseIntError),
    IOError(std::io::Error),
    Hex(hex::FromHexError),
    JsonError(serde_json::Error),
    JustificationInvalid,
    LocatorDeserializationFailed,
    LocatorFileAlreadyExists,
    LocatorFileAlreadyExistsAndOpen,
    LocatorFileAlreadyOpen,
    LocatorFileMissing,
    LocatorFileNotOpen,
    LocatorFileShouldBeOpen,
    LocatorSerializationFailed,
    NextChallengeHashAlreadyExists,
    NextChallengeHashSizeInvalid,
    NextChallengeHashMissing,
    NextRoundAlreadyInPrecommit,
    NextRoundShouldBeEmpty,
    NumberOfChunksInvalid,
    NumberOfContributionsDiffer,
    ParticipantAlreadyAdded,
    ParticipantAlreadyAddedChunk,
    ParticipantAlreadyBanned,
    ParticipantAlreadyDropped,
    ParticipantAlreadyFinished,
    ParticipantAlreadyFinishedChunk { chunk_id: u64 },
    ParticipantAlreadyFinishedTask(Task),
    ParticipantAlreadyHasLockedChunk,
    ParticipantAlreadyHasLockedChunks,
    ParticipantAlreadyPrecommitted,
    ParticipantAlreadyStarted,
    ParticipantAlreadyWorkingOnChunk { chunk_id: u64 },
    ParticipantBanned,
    ParticipantDidNotDoWork,
    ParticipantDidntLockChunkId,
    ParticipantHasAssignedTasks,
    ParticipantHasLockedMaximumChunks,
    ParticipantHasNotStarted,
    ParticipantHasNoRemainingTasks,
    ParticipantHasRemainingTasks,
    ParticipantInCurrentRoundCannotJoinQueue,
    ParticipantLockedChunkWithManyContributions,
    ParticipantMissing,
    ParticipantMissingDisposingTask,
    ParticipantMissingPendingTask { pending_task: Task },
    ParticipantNotFound(Participant),
    ParticipantNotReady,
    ParticipantRoundHeightInvalid,
    ParticipantRoundHeightMissing,
    ParticipantShouldHavePendingTasks,
    ParticipantShouldNotBeFinished,
    ParticipantStillHasLock,
    ParticipantStillHasLocks,
    ParticipantStillHasTaskAsAssigned,
    ParticipantStillHasTaskAsPending,
    ParticipantUnauthorized,
    ParticipantUnauthorizedForChunkId { chunk_id: u64 },
    ParticipantWasDropped,
    PendingTasksMustContainResponseTask { response_task: Task },
    Phase1Setup(setup_utils::Error),
    QueueIsEmpty,
    QueueWaitTimeIncomplete,
    ResponseHashSizeInvalid,
    RoundAggregationFailed,
    RoundAlreadyInitialized,
    RoundAlreadyAggregated,
    RoundCommitFailedOrCorrupted,
    RoundContributorMissing,
    RoundContributorsMissing,
    RoundContributorsNotUnique,
    RoundDirectoryMissing,
    RoundDoesNotExist,
    RoundFileMissing,
    RoundFileSizeMismatch,
    RoundHeightIsZero,
    RoundHeightMismatch,
    RoundHeightNotSet,
    RoundLocatorAlreadyExists,
    RoundLocatorMissing,
    RoundNotAggregated,
    RoundNotComplete,
    RoundNotReady,
    RoundNumberOfContributorsUnauthorized,
    RoundNumberOfVerifiersUnauthorized,
    RoundShouldNotExist,
    RoundStateMissing,
    RoundUpdateCorruptedStateOfContributors,
    RoundUpdateCorruptedStateOfVerifiers,
    RoundVerifiersMissing,
    RoundVerifiersNotUnique,
    SignatureSchemeIsInsecure,
    StorageCopyFailed,
    StorageFailed,
    StorageInitializationFailed,
    StorageLocatorAlreadyExists,
    StorageLocatorAlreadyExistsAndOpen,
    StorageLocatorFormatIncorrect,
    StorageLocatorMissing,
    StorageLocatorNotOpen,
    StorageLockFailed,
    StorageReaderFailed,
    StorageSizeLookupFailed,
    StorageUpdateFailed,
    TaskInitializationFailed(TaskInitializationError),
    PreviousContributionMissing { current_task: Task },
    TryFromSliceError(std::array::TryFromSliceError),
    UnauthorizedChunkContributor,
    UnauthorizedChunkVerifier,
    VerificationFailed,
    VerificationOnContributionIdZero,
    VerifierMissing,
    VerifierSignatureInvalid,
    VerifiersMissing,
}

impl From<TaskInitializationError> for CoordinatorError {
    fn from(error: TaskInitializationError) -> Self {
        Self::TaskInitializationFailed(error)
    }
}

impl From<anyhow::Error> for CoordinatorError {
    fn from(error: anyhow::Error) -> Self {
        CoordinatorError::Error(error)
    }
}

impl From<hex::FromHexError> for CoordinatorError {
    fn from(error: hex::FromHexError) -> Self {
        CoordinatorError::Hex(error)
    }
}

impl From<serde_json::Error> for CoordinatorError {
    fn from(error: serde_json::Error) -> Self {
        CoordinatorError::JsonError(error)
    }
}

impl From<setup_utils::Error> for CoordinatorError {
    fn from(error: setup_utils::Error) -> Self {
        CoordinatorError::Phase1Setup(error)
    }
}

impl From<std::array::TryFromSliceError> for CoordinatorError {
    fn from(error: std::array::TryFromSliceError) -> Self {
        CoordinatorError::TryFromSliceError(error)
    }
}

impl From<std::io::Error> for CoordinatorError {
    fn from(error: std::io::Error) -> Self {
        CoordinatorError::IOError(error)
    }
}

impl From<std::num::ParseIntError> for CoordinatorError {
    fn from(error: std::num::ParseIntError) -> Self {
        CoordinatorError::Integer(error)
    }
}

impl fmt::Display for CoordinatorError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        error!("{}", self);
        write!(f, "{:?}", self)
    }
}

impl From<CoordinatorError> for anyhow::Error {
    fn from(error: CoordinatorError) -> Self {
        error!("{}", error);
        match error {
            CoordinatorError::Error(anyhow_error) => anyhow_error,
            _ => Self::msg(error.to_string()),
        }
    }
}

/// A trait for providing a source of time to the coordinator, used
/// for mocking system time during testing.
pub trait TimeSource: Send + Sync {
    /// Provide the current time now in the UTC timezone
    fn now_utc(&self) -> OffsetDateTime;
}

// Private tuple field to force use of constructor.
/// A [TimeSource] implementation that fetches the current system time
/// using [Utc].
pub(crate) struct SystemTimeSource(());

impl SystemTimeSource {
    pub fn new() -> Self {
        Self(())
    }
}

impl TimeSource for SystemTimeSource {
    fn now_utc(&self) -> OffsetDateTime {
        OffsetDateTime::now_utc()
    }
}

/// A time source to use for testing, allows the current time to be
/// set manually.
pub struct MockTimeSource {
    time: RwLock<OffsetDateTime>,
}

impl MockTimeSource {
    pub fn new(time: OffsetDateTime) -> Self {
        Self {
            time: RwLock::new(time),
        }
    }

    pub fn set_time(&self, time: OffsetDateTime) {
        *self.time.write().expect("Unable to lock to write time") = time;
    }

    pub fn time(&self) -> OffsetDateTime {
        *self.time.read().expect("Unable to obtain lock to read time")
    }

    pub fn update<F: Fn(OffsetDateTime) -> OffsetDateTime>(&self, f: F) {
        self.set_time(f(self.time()))
    }
}

impl TimeSource for MockTimeSource {
    fn now_utc(&self) -> OffsetDateTime {
        *self.time.read().expect("Unable to obtain lock to read time")
    }
}

/// A core structure for operating the Phase 1 ceremony. This struct
/// is designed to be [Send] + [Sync]. The state of the ceremony is
/// stored in a [CoordinatorState] object.
pub struct Coordinator {
    /// The parameters and settings of this coordinator.
    environment: Environment,
    /// The signature scheme for contributors & verifiers with this coordinator.
    signature: Arc<dyn Signature>,
    /// The storage of contributions and rounds for this coordinator.
    storage: Disk,
    /// The current round and participant self.
    state: CoordinatorState,
    /// The source of time, allows mocking system time for testing.
    time: Arc<dyn TimeSource>,
    /// Callback to call after aggregation is done
    aggregation_callback: Arc<dyn Fn(Vec<Participant>) -> () + Send + Sync>,
}

impl Coordinator {
    ///
    /// Creates a new instance of the `Coordinator`, for a given environment.
    ///
    /// The coordinator loads and instantiates an internal instance of storage.
    /// All subsequent interactions with the coordinator are directly from storage.
    ///
    /// The coordinator is forbidden from caching state about any round.
    ///
    #[inline]
    pub fn new(environment: Environment, signature: Arc<dyn Signature>) -> Result<Self, CoordinatorError> {
        Self::new_with_time(environment, signature, Arc::new(SystemTimeSource::new()))
    }

    /// Constructor that allows mocking time for testing.
    pub fn new_with_time(
        environment: Environment,
        signature: Arc<dyn Signature>,
        time: Arc<dyn TimeSource>,
    ) -> Result<Self, CoordinatorError> {
        // Load an instance of storage.
        let storage = environment.storage()?;
        // Load an instance of coordinator self.
        let state = match storage.get(&Locator::CoordinatorState)? {
            Object::CoordinatorState(state) => state,
            _ => return Err(CoordinatorError::StorageFailed),
        };

        Ok(Self {
            environment: environment.clone(),
            signature,
            storage,
            state,
            time,
            aggregation_callback: Arc::new(|_| ()),
        })
    }

    ///
    /// Set a callback which will be called after the round is aggregated.
    /// Current round finished contributors will be passed to a callback
    /// as an argument
    ///
    pub fn set_aggregation_callback(&mut self, callback: Arc<dyn Fn(Vec<Participant>) -> () + Send + Sync>) {
        self.aggregation_callback = callback;
    }
}

impl Coordinator {
    ///
    /// Runs a set of operations to initialize state and start the coordinator.
    ///
    #[inline]
    pub fn initialize(&mut self) -> Result<(), CoordinatorError> {
        // Check if the deployment is in production, that the signature scheme is secure.
        // TODO: add again the signature check after the Signature trait is implemented
        // if *self.environment.deployment() == Deployment::Production && !self.signature.is_secure() {
        //     return Err(CoordinatorError::SignatureSchemeIsInsecure);
        // }

        info!("Coordinator is booting up");
        info!("{:#?}", self.environment.parameters());

        // Ensure the ceremony is initialized, if it has not started yet.
        {
            // Check if the ceremony has been initialized yet.
            if Self::load_current_round_height(&self.storage).is_err() {
                info!("Initializing ceremony");
                let round_height = self.run_initialization(self.time.now_utc())?;
                info!("Initialized ceremony");

                // Initialize the coordinator state to round 0.
                self.state.initialize(round_height);
                self.save_state()?;
            }
        }

        // Fetch the current round height from storage. As a sanity check,
        // this call will fail if the ceremony was not initialized.
        let current_round_height = self.current_round_height()?;

        info!("Current round height is {}", current_round_height);
        info!("{}", serde_json::to_string_pretty(&self.current_round()?)?);
        info!("Coordinator has booted up");

        Ok(())
    }

    /// Save the current state of the coordinator to storage.
    pub fn save_state(&mut self) -> Result<(), CoordinatorError> {
        self.state.save(&mut self.storage)
    }

    ///
    /// Runs a set of operations to update the coordinator state to reflect
    /// newly finished, dropped, or banned participants.
    ///
    pub fn update(&mut self) -> Result<(), CoordinatorError> {
        // Process ceremony updates for the current round and queue.
        let (is_current_round_finished, is_current_round_aggregated) = {
            // Acquire the state write lock.
            info!("\n{}", self.state.status_report(self.time.as_ref()));

            // Update the metrics for the current round and participants.
            self.state.update_round_metrics();
            self.save_state()?;

            // Update the state of the queue.
            self.state.update_queue()?;
            self.save_state()?;

            // Update the state of current round contributors.
            self.state.update_current_contributors(self.time.as_ref())?;
            self.save_state()?;

            // Drop disconnected participants from the current round.
            for drop in self.state.update_dropped_participants(self.time.as_ref())? {
                // Update the round to reflect the coordinator state changes.
                self.drop_participant_from_storage(&drop)?;
            }
            self.save_state()?;

            self.state.update_dropped_queued_participants(self.time.as_ref())?;
            self.save_state()?;

            // Ban any participants who meet the coordinator criteria.
            self.state.update_banned_participants()?;
            self.save_state()?;

            // Check if the current round is finished and if the current round is aggregated.
            (
                self.state.is_current_round_finished(),
                self.state.is_current_round_aggregated(),
            )
        };

        // Try aggregating the current round if the current round is finished,
        // and has not yet been aggregated.
        let (is_current_round_aggregated, is_precommit_next_round_ready) = {
            // Check if the coordinator should aggregate, and attempt aggregation.
            if is_current_round_finished && !is_current_round_aggregated {
                // Aggregate the current round.
                self.try_aggregate()?;

                // Update the metrics for the current round and participants.
                self.state.update_round_metrics();
                self.save_state()?;

                match self.state.current_round_finished_contributors() {
                    Ok(contributors) => {
                        (self.aggregation_callback)(contributors);
                    }
                    Err(e) => {
                        tracing::error!("Failed to get current round finished contributors: {}", e);
                    }
                }
            }

            // Check if the current round is aggregated, and if the precommit for
            // the next round is ready.
            (
                self.state.is_current_round_aggregated(),
                self.state.is_precommit_next_round_ready(self.time.as_ref()),
            )
        };

        // Check if the manual lock for transitioning to the next round is enabled.
        {
            // Check if the manual lock is enabled.
            if self.state.is_manual_lock_enabled() {
                info!("Manual lock is enabled");
                return Ok(());
            }
        }

        // Try advancing to the next round if the current round is finished,
        // the current round has been aggregated, and the precommit for
        // the next round is now ready.
        if is_current_round_finished && is_current_round_aggregated && is_precommit_next_round_ready {
            // Backup a copy of the current coordinator.

            // Fetch the current time.
            let started_at = self.time.now_utc();

            // Attempt to advance to the next round.
            let next_round_height = self.try_advance(started_at)?;

            info!("Advanced ceremony to round {}", next_round_height);
        }

        Ok(())
    }

    ///
    /// Initializes a listener to handle the shutdown signal.
    ///
    pub fn shutdown(&mut self) -> anyhow::Result<()> {
        warn!("\n\nATTENTION - Coordinator is shutting down...\n");

        // Save the coordinator state to storage.
        self.save_state().unwrap();
        debug!("Coordinator has safely shutdown storage");

        // Print the final coordinator self.
        let final_state = serde_json::to_string_pretty(&self.state).unwrap();
        info!("\n\nCoordinator State at Shutdown\n\n{}\n", final_state);

        info!("\n\nCoordinator has safely shutdown.\n\nGoodbye.\n");

        Ok(())
    }

    ///
    /// Returns `true` if the given participant is a contributor in the queue.
    ///
    #[inline]
    pub fn is_queue_contributor(&self, participant: &Participant) -> bool {
        self.state.is_queue_contributor(&participant)
    }

    ///
    /// Returns the total number of contributors currently in the queue.
    ///
    #[inline]
    pub fn number_of_queue_contributors(&self) -> usize {
        self.state.number_of_queue_contributors()
    }

    ///
    /// Returns a list of the contributors currently in the queue.
    ///
    #[inline]
    pub fn queue_contributors(&self) -> Vec<(Participant, (u8, Option<u64>, OffsetDateTime, OffsetDateTime))> {
        self.state.queue_contributors()
    }

    ///
    /// Returns a list of the contributors currently in the round.
    ///
    #[inline]
    pub fn current_contributors(&self) -> Vec<(Participant, ParticipantInfo)> {
        self.state.current_contributors()
    }

    ///
    /// Returns a list of participants that were dropped from the current round.
    ///
    #[inline]
    pub fn dropped_participants(&self) -> Vec<ParticipantInfo> {
        self.state.dropped_participants()
    }

    ///
    /// Returns the metrics for the current round and current round participants.
    ///
    #[inline]
    pub fn current_round_metrics(&self) -> Option<RoundMetrics> {
        self.state.current_round_metrics()
    }

    ///
    /// Adds the given participant to the queue if they are permitted to participate.
    ///
    #[inline]
    pub fn add_to_queue(
        &mut self,
        participant: Participant,
        participant_ip: Option<IpAddr>,
        reliability_score: u8,
    ) -> Result<(), CoordinatorError> {
        // Attempt to add the participant to the next round.
        self.state
            .add_to_queue(participant, participant_ip, reliability_score, self.time.as_ref())?;

        // Save the coordinator state in storage.
        self.save_state()?;

        Ok(())
    }

    ///
    /// Removes the given participant from the queue if they are in the queue.
    ///
    #[inline]
    pub fn remove_from_queue(&mut self, participant: &Participant) -> Result<(), CoordinatorError> {
        // Attempt to remove the participant from the next round.
        self.state.remove_from_queue(participant)?;

        // Save the coordinator state in storage.
        self.save_state()?;

        Ok(())
    }

    ///
    /// Drops the given participant from the ceremony.
    ///
    #[tracing::instrument(
        skip(self, participant),
        fields(participant = %participant)
    )]
    pub fn drop_participant(&mut self, participant: &Participant) -> Result<(), CoordinatorError> {
        // Drop the participant from the ceremony.
        let drop = self.state.drop_participant(participant, self.time.as_ref())?;

        // Update the round to reflect the coordinator state change.
        self.drop_participant_from_storage(&drop)?;

        // Save the coordinator state in storage.
        self.save_state()?;

        Ok(())
    }

    ///
    /// Bans the given participant from the ceremony.
    ///
    #[inline]
    pub fn ban_participant(&mut self, participant: &Participant) -> Result<(), CoordinatorError> {
        // Ban the participant from the ceremony.
        let drop = self.state.ban_participant(participant, self.time.as_ref())?;

        // Update the round on disk to reflect the coordinator state change.
        self.drop_participant_from_storage(&drop)?;

        // Save the coordinator state in storage.
        self.save_state()?;

        Ok(())
    }

    ///
    /// Unbans the given participant from joining the queue.
    ///
    #[inline]
    pub fn unban_participant(&mut self, participant: &Participant) -> Result<(), CoordinatorError> {
        // Unban the participant from the ceremony.
        self.state.unban_participant(participant);

        // Save the coordinator state in storage.
        self.save_state()?;

        Ok(())
    }

    ///
    /// Returns `true` if the manual lock for transitioning to the next round is enabled.
    ///
    #[inline]
    pub fn is_manual_lock_enabled(&self) -> bool {
        // Fetch the state of the manual lock.
        self.state.is_manual_lock_enabled()
    }

    ///
    /// Sets the manual lock for transitioning to the next round to `true`.
    ///
    #[inline]
    pub fn enable_manual_lock(&mut self) -> Result<(), CoordinatorError> {
        // Sets the manual lock to `true`.
        self.state.enable_manual_lock();

        // Save the coordinator state in storage.
        self.save_state()?;

        Ok(())
    }

    ///
    /// Sets the manual lock for transitioning to the next round to `false`.
    ///
    #[inline]
    pub fn disable_manual_lock(&mut self) -> Result<(), CoordinatorError> {
        // Sets the manual lock to `false`.
        self.state.disable_manual_lock();

        // Save the coordinator state in storage.
        self.save_state()?;

        Ok(())
    }

    ///
    /// Returns `true` if the given participant is authorized as a
    /// contributor and listed in the contributor IDs for this round.
    ///
    /// If the participant is not a contributor, or if there are
    /// no prior rounds, returns `false`.
    ///
    #[inline]
    pub fn is_current_contributor(&self, participant: &Participant) -> bool {
        // Fetch the current round from storage.
        let round = match Self::load_current_round(&self.storage) {
            // Case 1 - This is a typical round of the ceremony.
            Ok(round) => round,
            // Case 2 - The ceremony has not started or storage has failed.
            _ => return false,
        };

        // Check that the participant is a contributor for the given round height.
        if !round.is_contributor(participant) {
            return false;
        }

        // Check that the participant is a current contributor.
        self.state.is_current_contributor(participant)
    }

    ///
    /// Returns `true` if the given participant has finished contributing
    ///
    #[inline]
    pub fn is_finished_contributor(&self, participant: &Participant) -> bool {
        // Fetch the state of the current contributor.
        self.state.is_finished_contributor(&participant)
    }

    ///
    /// Returns `true` if the given participant is a contributor managed
    /// by the coordinator.
    ///
    #[inline]
    pub fn is_coordinator_contributor(&self, participant: &Participant) -> bool {
        // Check that the participant is a coordinator contributor.
        self.state.is_coordinator_contributor(&participant)
    }

    ///
    /// Returns `true` if the given participant is a verifier managed
    /// by the coordinator.
    ///
    #[inline]
    pub fn is_coordinator_verifier(&self, participant: &Participant) -> bool {
        // Check that the participant is a coordinator verifier.
        self.state.is_coordinator_verifier(&participant)
    }

    ///
    /// Returns the current round height of the ceremony from storage,
    /// irrespective of the stage of its completion.
    ///
    /// When loading the current round height from storage, this function
    /// checks that the corresponding round is in storage. Note that it
    /// only checks for the existence of a round value and does not
    /// check for its correctness.
    ///
    #[inline]
    pub fn current_round_height(&self) -> Result<u64, CoordinatorError> {
        trace!("Fetching the current round height from storage");

        // Fetch the current round height from storage.
        let current_round_height = Self::load_current_round_height(&self.storage)?;

        // Fetch the current round from storage.
        match self.storage.exists(&Locator::RoundState {
            round_height: current_round_height,
        }) {
            // Case 1 - This is a typical round of the ceremony.
            true => Ok(current_round_height),
            // Case 2 - Storage failed to locate the current round.
            false => Err(CoordinatorError::StorageFailed),
        }
    }

    ///
    /// Returns the current round state of the ceremony from storage,
    /// irrespective of the stage of its completion.
    ///
    /// If there are no prior rounds in storage, returns `CoordinatorError`.
    ///
    /// When loading the current round from storage, this function
    /// checks that the current round height matches the height
    /// set in the returned `Round` instance.
    ///
    #[inline]
    pub fn current_round(&self) -> Result<Round, CoordinatorError> {
        trace!("Fetching the current round from storage");

        // Fetch the current round from storage.
        Self::load_current_round(&self.storage)
    }

    ///
    /// Returns the round state corresponding to the given height from storage.
    ///
    /// If there are no prior rounds, returns a `CoordinatorError`.
    ///
    pub fn get_round(&self, round_height: u64) -> Result<Round, CoordinatorError> {
        // Fetch the current round height from storage.
        let current_round_height = Self::load_current_round_height(&self.storage)?;

        // Check that the given round height is valid.
        match round_height <= current_round_height {
            // Fetch the round corresponding to the given round height from storage.
            true => Ok(serde_json::from_slice(
                self.storage.reader(&Locator::RoundState { round_height })?.as_ref(),
            )?),
            // The given round height does not exist.
            false => Err(CoordinatorError::RoundDoesNotExist),
        }
    }

    /// Lets the coordinator know that the participant is still alive
    /// and participating (or waiting to participate) in the ceremony.
    pub fn heartbeat(&mut self, participant: &Participant) -> Result<(), CoordinatorError> {
        self.state.heartbeat(participant, self.time.as_ref())
    }

    ///
    /// Attempts to acquire the lock to a chunk for the given participant.
    ///
    /// On failure, this function returns a `CoordinatorError`.
    ///
    #[tracing::instrument(
        level = "error",
        skip(self),
        fields(participant = %participant),
        err
    )]
    pub fn try_lock(&mut self, participant: &Participant) -> Result<(u64, LockedLocators), CoordinatorError> {
        if participant.is_verifier() {
            return Err(CoordinatorError::ExpectedContributor);
        }

        // Check that the participant is in the current round, and has not been dropped or finished.
        if !self.state.is_current_contributor(participant) {
            return Err(CoordinatorError::ParticipantUnauthorized);
        }

        // Check that the current round is not yet finished.
        if self.state.is_current_round_finished() {
            return Err(CoordinatorError::CurrentRoundFinished);
        }

        // Check that the current round is not yet aggregating.
        if self.state.is_current_round_aggregating() {
            return Err(CoordinatorError::CurrentRoundAggregating);
        }

        // Check that the current round is not yet aggregated.
        if self.state.is_current_round_aggregated() {
            return Err(CoordinatorError::CurrentRoundAggregated);
        }

        // Attempt to fetch the next chunk ID and contribution ID for the given participant.
        let current_task = self.state.fetch_task(participant, self.time.as_ref())?;
        trace!("Fetched task {} for {}", current_task, participant);

        let round = Self::load_current_round(&self.storage)?;
        let chunk = round.chunk(current_task.chunk_id())?;
        if current_task.contribution_id() > (chunk.current_contribution_id() + 1) {
            self.state
                .rollback_pending_task(participant, current_task, &*self.time)?;
            return Err(CoordinatorError::PreviousContributionMissing { current_task });
        }

        debug!("Locking chunk {} for {}", current_task.chunk_id(), participant);
        match self.try_lock_chunk(current_task.chunk_id(), participant) {
            // Case 1 - Participant acquired lock, return the locator.
            Ok(locked_locators) => {
                trace!("Incrementing the number of locks held by {}", participant);
                self.state
                    .acquired_lock(participant, current_task.chunk_id(), self.time.as_ref())?;

                // Save the coordinator state in storage.
                self.save_state()?;

                info!("Acquired lock on chunk {} for {}", current_task.chunk_id(), participant);
                Ok((current_task.chunk_id(), locked_locators))
            }
            // Case 2 - Participant failed to acquire the lock, put the chunk ID back.
            Err(error) => {
                info!("Failed to acquire lock for {}", participant);

                trace!("Adding task {} back to assigned tasks", current_task);
                self.state
                    .rollback_pending_task(participant, current_task, self.time.as_ref())?;

                // Save the coordinator state in storage.
                self.save_state()?;

                error!("{}", error);
                return Err(error);
            }
        }
    }

    /// Returns previous contribution, current contribution and next contribution paths
    pub fn get_chunk_locators_for_verifier(
        &self,
        participant: &Participant,
        chunk_id: u64,
        contribution_id: u64,
    ) -> Result<LockedLocators, CoordinatorError> {
        if !participant.is_verifier() {
            return Err(CoordinatorError::ExpectedVerifier);
        }
        let round = Self::load_current_round(&self.storage)?;
        round.get_chunk_locators_for_verifier(&self.storage, participant, chunk_id, contribution_id)
    }

    /// Initialize the files for the next challenge
    pub fn initialize_verifier_response_files(
        &mut self,
        participant: &Participant,
        chunk_id: u64,
        locators: &LockedLocators,
    ) -> Result<(), CoordinatorError> {
        if !participant.is_verifier() {
            return Err(CoordinatorError::ExpectedVerifier);
        }
        let round = Self::load_current_round(&self.storage)?;
        round.initialize_verifier_response_files(&self.environment, &mut self.storage, participant, chunk_id, locators)
    }

    ///
    /// Attempts to add a contribution for the given chunk ID from the given participant.
    ///
    /// This function constructs the expected response locator for the given participant
    /// and chunk ID, and checks that the participant has uploaded the response file and
    /// contribution file signature prior to adding the unverified contribution to the
    /// round self.
    ///
    /// On success, this function releases the lock from the contributor and returns
    /// the response file locator.
    ///
    /// On failure, it returns a `CoordinatorError`.
    ///
    #[tracing::instrument(
        level = "error",
        skip(self, participant, chunk_id),
        fields(participant = %participant, chunk = chunk_id),
        err
    )]
    pub fn try_contribute(
        &mut self,
        participant: &Participant,
        chunk_id: u64,
    ) -> Result<ContributionLocator, CoordinatorError> {
        // Check that the participant is a contributor.
        if !participant.is_contributor() {
            return Err(CoordinatorError::ExpectedContributor);
        }

        // Check that the chunk ID is valid.
        if chunk_id > self.environment.number_of_chunks() {
            return Err(CoordinatorError::ChunkIdInvalid);
        }

        // Check that the participant is in the current round, and has not been dropped or finished.
        if !self.state.is_current_contributor(participant) {
            return Err(CoordinatorError::ParticipantUnauthorized);
        }

        // Check that the current round is not yet finished.
        if self.state.is_current_round_finished() {
            return Err(CoordinatorError::CurrentRoundFinished);
        }

        // Check that the current round is not yet aggregating.
        if self.state.is_current_round_aggregating() {
            return Err(CoordinatorError::CurrentRoundAggregating);
        }

        // Check that the current round is not yet aggregated.
        if self.state.is_current_round_aggregated() {
            return Err(CoordinatorError::CurrentRoundAggregated);
        }

        // Fetch the current round height from storage.
        let round_height = Self::load_current_round_height(&self.storage)?;
        trace!("Current round height in storage is {}", round_height);

        // Check if the participant should dispose the response being contributed.
        if let Some(task) = self.state.lookup_disposing_task(participant, chunk_id)?.cloned() {
            let contribution_id = task.contribution_id();

            // Move the task to the disposed tasks of the contributor.
            self.state.disposed_task(participant, &task, self.time.as_ref())?;

            // Remove the response file from storage.
            let response = ContributionLocator::new(round_height, chunk_id, contribution_id, false);
            self.storage.remove(&Locator::ContributionFile(response.clone()))?;

            // Save the coordinator state in storage.
            self.save_state()?;

            debug!("Removing lock for disposed task {} {}", chunk_id, contribution_id);

            // Fetch the current round from storage.
            let mut round = Self::load_current_round(&self.storage)?;

            // TODO (raychu86): Move this unsafe call out of `try_contribute`.
            // Release the lock on this chunk from the contributor.
            round.chunk_mut(chunk_id)?.set_lock_holder_unsafe(None);

            // Save the updated round to storage.
            self.storage.update(
                &Locator::RoundState {
                    round_height: round.round_height(),
                },
                Object::RoundState(round),
            )?;

            return Ok(response);
        }

        // Check if the participant has this chunk ID in a pending task.
        if let Some(task) = self.state.lookup_pending_task(participant, chunk_id)?.cloned() {
            debug!("Adding contribution for chunk");

            match self.add_contribution(chunk_id, participant) {
                // Case 1 - Participant added contribution, return the response file locator.
                Ok((locator, contribution_id)) => {
                    trace!("Release the lock on chunk");
                    let completed_task = Task::new(chunk_id, contribution_id);
                    self.state
                        .completed_task(participant, &completed_task, self.time.as_ref())?;

                    // Save the coordinator state in storage.
                    self.save_state()?;

                    info!("Added contribution");
                    return Ok(locator);
                }
                // Case 2 - Participant failed to add their contribution, remove the contribution file.
                Err(error) => {
                    info!("Failed to add a contribution and removing the contribution file");
                    // Remove the invalid response file from storage.
                    let response = Locator::ContributionFile(ContributionLocator::new(
                        round_height,
                        chunk_id,
                        task.contribution_id(),
                        false,
                    ));
                    self.storage.remove(&response)?;

                    error!("{}", error);
                    return Err(error);
                }
            }
        }

        Err(CoordinatorError::ContributionFailed)
    }

    ///
    /// Attempts to add a verification for the given chunk ID from the given participant.
    ///
    /// This function constructs the expected next challenge locator for the given participant
    /// and chunk ID, and checks that the participant has uploaded the next challenge file and
    /// contribution file signature prior to adding the verified contribution to the round state.
    ///
    /// On success, this function releases the lock from the verifier.
    ///
    /// On failure, it returns a `CoordinatorError`.
    ///
    #[tracing::instrument(
        level = "error",
        skip(self, task),
        fields(participant = %participant),
        err
    )]
    pub fn try_verify(&mut self, participant: &Participant, task: &Task) -> Result<(), CoordinatorError> {
        // Check that the participant is a verifier.
        if !participant.is_verifier() {
            return Err(CoordinatorError::ExpectedVerifier);
        }

        // Check that the chunk ID is valid.
        if task.chunk_id() > self.environment.number_of_chunks() {
            return Err(CoordinatorError::ChunkIdInvalid);
        }

        // Check that the current round is not yet finished.
        if self.state.is_current_round_finished() {
            return Err(CoordinatorError::CurrentRoundFinished);
        }

        // Check that the current round is not yet aggregating.
        if self.state.is_current_round_aggregating() {
            return Err(CoordinatorError::CurrentRoundAggregating);
        }

        // Check that the current round is not yet aggregated.
        if self.state.is_current_round_aggregated() {
            return Err(CoordinatorError::CurrentRoundAggregated);
        }

        debug!(
            "Adding verification from {} for chunk {} contribution {}",
            participant,
            task.chunk_id(),
            task.contribution_id()
        );

        match self.verify_contribution(task, participant) {
            // Case 1 - Participant verified contribution, return the response file locator.
            Ok(contribution_id) => {
                if contribution_id != task.contribution_id() {
                    return Err(CoordinatorError::ContributionIdMismatch);
                }
                self.state.completed_task(participant, task, self.time.as_ref())?;

                // Save the coordinator state in storage.
                self.save_state()?;

                info!("Added verification from {} for chunk {}", participant, task.chunk_id());
                Ok(())
            }
            // Case 2 - Participant failed to add their contribution, remove the contribution file.
            Err(error) => {
                info!("Failed to add a verification and removing the contribution file");

                // Fetch the current round from storage.
                let round = Self::load_current_round(&self.storage)?;

                // Fetch the next challenge locator.
                let is_final_contribution = task.contribution_id() == round.expected_number_of_contributions() - 1;
                let next_challenge = match is_final_contribution {
                    true => Locator::ContributionFile(ContributionLocator::new(
                        round.round_height() + 1,
                        task.chunk_id(),
                        0,
                        true,
                    )),
                    false => Locator::ContributionFile(ContributionLocator::new(
                        round.round_height(),
                        task.chunk_id(),
                        task.contribution_id(),
                        true,
                    )),
                };

                // Remove the invalid next challenge file from storage.
                if self.storage.exists(&next_challenge) {
                    self.storage.remove(&next_challenge)?;
                }

                error!("{}", error);
                Err(error)
            }
        }
    }

    ///
    /// Attempts to aggregate the contributions of the current round of the ceremony.
    ///
    #[inline]
    pub fn try_aggregate(&mut self) -> Result<(), CoordinatorError> {
        // Check that the current round height matches in storage and self.
        let current_round_height = {
            // Fetch the current round height from storage.
            let current_round_height_in_storage = Self::load_current_round_height(&self.storage)?;
            trace!("Current round height in storage is {}", current_round_height_in_storage);

            // Fetch the current round height from coordinator self.
            let current_round_height = self.state.current_round_height();
            trace!("Current round height in coordinator state is {}", current_round_height);

            // Check that the current round height matches in storage and self.
            match current_round_height_in_storage == current_round_height {
                true => current_round_height,
                false => return Err(CoordinatorError::RoundHeightMismatch),
            }
        };

        // Check that the current round is finished by contributors and verifiers.
        if !self.state.is_current_round_finished() {
            error!("Coordinator state shows round {} is not finished", current_round_height);
            return Err(CoordinatorError::RoundNotReady);
        }

        // Check that the current round has not been aggregated.
        if self.state.is_current_round_aggregated() {
            error!("Coordinator state shows round {} was aggregated", current_round_height);
            return Err(CoordinatorError::RoundAlreadyAggregated);
        }

        // Update the coordinator state to set the start of aggregation for the current round.
        self.state.aggregating_current_round(self.time.as_ref())?;

        // Check if this is round 0, as coordinator may safely skip aggregation.
        if current_round_height == 0 {
            // Set the current round as aggregated in coordinator self.
            self.state.aggregated_current_round(self.time.as_ref())?;

            debug!("Coordinator is safely skipping aggregation for round 0");
            return Ok(());
        }

        // Attempt to aggregate the current round.
        trace!("Trying to aggregate round {}", current_round_height);
        match self.aggregate_contributions() {
            // Case 1a - Coordinator aggregated the current round.
            Ok(()) => {
                info!("Coordinator has aggregated round {}", current_round_height);

                // Set the current round as aggregated in coordinator self.
                self.state.aggregated_current_round(self.time.as_ref())?;

                // Check that the current round has now been aggregated.
                if !self.state.is_current_round_aggregated() {
                    error!("Coordinator state says round {} isn't aggregated", current_round_height);
                    return Err(CoordinatorError::RoundAggregationFailed);
                }

                Ok(())
            }
            // Case 1b - Coordinator failed to aggregate the current round.
            Err(error) => {
                error!("Coordinator failed to aggregate the current round\n{}", error);

                // Rollback the current round aggregation.
                self.state.rollback_aggregating_current_round()?;

                Err(error)
            }
        }
    }

    ///
    /// Attempts to advance the ceremony to the next round.
    ///
    #[tracing::instrument(skip(self, started_at))]
    pub fn try_advance(&mut self, started_at: OffsetDateTime) -> Result<u64, CoordinatorError> {
        tracing::debug!("Trying to advance to the next round.");

        // Check that the current round height matches in storage and self.
        let current_round_height = {
            // Fetch the current round height from storage.
            let current_round_height_in_storage = Self::load_current_round_height(&self.storage)?;
            debug!("Current round height in storage is {}", current_round_height_in_storage);

            // Fetch the current round height from coordinator self.
            let current_round_height = self.state.current_round_height();
            debug!("Current round height in coordinator state is {}", current_round_height);

            // Check that the current round height matches in storage and self.
            if current_round_height_in_storage == current_round_height {
                current_round_height
            } else {
                tracing::error!(
                    "Round height in storage ({}) does not match the \
                    round height in coordinator state ({})",
                    current_round_height_in_storage,
                    current_round_height,
                );
                return Err(CoordinatorError::RoundHeightMismatch);
            }
        };

        // Check that the current round is finished.
        if !self.state.is_current_round_finished() {
            return Err(CoordinatorError::CurrentRoundNotFinished);
        }

        // Check that the current round is aggregated, if this is not round 0.
        if current_round_height > 0 && !self.state.is_current_round_aggregated() {
            return Err(CoordinatorError::CurrentRoundNotAggregated);
        }

        // Attempt to advance the round.
        trace!("Running precommit for the next round");
        let result = match self
            .state
            .precommit_next_round(current_round_height + 1, self.time.as_ref())
        {
            // Case 1 - Precommit succeed, attempt to advance the round.
            Ok(contributors) => {
                trace!("Trying to add advance to the next round");
                match self.next_round(started_at, contributors) {
                    // Case 1a - Coordinator advanced the round.
                    Ok(next_round_height) => {
                        // If success, update coordinator state to next round.
                        info!("Coordinator has advanced to round {}", next_round_height);
                        self.state.commit_next_round();
                        Ok(next_round_height)
                    }
                    // Case 1b - Coordinator failed to advance the round.
                    Err(error) => {
                        // If failed, rollback coordinator state to the current round.
                        error!("Coordinator failed to advance the next round, performing state rollback");
                        self.state.rollback_next_round(self.time.as_ref());
                        error!("{}", error);
                        Err(error)
                    }
                }
            }
            // Case 2 - Precommit failed, roll back precommit.
            Err(error) => {
                // If failed, rollback coordinator state to the current round.
                error!("Precommit for next round has failed, performing state rollback");
                self.state.rollback_next_round(self.time.as_ref());
                error!("{}", error);
                Err(error)
            }
        };

        // Save the coordinator state in storage.
        self.save_state()?;

        result
    }

    ///
    /// Returns the chunk ID from the given contribution file locator path.
    ///
    /// If the given locator path is invalid, returns a `CoordinatorError`.
    ///
    #[inline]
    pub fn contribution_locator_to_chunk_id(&self, locator_path: &LocatorPath) -> Result<u64, CoordinatorError> {
        // Fetch the chunk ID corresponding to the given locator path.
        let locator = self.storage.to_locator(&locator_path)?;
        match &locator {
            Locator::ContributionFile(contribution_locator) => match self.storage.exists(&locator) {
                true => Ok(contribution_locator.chunk_id()),
                false => Err(CoordinatorError::ContributionLocatorMissing),
            },
            _ => Err(CoordinatorError::ContributionLocatorIncorrect),
        }
    }

    /// Convert a locator to a path string using the coordinator's
    /// storage layer.
    pub fn locator_to_path(&self, locator: Locator) -> Result<LocatorPath, CoordinatorError> {
        self.storage.to_path(&locator)
    }

    ///
    /// Attempts to acquire the lock for a given chunk ID and
    /// participant.
    ///
    /// **Important**: The returned next contribution locator does not
    /// always match the current task being performed. If it is the
    /// final verification for a chunk, the a [Participant::Verifier]
    /// will receive a locator to the contribution 0 of the same chunk
    /// in the next round.
    ///
    /// On failure, this function returns a `CoordinatorError`.
    ///
    pub(crate) fn try_lock_chunk(
        &mut self,
        chunk_id: u64,
        participant: &Participant,
    ) -> Result<LockedLocators, CoordinatorError> {
        // Check that the chunk ID is valid.
        if chunk_id > self.environment.number_of_chunks() {
            return Err(CoordinatorError::ChunkIdInvalid);
        }

        // Fetch the current round height from storage.
        let current_round_height = Self::load_current_round_height(&self.storage)?;
        trace!("Current round height from storage is {}", current_round_height);

        // Fetch the current round from storage.
        let mut round = Self::load_current_round(&self.storage)?;

        // Attempt to acquire the chunk lock for participant.
        trace!("Preparing to lock chunk {}", chunk_id);
        let locked_locators = round.try_lock_chunk(&self.environment, &mut self.storage, chunk_id, &participant)?;
        trace!("Participant {} locked chunk {}", participant, chunk_id);

        // Add the updated round to storage.
        match self.storage.update(
            &Locator::RoundState {
                round_height: current_round_height,
            },
            Object::RoundState(round),
        ) {
            Ok(_) => {
                debug!("{} acquired lock on chunk {}", participant, chunk_id);
                Ok(locked_locators)
            }
            _ => Err(CoordinatorError::StorageUpdateFailed),
        }
    }

    ///
    /// Attempts to add a contribution for a given chunk ID from a given participant.
    ///
    /// This function checks that the participant is a contributor and has uploaded
    /// a valid response file to the coordinator. The coordinator sanity checks
    /// (however, does not verify) the contribution before accepting the response file.
    ///
    /// On success, this function releases the chunk lock from the contributor and
    /// returns the response file locator and contribution ID of the response file.
    ///
    /// On failure, it returns a `CoordinatorError`.
    ///
    #[inline]
    pub(crate) fn add_contribution(
        &mut self,
        chunk_id: u64,
        participant: &Participant,
    ) -> Result<(ContributionLocator, u64), CoordinatorError> {
        debug!("Adding contribution from {} to chunk {}", participant, chunk_id);

        // Fetch the current round height from storage.
        let current_round_height = Self::load_current_round_height(&self.storage)?;
        trace!("Current round height from storage is {}", current_round_height);

        // Fetch the current round from storage.
        let mut round = Self::load_current_round(&self.storage)?;
        {
            // Check that the participant is an authorized contributor to the current round.
            if !round.is_contributor(participant) {
                error!("{} is unauthorized to contribute to chunk {})", participant, chunk_id);
                return Err(CoordinatorError::UnauthorizedChunkContributor);
            }

            // Check that the chunk lock is currently held by this contributor.
            if !round.is_chunk_locked_by(chunk_id, participant) {
                error!("{} should have lock on chunk {} but does not", participant, chunk_id);
                return Err(CoordinatorError::ChunkNotLockedOrByWrongParticipant);
            }
        }

        // Fetch the expected number of contributions for the current round.
        let expected_num_contributions = round.expected_number_of_contributions();
        // Fetch the chunk corresponding to the given chunk ID.
        let chunk = round.chunk(chunk_id)?;
        // Fetch the next contribution ID of the chunk.
        let contribution_id = chunk.next_contribution_id(expected_num_contributions)?;

        // Check that the next contribution ID is one above the current contribution ID.
        if !chunk.is_next_contribution_id(contribution_id, expected_num_contributions) {
            return Err(CoordinatorError::ContributionIdMismatch);
        }

        // Fetch the challenge, response, and contribution file signature locators.
        let challenge_file_locator = Locator::ContributionFile(ContributionLocator::new(
            current_round_height,
            chunk_id,
            chunk.current_contribution_id(),
            true,
        ));
        let response_file_locator = ContributionLocator::new(current_round_height, chunk_id, contribution_id, false);
        let contribution_file_signature_locator = Locator::ContributionFileSignature(
            ContributionSignatureLocator::new(current_round_height, chunk_id, contribution_id, false),
        );

        // Check the challenge-response hash chain.
        let (challenge_hash, response_hash) = {
            // Compute the challenge hash using the challenge file.
            let challenge_reader = self.storage.reader(&challenge_file_locator)?;
            let challenge_hash = calculate_hash(challenge_reader.as_ref());
            trace!(
                "Challenge is located in {}",
                self.storage.to_path(&challenge_file_locator)?
            );

            debug!("Challenge is {}", pretty_hash!(&challenge_reader));
            debug!("Challenge hash is {}", pretty_hash!(&challenge_hash.as_slice()));

            // Compute the response hash.
            let response_reader = self.storage.reader(&Locator::ContributionFile(response_file_locator))?;
            let response_hash = calculate_hash(response_reader.as_ref());
            trace!(
                "Response is located in {}",
                self.storage
                    .to_path(&Locator::ContributionFile(response_file_locator))?
            );
            debug!("Response is {}", pretty_hash!(&response_reader));
            debug!("Response hash is {}", pretty_hash!(&response_hash.as_slice()));

            // Fetch the challenge hash from the response file.
            let challenge_hash_in_response = &response_reader
                .get(0..64)
                .ok_or(CoordinatorError::StorageReaderFailed)?[..];
            let pretty_hash = pretty_hash!(&challenge_hash_in_response);

            // Check the starting hash in the response file is based on the challenge.
            info!("The challenge hash is {}", pretty_hash!(&challenge_hash.as_slice()));
            info!("The challenge hash in response file is {}", pretty_hash);
            if challenge_hash_in_response != challenge_hash.as_slice() {
                error!("Challenge hash in response file does not match the expected challenge hash.");
                return Err(CoordinatorError::ContributionHashMismatch);
            }

            (challenge_hash, response_hash)
        };

        // Check the challenge-response contribution signature.
        {
            // Fetch the stored contribution file signature.
            let contribution_file_signature: ContributionFileSignature =
                serde_json::from_slice(&*self.storage.reader(&contribution_file_signature_locator)?)?;

            // Check that the contribution file signature is valid.
            let address = &participant.to_string();

            let address = address
                .split(".")
                .next()
                .expect("splitting a string should yield at least one item");

            if !self.signature.verify(
                &address,
                &serde_json::to_string(&contribution_file_signature.get_state())?,
                contribution_file_signature.get_signature(),
            ) {
                error!("Contribution file signature failed to verify for {}", participant);
                return Err(CoordinatorError::ContributorSignatureInvalid);
            }

            // Check that the contribution file signature challenge hash is correct.
            if hex::decode(contribution_file_signature.get_challenge_hash())? != challenge_hash.as_slice() {
                error!("The signed challenge hash does not match the expected challenge hash.");
                return Err(CoordinatorError::ContributionHashMismatch);
            }

            // Check that the contribution file signature response hash is correct.
            if hex::decode(contribution_file_signature.get_response_hash())? != response_hash.as_slice() {
                error!("The signed response hash does not match the expected response hash.");
                return Err(CoordinatorError::ContributionHashMismatch);
            }

            // Check that the contribution file signature next challenge hash does not exist.
            if contribution_file_signature.get_next_challenge_hash().is_some() {
                error!("The signed next challenge hash should not exist");
                return Err(CoordinatorError::NextChallengeHashAlreadyExists);
            }
        }

        // Add the contribution response to the current chunk.
        round.chunk_mut(chunk_id)?.add_contribution(
            contribution_id,
            participant,
            self.storage
                .to_path(&Locator::ContributionFile(response_file_locator))?,
            self.storage.to_path(&contribution_file_signature_locator)?,
        )?;

        // Add the updated round to storage.
        match self.storage.update(
            &Locator::RoundState {
                round_height: current_round_height,
            },
            Object::RoundState(round),
        ) {
            Ok(_) => {
                debug!("Updated round {} in storage", current_round_height);
                debug!("{} added a contribution to chunk {}", participant, chunk_id);
                Ok((response_file_locator, contribution_id))
            }
            _ => Err(CoordinatorError::StorageUpdateFailed),
        }
    }

    #[inline]
    pub(crate) fn get_challenge(
        &mut self,
        round_height: u64,
        chunk_id: u64,
        contribution_id: u64,
        is_verified: bool,
    ) -> Result<Vec<u8>, CoordinatorError> {
        // Fetch the challenge, response, and contribution file signature locators.
        let challenge_file_locator = Locator::ContributionFile(ContributionLocator::new(
            round_height,
            chunk_id,
            contribution_id,
            is_verified,
        ));
        // Get the challenge from the challenge file locator
        let challenge_reader = self.storage.reader(&challenge_file_locator)?;
        // let challenge_hash = calculate_hash(challenge_reader.as_ref());

        Ok(challenge_reader.to_vec())
    }

    /// Writes the bytes of a contribution to storage at the appropriate file
    /// locator.
    #[inline]
    pub(crate) fn write_contribution<T>(
        &mut self,
        contribution_locator: ContributionLocator,
        contribution: T,
    ) -> Result<(), CoordinatorError>
    where
        T: Into<Vec<u8>>,
    {
        self.storage.update(
            &Locator::ContributionFile(contribution_locator),
            Object::ContributionFile(contribution.into()),
        )
    }

    /// Writes the bytes of a contribution file signature to storage at the appropriate  
    /// locator.
    #[inline]
    pub(crate) fn write_contribution_file_signature(
        &mut self,
        locator: ContributionSignatureLocator,
        contribution_file_signature: ContributionFileSignature,
    ) -> Result<(), CoordinatorError> {
        self.storage.update(
            &Locator::ContributionFileSignature(locator),
            Object::ContributionFileSignature(contribution_file_signature),
        )
    }

    ///
    /// Attempts to run verification in the current round for a given
    /// chunk ID and participant.
    ///
    /// This function checks that the participant is a verifier and
    /// has uploaded a valid next challenge file to the coordinator.
    /// The coordinator sanity checks that the next challenge file
    /// contains the hash of the corresponding response file.
    ///
    /// This function stores the next challenge locator into the round
    /// transcript and releases the chunk lock from the verifier.
    ///
    /// On success, this function returns the contribution ID of the
    /// unverified response file.
    ///
    /// **Important**: if the contribution is the final contribution
    /// for the chunk for the round, its verification will be stored
    /// in the next round's directory as contribution 0.
    ///
    #[tracing::instrument(
        skip(self, participant),
        fields(participant = %participant)
    )]
    pub(crate) fn verify_contribution(
        &mut self,
        task: &Task,
        participant: &Participant,
    ) -> Result<u64, CoordinatorError> {
        let chunk_id = task.chunk_id();
        debug!("Attempting to verify a contribution for chunk {}", chunk_id);
        if !participant.is_verifier() {
            return Err(CoordinatorError::ExpectedVerifier);
        }

        // Fetch the current round height from storage.
        let current_round_height = Self::load_current_round_height(&self.storage)?;
        trace!("Current round height from storage is {}", current_round_height);

        // Fetch the current round from storage.
        let mut round = Self::load_current_round(&self.storage)?;

        // Fetch the chunk corresponding to the given chunk ID.
        let chunk = round.chunk(chunk_id)?;
        // Fetch the current contribution ID.
        let contribution_id = chunk.current_contribution_id();
        if contribution_id != task.contribution_id() {
            return Err(CoordinatorError::ContributionIdMismatch);
        }

        // Fetch the challenge, response, next challenge, and contribution file signature locators.
        let challenge_file_locator = Locator::ContributionFile(ContributionLocator::new(
            current_round_height,
            chunk_id,
            contribution_id - 1,
            true,
        ));
        let response_file_locator = Locator::ContributionFile(ContributionLocator::new(
            current_round_height,
            chunk_id,
            contribution_id,
            false,
        ));
        let (next_challenge_locator, contribution_file_signature_locator) = {
            // Fetch whether this is the final contribution of the specified chunk.
            let is_final_contribution = chunk.only_contributions_complete(round.expected_number_of_contributions());
            match is_final_contribution {
                true => (
                    Locator::ContributionFile(ContributionLocator::new(current_round_height + 1, chunk_id, 0, true)),
                    Locator::ContributionFileSignature(ContributionSignatureLocator::new(
                        current_round_height + 1,
                        chunk_id,
                        0,
                        true,
                    )),
                ),
                false => (
                    Locator::ContributionFile(ContributionLocator::new(
                        current_round_height,
                        chunk_id,
                        contribution_id,
                        true,
                    )),
                    Locator::ContributionFileSignature(ContributionSignatureLocator::new(
                        current_round_height,
                        chunk_id,
                        contribution_id,
                        true,
                    )),
                ),
            }
        };

        // Check the challenge-response hash chain.
        let (challenge_hash, response_hash) = {
            // Compute the challenge hash using the challenge file.
            let challenge_reader = self.storage.reader(&challenge_file_locator)?;
            let challenge_hash = calculate_hash(challenge_reader.as_ref());
            trace!(
                "Challenge is located in {}",
                self.storage.to_path(&challenge_file_locator)?
            );
            debug!("Challenge hash is {}", pretty_hash!(&challenge_hash.as_slice()));

            // Compute the response hash.
            let response_reader = self.storage.reader(&response_file_locator)?;
            let response_hash = calculate_hash(response_reader.as_ref());
            trace!(
                "Response is located in {}",
                self.storage.to_path(&response_file_locator)?
            );
            debug!("Response hash is {}", pretty_hash!(&response_hash.as_slice()));

            // Fetch the challenge hash from the response file.
            let challenge_hash_in_response = &response_reader
                .get(0..64)
                .ok_or(CoordinatorError::StorageReaderFailed)?[..];
            let pretty_hash = pretty_hash!(&challenge_hash_in_response);

            // Check the starting hash in the response file is based on the challenge.
            info!("The challenge hash is {}", pretty_hash!(&challenge_hash.as_slice()));
            info!("The challenge hash in response file is {}", pretty_hash);
            if challenge_hash_in_response != challenge_hash.as_slice() {
                error!("Challenge hash in response file does not match the expected challenge hash.");
                return Err(CoordinatorError::ContributionHashMismatch);
            }

            (challenge_hash, response_hash)
        };

        // Check the response-next_challenge hash chain.
        let next_challenge_hash = {
            // Compute the next challenge hash.
            let next_challenge_reader = self.storage.reader(&next_challenge_locator)?;
            let next_challenge_hash = calculate_hash(next_challenge_reader.as_ref());
            trace!(
                "Next challenge is located in {}",
                self.storage.to_path(&next_challenge_locator)?
            );
            debug!(
                "Next challenge hash is {}",
                pretty_hash!(&next_challenge_hash.as_slice())
            );

            // Fetch the saved response hash in the next challenge file.
            let saved_response_hash = next_challenge_reader.as_ref().chunks(64).next().unwrap().to_vec();
            let pretty_hash = pretty_hash!(&saved_response_hash);

            // Check that the response hash matches the next challenge hash.
            info!("The response hash is {}", pretty_hash!(&response_hash));
            info!("The response hash in next challenge file is {}", pretty_hash);
            if response_hash.as_slice() != saved_response_hash {
                error!("Response hash does not match the saved response hash.");
                return Err(CoordinatorError::ContributionHashMismatch);
            }

            next_challenge_hash
        };

        // Check the response-next_challenge contribution signature.
        {
            // Fetch the stored contribution file signature.
            let contribution_file_signature: ContributionFileSignature =
                serde_json::from_slice(&*self.storage.reader(&contribution_file_signature_locator)?)?;

            // Check that the contribution file signature is valid.
            let address = &participant.to_string();

            let address = address
                .split(".")
                .next()
                .expect("splitting a string should yield at least one item");

            if !self.signature.verify(
                &address,
                &serde_json::to_string(&contribution_file_signature.get_state())?,
                contribution_file_signature.get_signature(),
            ) {
                error!("Contribution file signature failed to verify for {}", participant);
                return Err(CoordinatorError::VerifierSignatureInvalid);
            }

            // Check that the contribution file signature challenge hash is correct.
            if hex::decode(contribution_file_signature.get_challenge_hash())? != challenge_hash.as_slice() {
                error!("The signed challenge hash does not match the expected challenge hash.");
                return Err(CoordinatorError::ContributionHashMismatch);
            }

            // Check that the contribution file signature response hash is correct.
            if hex::decode(contribution_file_signature.get_response_hash())? != response_hash.as_slice() {
                error!("The signed response hash does not match the expected response hash.");
                return Err(CoordinatorError::ContributionHashMismatch);
            }

            // Check that the contribution file signature next challenge hash exists.
            if contribution_file_signature.get_next_challenge_hash().is_none() {
                error!("The signed next challenge hash is missing");
                return Err(CoordinatorError::NextChallengeHashMissing);
            }

            // Check that the contribution file signature next challenge hash is correct.
            let signed_next_challenge_hash = contribution_file_signature
                .get_next_challenge_hash()
                .as_ref()
                .ok_or(CoordinatorError::NextChallengeHashMissing)?;
            if hex::decode(signed_next_challenge_hash)? != next_challenge_hash.as_slice() {
                error!("The signed next challenge hash does not match the expected next challenge hash.");
                return Err(CoordinatorError::ContributionHashMismatch);
            }
        }

        // Sets the current contribution as verified in the current round.
        round.verify_contribution(
            chunk_id,
            contribution_id,
            participant.clone(),
            self.storage.to_path(&next_challenge_locator)?,
            self.storage.to_path(&contribution_file_signature_locator)?,
        )?;

        // Add the updated round to storage.
        match self.storage.update(
            &Locator::RoundState {
                round_height: current_round_height,
            },
            Object::RoundState(round),
        ) {
            Ok(_) => {
                debug!("Updated round {} in storage", current_round_height);
                debug!(
                    "{} verified chunk {} contribution {}",
                    participant, chunk_id, contribution_id
                );
                Ok(contribution_id)
            }
            _ => Err(CoordinatorError::StorageUpdateFailed),
        }
    }

    ///
    /// Aggregates the contributions for the current round of the ceremony.
    ///
    /// This function loads the current round from storage and checks that
    /// it is fully verified before proceeding to aggregate the round, and
    /// initialize the next round, saving it to storage for the coordinator.
    ///
    /// On success, the function returns the new round height.
    /// Otherwise, it returns a `CoordinatorError`.
    ///
    #[inline]
    pub(crate) fn aggregate_contributions(&mut self) -> Result<(), CoordinatorError> {
        // Fetch the current round height from storage.
        let current_round_height = Self::load_current_round_height(&self.storage)?;

        trace!("Current round height from storage is {}", current_round_height);

        // This function executes aggregation of the current round in preparation
        // for transitioning to the next round. If this is the initial round,
        // there should be nothing to aggregate and we return a `CoordinatorError`.
        if current_round_height == 0 {
            error!("Unauthorized to aggregate round 0");
            return Err(CoordinatorError::RoundDoesNotExist);
        }

        // Check that the current round state exists in storage.
        if !self.storage.exists(&Locator::RoundState {
            round_height: current_round_height,
        }) {
            return Err(CoordinatorError::RoundStateMissing);
        }

        // Check that the next round state does not exist in storage.
        if self.storage.exists(&Locator::RoundState {
            round_height: current_round_height + 1,
        }) {
            return Err(CoordinatorError::RoundShouldNotExist);
        }

        // Check that the current round file does not exist.
        let round_file = Locator::RoundFile {
            round_height: current_round_height,
        };
        if self.storage.exists(&round_file) {
            warn!(
                "Round file locator already exists ({}), removing...",
                self.storage.to_path(&round_file)?
            );
            self.storage.remove(&round_file)?;
        }

        // Fetch the current round from storage.
        let round = Self::load_current_round(&self.storage)?;

        // Check that the final unverified and verified contribution locators exist.
        let contribution_id = round.expected_number_of_contributions() - 1;
        for chunk_id in 0..self.environment.number_of_chunks() {
            // Check that the final unverified contribution locator exists.
            let locator = Locator::ContributionFile(ContributionLocator::new(
                current_round_height,
                chunk_id,
                contribution_id,
                false,
            ));
            if !self.storage.exists(&locator) {
                error!(
                    "Unverified contribution is missing ({})",
                    self.storage.to_path(&locator)?
                );
                return Err(CoordinatorError::ContributionMissing);
            }
            // Check that the final verified contribution locator exists.
            let locator =
                Locator::ContributionFile(ContributionLocator::new(current_round_height + 1, chunk_id, 0, true));
            if !self.storage.exists(&locator) {
                error!("Verified contribution is missing ({})", self.storage.to_path(&locator)?);
                return Err(CoordinatorError::ContributionMissing);
            }
        }

        // Check that all chunks in the current round are verified.
        if !round.is_complete() {
            error!("Round {} is not complete", current_round_height);
            trace!("{:#?}", &round);
            return Err(CoordinatorError::RoundNotComplete);
        }

        // Execute round aggregation and aggregate verification for the current round.
        {
            debug!("Coordinator is starting aggregation and aggregate verification");
            // FIXME: removed aggregation and CoordinatorError::RoundFileMissing. We don't need the aggregation function. Hopefully, it doesn't break anything.
            Aggregation::run(&self.environment, &mut self.storage, &round)?;
            debug!("Coordinator completed aggregation and aggregate verification");
        }

        // Check that the round file for the current round now exists.
        if !self.storage.exists(&round_file) {
            error!("Round file locator is missing ({})", self.storage.to_path(&round_file)?);
            return Err(CoordinatorError::RoundFileMissing);
        }

        Ok(())
    }

    ///
    /// Initiates the next round of the ceremony.
    ///
    /// If there are no prior rounds in storage, this initializes a new ceremony
    /// by invoking `Initialization`, and saves it to storage.
    ///
    /// Otherwise, this loads the current round from storage and checks that
    /// it is fully verified before proceeding to aggregate the round, and
    /// initialize the next round, saving it to storage for the coordinator.
    ///
    /// On success, the function returns the new round height.
    /// Otherwise, it returns a `CoordinatorError`.
    ///
    #[inline]
    pub(crate) fn next_round(
        &mut self,
        started_at: OffsetDateTime,
        contributors: Vec<Participant>,
    ) -> Result<u64, CoordinatorError> {
        // Check that the next round has at least one authorized contributor.
        if contributors.is_empty() {
            return Err(CoordinatorError::ContributorsMissing);
        }

        // Fetch the current round height from storage.
        let current_round_height = Self::load_current_round_height(&self.storage)?;

        trace!("Current round height from storage is {}", current_round_height);

        // Ensure the current round has been aggregated if this is not the initial round.
        if current_round_height != 0 {
            // Check that the round file for the current round exists.
            let round_file = Locator::RoundFile {
                round_height: current_round_height,
            };
            if !self.storage.exists(&round_file) {
                error!("Round file locator is missing ({})", self.storage.to_path(&round_file)?);
                warn!("Coordinator may be missing a call to `try_aggregate` for the current round");
                return Err(CoordinatorError::RoundFileMissing);
            }
            self.aggregate_contributions()?;
        }

        // Create the new round height.
        let new_height = current_round_height + 1;
        info!("Transitioning from round {} to {}", current_round_height, new_height);

        // Check that the new round does not exist in storage.
        // If it exists, this means the round was already initialized.
        let locator = Locator::RoundState {
            round_height: new_height,
        };
        if self.storage.exists(&locator) {
            error!(
                "Round {} already exists ({})",
                new_height,
                self.storage.to_path(&locator)?
            );
            return Err(CoordinatorError::RoundAlreadyInitialized);
        }

        // Check that each contribution for the next round exists.
        for chunk_id in 0..self.environment.number_of_chunks() {
            debug!("Locating round {} chunk {} contribution 0", new_height, chunk_id);
            let locator = Locator::ContributionFile(ContributionLocator::new(new_height, chunk_id, 0, true));
            if !self.storage.exists(&locator) {
                error!("Contribution locator is missing ({})", self.storage.to_path(&locator)?);
                return Err(CoordinatorError::ContributionLocatorMissing);
            }
        }

        // Instantiate the new round and height.
        let new_round = Round::new(
            &self.environment,
            &mut self.storage,
            new_height,
            started_at,
            contributors,
        )?;

        #[cfg(test)]
        trace!("{:#?}", &new_round);

        // Insert the new round into storage.
        self.storage.insert(
            Locator::RoundState {
                round_height: new_height,
            },
            Object::RoundState(new_round),
        )?;

        // Next, update the round height to reflect the new round.
        self.storage
            .update(&Locator::RoundHeight, Object::RoundHeight(new_height))?;

        debug!("Added round {} to storage", current_round_height);
        info!("Transitioned from round {} to {}", current_round_height, new_height);
        Ok(new_height)
    }

    ///
    /// Attempts to run initialization for the ceremony.
    ///
    /// If this is the initial round, ensure the round does not exist yet.
    ///
    /// If there is no round in storage, proceed to create a new round instance,
    /// and run `Initialization` to start the ceremony.
    ///
    #[inline]
    pub(super) fn run_initialization(&mut self, started_at: OffsetDateTime) -> Result<u64, CoordinatorError> {
        // Check that the ceremony has not begun yet.
        if Self::load_current_round_height(&self.storage).is_ok() {
            return Err(CoordinatorError::RoundAlreadyInitialized);
        }

        // Establish the round height convention for initialization.
        let round_height = 0;

        // Check that the current round does not exist in storage.
        if self.storage.exists(&Locator::RoundState { round_height }) {
            return Err(CoordinatorError::RoundShouldNotExist);
        }

        // Check that the next round does not exist in storage.
        if self.storage.exists(&Locator::RoundState {
            round_height: round_height + 1,
        }) {
            return Err(CoordinatorError::RoundShouldNotExist);
        }

        // Create an instantiation of `Round` for round 0.
        let mut round = {
            // Initialize the contributors as an empty list as this is for initialization.
            let contributors = vec![];

            // Create a new round instance.
            Round::new(
                &self.environment,
                &mut self.storage,
                round_height,
                started_at,
                contributors,
            )?
        };

        info!("Starting initialization of round {}", round_height);

        // Execute initialization of contribution 0 for all chunks
        // in the new round and check that the new locators exist.
        for chunk_id in 0..self.environment.number_of_chunks() {
            // 1 - Check that the contribution locator corresponding to this round's chunk does not exist.
            let locator = Locator::ContributionFile(ContributionLocator::new(round_height, chunk_id, 0, true));
            if self.storage.exists(&locator) {
                error!(
                    "Contribution locator already exists ({})",
                    self.storage.to_path(&locator)?
                );
                return Err(CoordinatorError::ContributionLocatorAlreadyExists);
            }

            // 2 - Check that the contribution locator corresponding to the next round's chunk does not exists.
            let locator = Locator::ContributionFile(ContributionLocator::new(round_height + 1, chunk_id, 0, true));
            if self.storage.exists(&locator) {
                error!(
                    "Contribution locator already exists ({})",
                    self.storage.to_path(&locator)?
                );
                return Err(CoordinatorError::ContributionLocatorAlreadyExists);
            }

            info!("Coordinator is starting initialization on chunk {}", chunk_id);
            let _contribution_hash = Initialization::run(&self.environment, &mut self.storage, round_height, chunk_id)?;
            info!("Coordinator completed initialization on chunk {}", chunk_id);

            // 1 - Check that the contribution locator corresponding to this round's chunk now exists.
            let locator = Locator::ContributionFile(ContributionLocator::new(round_height, chunk_id, 0, true));
            if !self.storage.exists(&locator) {
                error!("Contribution locator is missing ({})", self.storage.to_path(&locator)?);
                return Err(CoordinatorError::ContributionLocatorMissing);
            }

            // 2 - Check that the contribution locator corresponding to the next round's chunk now exists.
            let locator = Locator::ContributionFile(ContributionLocator::new(round_height + 1, chunk_id, 0, true));
            if !self.storage.exists(&locator) {
                error!("Contribution locator is missing ({})", self.storage.to_path(&locator)?);
                return Err(CoordinatorError::ContributionLocatorMissing);
            }
        }

        // Set the finished time for round 0.
        round.try_finish(self.time.now_utc());

        // Add the new round to storage.
        self.storage
            .insert(Locator::RoundState { round_height }, Object::RoundState(round))?;

        // Next, add the round height to storage.
        self.storage
            .insert(Locator::RoundHeight, Object::RoundHeight(round_height))?;

        info!("Completed initialization of round {}", round_height);

        Ok(round_height)
    }

    /// Update the round on disk after a drop has occured.
    #[inline]
    fn drop_participant_from_storage(&mut self, drop: &DropParticipant) -> Result<(), CoordinatorError> {
        debug!(
            "Dropping participant from storage with the following information: {:#?}",
            drop
        );

        // Check the justification and extract the tasks.
        let drop_data = match drop {
            DropParticipant::DropCurrent(data) => data,
            DropParticipant::DropQueue(_) => {
                // Participant is not part of the round, therefore
                // there is nothing to do.
                return Ok(());
            }
        };

        match &drop_data.storage_action {
            CeremonyStorageAction::ResetCurrentRound(reset_action) => {
                self.reset_round_storage(reset_action)?;
            }
            CeremonyStorageAction::ReplaceContributor(replace_action) => {
                warn!(
                    "Replacing contributor {} with {}",
                    replace_action.dropped_contributor, replace_action.replacement_contributor
                );

                // Fetch the current round from storage.
                let mut round = Self::load_current_round(&self.storage)?;

                // Remove the contributor from the round self.
                round.remove_contributor_unsafe(
                    &mut self.storage,
                    &replace_action.dropped_contributor,
                    &replace_action.locked_chunks,
                    &replace_action.tasks,
                )?;

                // Assign a replacement contributor to the dropped tasks for the current round.
                round.add_replacement_contributor_unsafe(replace_action.replacement_contributor.clone())?;
                warn!(
                    "Added a replacement contributor {}",
                    replace_action.replacement_contributor
                );

                // Save the updated round to storage.
                self.storage.update(
                    &Locator::RoundState {
                        round_height: round.round_height(),
                    },
                    Object::RoundState(round),
                )?;
            }
        }

        Ok(())
    }

    #[inline]
    fn load_current_round_height(storage: &Disk) -> Result<u64, CoordinatorError> {
        if storage.exists(&Locator::RoundHeight) {
            // Fetch the current round height from storage.
            match storage.get(&Locator::RoundHeight)? {
                // Case 1 - This is a typical round of the ceremony.
                Object::RoundHeight(current_round_height) => Ok(current_round_height),
                // Case 2 - Storage failed to fetch the round height.
                _ => Err(CoordinatorError::StorageFailed),
            }
        } else {
            Err(CoordinatorError::RoundHeightNotSet)
        }
    }

    #[inline]
    fn load_current_round(storage: &Disk) -> Result<Round, CoordinatorError> {
        // Fetch the current round height from storage.
        let current_round_height = Self::load_current_round_height(storage)?;

        // Fetch the current round from storage.
        Self::load_round(storage, current_round_height)
    }

    #[inline]
    fn load_round(storage: &Disk, round_height: u64) -> Result<Round, CoordinatorError> {
        // Fetch the current round height from storage.
        let current_round_height = Self::load_current_round_height(storage)?;

        // Fetch the specified round from storage.
        match round_height <= current_round_height {
            // Load the corresponding round data from storage.
            true => match storage.get(&Locator::RoundState { round_height })? {
                // Case 1 - The ceremony is running and the round state was fetched.
                Object::RoundState(round) => Ok(round),
                // Case 2 - Storage failed to fetch the round height.
                _ => Err(CoordinatorError::StorageFailed),
            },
            // Case 3 - There are no prior rounds of the ceremony.
            false => Err(CoordinatorError::RoundDoesNotExist),
        }
    }

    ///
    /// Returns a reference to the instantiation of `Storage` that this
    /// coordinator is using.
    ///
    #[cfg(any(test, testing))]
    #[inline]
    pub(super) fn storage(&self) -> &Disk {
        &self.storage
    }

    ///
    /// Returns a mutable reference to the instantiation of `Storage`
    /// that this coordinator is using.
    ///
    #[cfg(test)]
    #[inline]
    pub(super) fn storage_mut(&mut self) -> &mut Disk {
        &mut self.storage
    }

    ///
    /// Returns a reference to the instantiation of `Signature` that this
    /// coordinator is using.
    ///
    #[cfg(test)]
    #[inline]
    pub(super) fn signature(&self) -> Arc<dyn Signature> {
        self.signature.clone()
    }

    ///
    /// Resets the current round, with a rollback to the end of the
    /// previous round to invite new participants into the round.
    ///
    pub fn reset_round(&mut self) -> Result<(), CoordinatorError> {
        let reset_action = self.state.reset_current_round(true, &*self.time)?;

        self.storage
            .update(&Locator::CoordinatorState, Object::CoordinatorState(self.state.clone()))?;
        self.reset_round_storage(&reset_action)?;

        Ok(())
    }

    /// Reset the current round in storage.
    ///
    /// + `remove_participants` is a list of participants that will
    ///   have their contributions removed from the round.
    /// + If `rollback` is set to `true`, the current round will be
    ///   decremented by 1. If `rollback` is set to `true` and the
    ///   current round height is `0` then this will return an error
    ///   [CoordinatorError::RoundHeightIsZero]
    fn reset_round_storage(&mut self, reset_action: &ResetCurrentRoundStorageAction) -> Result<(), CoordinatorError> {
        // Fetch the current height of the ceremony.
        let current_round_height = Self::load_current_round_height(&mut self.storage)?;
        let span = tracing::error_span!("reset_round", round = current_round_height);
        let _guard = span.enter();
        warn!("Resetting round {}", current_round_height);

        let mut round = Self::load_round(&mut self.storage, current_round_height)?;

        tracing::debug!("Resetting round and applying storage changes");
        self.storage.process(round.reset(&reset_action.remove_participants))?;

        // Clear all files
        self.storage
            .process(StorageAction::ClearRoundFiles(current_round_height))?;

        if reset_action.rollback {
            if current_round_height == 0 {
                return Err(CoordinatorError::RoundHeightIsZero);
            }

            let new_round_height = current_round_height - 1;
            tracing::debug!("Rolling back to round {} in storage.", new_round_height);

            self.storage.remove(&Locator::RoundState {
                round_height: current_round_height,
            })?;
            self.storage
                .update(&Locator::RoundHeight, Object::RoundHeight(new_round_height))?;
        }

        warn!("Finished resetting round {} storage", current_round_height);

        Ok(())
    }
}

#[cfg(any(test, feature = "operator"))]
use crate::commands::{Computation, Seed, SigningKey, Verification};

#[cfg(any(test, feature = "operator"))]
impl Coordinator {
    #[tracing::instrument(
        skip(self, contributor, contributor_signing_key, contributor_seed),
        fields(contributor = %contributor),
    )]
    pub fn contribute(
        &mut self,
        contributor: &Participant,
        contributor_signing_key: &SigningKey,
        contributor_seed: &Seed,
    ) -> Result<(), CoordinatorError> {
        let (_chunk_id, locked_locators) = self.try_lock(contributor)?;
        let response_locator = locked_locators.next_contribution();
        let round_height = response_locator.round_height();
        let chunk_id = response_locator.chunk_id();
        let contribution_id = response_locator.contribution_id();

        // check that the response matches a pending task
        {
            let response_task = Task::new(chunk_id, contribution_id);
            let info = self.state.current_participant_info(contributor).unwrap().clone();

            if info.pending_tasks().is_empty() {
                return Err(CoordinatorError::ContributorPendingTasksCannotBeEmpty(
                    contributor.clone(),
                ));
            }

            if !info
                .pending_tasks()
                .iter()
                .find(|pending_task| pending_task == &&response_task)
                .is_some()
            {
                return Err(CoordinatorError::PendingTasksMustContainResponseTask { response_task });
            }
        }

        debug!("Computing contributions for round {} chunk {}", round_height, chunk_id);
        self.run_computation(
            round_height,
            chunk_id,
            contribution_id,
            contributor,
            contributor_signing_key,
            contributor_seed,
        )?;
        let _response = self.try_contribute(contributor, chunk_id)?;
        debug!(
            "Successfully computed contribution for round {} chunk {}",
            round_height, chunk_id
        );
        Ok(())
    }

    pub fn get_pending_verifications(&self) -> &HashMap<Task, Participant> {
        self.state.get_pending_verifications()
    }

    /// Verify a contribution using the coordinator's default verifier.
    /// This is just an interface to [`verify`]
    ///
    /// #Error
    /// This function assumes that the given task has been indeed assigned to the
    /// default verifier.
    pub fn default_verify(&mut self, task: &Task) -> anyhow::Result<()> {
        let verifier = self.environment.coordinator_verifiers()[0].clone();
        let sigkey = self.environment.default_verifier_signing_key();

        self.verify(&verifier, &sigkey, task)
    }

    #[tracing::instrument(
        skip(self, verifier, verifier_signing_key),
        fields(verifier = %verifier),
    )]
    pub fn verify(
        &mut self,
        verifier: &Participant,
        verifier_signing_key: &SigningKey,
        task: &Task,
    ) -> anyhow::Result<()> {
        let round_height = self.current_round_height()?;
        debug!(
            "Running verification for round {} chunk {}",
            round_height,
            task.chunk_id()
        );
        let _next_challenge = self.run_verification(round_height, task, verifier, verifier_signing_key)?;
        self.try_verify(verifier, task)?;
        debug!(
            "Successful verification for round {} chunk {}",
            round_height,
            task.chunk_id()
        );
        Ok(())
    }

    ///
    /// Attempts to run computation for a given round height, given chunk ID, and contribution ID.
    ///
    /// This function is used for testing purposes and can also be used for completing contributions
    /// of participants who may have dropped off and handed over control of their session.
    ///
    #[inline]
    pub fn run_computation(
        &mut self,
        round_height: u64,
        chunk_id: u64,
        contribution_id: u64,
        participant: &Participant,
        participant_signing_key: &SigningKey,
        participant_seed: &Seed,
    ) -> Result<(), CoordinatorError> {
        info!(
            "Running computation for round {} chunk {} contribution {} as {}",
            round_height, chunk_id, contribution_id, participant
        );

        // Check that the chunk ID is valid.
        if chunk_id > self.environment.number_of_chunks() {
            return Err(CoordinatorError::ChunkIdInvalid);
        }

        // Check that the contribution ID is valid.
        if contribution_id == 0 {
            return Err(CoordinatorError::ContributionIdMustBeNonzero);
        }

        // Check that the participant is a contributor.
        if !participant.is_contributor() {
            return Err(CoordinatorError::ExpectedContributor);
        }

        // Fetch the specified round from storage.
        let round = Self::load_round(&mut self.storage, round_height)?;

        // Check that the chunk lock is currently held by this contributor.
        if !round.is_chunk_locked_by(chunk_id, &participant) {
            error!("{} should have lock on chunk {} but does not", &participant, chunk_id);
            return Err(CoordinatorError::ChunkNotLockedOrByWrongParticipant);
        }

        // Check that the given contribution ID does not exist yet.
        if round.chunk(chunk_id)?.get_contribution(contribution_id).is_ok() {
            return Err(CoordinatorError::ContributionShouldNotExist);
        }

        // Fetch the challenge locator.
        let challenge_locator = &Locator::ContributionFile(ContributionLocator::new(
            round_height,
            chunk_id,
            contribution_id - 1,
            true,
        ));
        // Fetch the response locator.
        let response_locator =
            &Locator::ContributionFile(ContributionLocator::new(round_height, chunk_id, contribution_id, false));
        // Fetch the contribution file signature locator.
        let contribution_file_signature_locator = &Locator::ContributionFileSignature(
            ContributionSignatureLocator::new(round_height, chunk_id, contribution_id, false),
        );

        info!(
            "Starting computation on round {} chunk {} contribution {} as {}",
            round_height, chunk_id, contribution_id, participant
        );
        Computation::run(
            &self.environment,
            &mut self.storage,
            self.signature.clone(),
            participant_signing_key,
            challenge_locator,
            response_locator,
            contribution_file_signature_locator,
            participant_seed,
        )?;
        info!(
            "Completed computation on round {} chunk {} contribution {} as {}",
            round_height, chunk_id, contribution_id, participant
        );

        Ok(())
    }

    ///
    /// Attempts to run verification for a given round height, given chunk ID, and contribution ID.
    ///
    /// On success, this function returns the verified contribution locator.
    ///
    /// This function copies the unverified contribution into the verified contribution locator,
    /// which is the next contribution ID within a round, or the next round height if this round
    /// is complete.
    ///
    #[inline]
    pub fn run_verification(
        &mut self,
        round_height: u64,
        task: &Task,
        participant: &Participant,
        participant_signing_key: &SigningKey,
    ) -> Result<LocatorPath, CoordinatorError> {
        let chunk_id = task.chunk_id();
        let contribution_id = task.contribution_id();
        info!(
            "Running verification for round {} chunk {} contribution {} as {}",
            round_height, chunk_id, contribution_id, participant
        );

        // Check that the chunk ID is valid.
        if chunk_id > self.environment.number_of_chunks() {
            return Err(CoordinatorError::ChunkIdInvalid);
        }

        // Check that the contribution ID is valid.
        if contribution_id == 0 {
            return Err(CoordinatorError::ContributionIdMustBeNonzero);
        }

        // Check that the participant is a verifier.
        if !participant.is_verifier() {
            return Err(CoordinatorError::ExpectedContributor);
        }

        // Fetch the specified round from storage.
        let round = Self::load_round(&self.storage, round_height)?;

        // Check that the contribution locator corresponding to the response file exists.
        let response_locator =
            Locator::ContributionFile(ContributionLocator::new(round_height, chunk_id, contribution_id, false));
        if !self.storage.exists(&response_locator) {
            error!(
                "Response file at {} is missing",
                self.storage.to_path(&response_locator)?
            );
            return Err(CoordinatorError::ContributionLocatorMissing);
        }

        // Fetch the chunk corresponding to the given chunk ID.
        let chunk = round.chunk(chunk_id)?;

        // Chat that the specified contribution ID has NOT been verified yet.
        if chunk.get_contribution(contribution_id)?.is_verified() {
            return Err(CoordinatorError::ContributionAlreadyVerified);
        }

        // Fetch whether this is the final contribution of the specified chunk.
        let is_final_contribution = chunk.only_contributions_complete(round.expected_number_of_contributions());
        info!(
            "EXPECTED NUMBER OF CONTRIBUTIONS: {}",
            round.expected_number_of_contributions()
        );

        // Fetch the verified response locator and the contribution file signature locator.
        let verified_locator = match is_final_contribution {
            true => Locator::ContributionFile(ContributionLocator::new(round_height + 1, chunk_id, 0, true)),
            false => Locator::ContributionFile(ContributionLocator::new(round_height, chunk_id, contribution_id, true)),
        };

        info!(
            "Starting verification on round {} chunk {} contribution {} as {}",
            round_height, chunk_id, contribution_id, participant
        );
        Verification::run(
            &self.environment,
            &mut self.storage,
            self.signature.clone(),
            participant_signing_key,
            round_height,
            chunk_id,
            contribution_id,
            is_final_contribution,
        )?;
        info!(
            "Completed verification on round {} chunk {} contribution {} as {}",
            round_height, chunk_id, contribution_id, participant
        );

        // Check that the verified contribution locator exists.
        if !self.storage.exists(&verified_locator) {
            let verified_response = self.storage.to_path(&verified_locator)?;
            error!("Verified response file at {} is missing", verified_response);
            return Err(CoordinatorError::ContributionLocatorMissing);
        }

        Ok(self.storage.to_path(&verified_locator)?)
    }

    ///
    /// Returns a reference to the instantiation of `CoordinatorState` that this
    /// coordinator is using.
    ///
    #[inline]
    pub fn state(&self) -> CoordinatorState {
        // Clone the state struct.
        self.state.clone()
    }

    ///
    /// Returns a reference to the instantiation of `Environment` that this
    /// coordinator is using.
    ///
    #[inline]
    pub fn environment(&self) -> &Environment {
        &self.environment
    }

    ///
    /// Rollback a task which was locked by a contributor. Should be used to unlock
    /// chunks which become stuck during the ceremony.
    ///
    pub fn rollback_locked_task(&mut self, participant: &Participant, task: Task) -> Result<(), CoordinatorError> {
        self.state.rollback_locked_task(participant, task, &*self.time)?;
        self.save_state()?;

        let mut round = self.current_round()?;
        round.remove_locks_unsafe(&mut self.storage, participant, &[task.chunk_id()])?;

        Ok(self.storage.process(StorageAction::Update(UpdateAction {
            locator: Locator::RoundState {
                round_height: self.current_round_height()?,
            },
            object: Object::RoundState(round),
        }))?)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        authentication::Dummy,
        commands::{Seed, SigningKey, SEED_LENGTH},
        environment::*,
        objects::{Participant, Task},
        testing::prelude::*,
        Coordinator,
    };

    use once_cell::sync::Lazy;
    use rand::RngCore;
    use std::{
        collections::HashMap,
        net::{IpAddr, Ipv4Addr},
        sync::Arc,
    };
    use time::OffsetDateTime;

    fn initialize_to_round_1(
        coordinator: &mut Coordinator,
        contributors: &[(Participant, IpAddr)],
    ) -> anyhow::Result<()> {
        // Initialize the ceremony and add the contributors and verifiers to the queue.
        {
            // Run initialization.
            info!("Initializing ceremony");
            let round_height = coordinator.run_initialization(*TEST_STARTED_AT)?;
            info!("Initialized ceremony");

            // Check current round height is now 0.
            assert_eq!(0, round_height);

            // Initialize the coordinator state to round 0.
            coordinator.state.initialize(round_height);
            coordinator.state.save(&mut coordinator.storage)?;

            // Add the contributor and verifier of the coordinator to execute round 1.
            for (contributor, contributor_ip) in contributors {
                coordinator.state.add_to_queue(
                    contributor.clone(),
                    Some(*contributor_ip),
                    10,
                    coordinator.time.as_ref(),
                )?;
            }

            // Update the queue.
            coordinator.state.update_queue()?;
            coordinator.state.save(&mut coordinator.storage)?;
        }

        info!("Advancing ceremony to round 1");
        coordinator.try_advance(*TEST_STARTED_AT)?;
        info!("Advanced ceremony to round 1");

        // Check current round height is now 1.
        assert_eq!(1, coordinator.current_round_height()?);

        // info!("Add contributions and verifications for round 1");
        // for _ in 0..coordinator.environment.number_of_chunks() {
        //     for contributor in contributors {
        //         coordinator.contribute(contributor)?;
        //     }
        //     for verifier in verifiers {
        //         coordinator.verify(verifier)?;
        //     }
        // }
        // info!("Added contributions and verifications for round 1");

        Ok(())
    }

    fn initialize_coordinator(coordinator: &mut Coordinator) -> anyhow::Result<()> {
        // Load the contributors and verifiers.
        let contributors = vec![
            Lazy::force(&TEST_CONTRIBUTOR_ID).clone(),
            Lazy::force(&TEST_CONTRIBUTOR_ID_2).clone(),
        ];

        let contributor_ips = vec![
            IpAddr::V4("0.0.0.1".parse().unwrap()),
            IpAddr::V4("0.0.0.2".parse().unwrap()),
        ];

        let contributors: Vec<(Participant, IpAddr)> = contributors.into_iter().zip(contributor_ips).collect();

        initialize_to_round_1(coordinator, &contributors)
    }

    fn initialize_coordinator_single_contributor(coordinator: &mut Coordinator) -> anyhow::Result<()> {
        // Load the contributors and verifiers.
        let contributors = vec![(
            Lazy::force(&TEST_CONTRIBUTOR_ID).clone(),
            IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        )];

        initialize_to_round_1(coordinator, &contributors)
    }

    #[test]
    #[serial]
    fn coordinator_initialization_matches_json() -> anyhow::Result<()> {
        initialize_test_environment(&TEST_ENVIRONMENT);

        let mut coordinator = Coordinator::new(TEST_ENVIRONMENT.clone(), Arc::new(Dummy))?;
        initialize_coordinator(&mut coordinator)?;

        // Check that round 0 matches the round 0 JSON specification.
        {
            let now = OffsetDateTime::now_utc();

            // Fetch round 0 from coordinator.
            let mut expected = test_round_0_json()?;
            let mut candidate = coordinator.get_round(0)?;

            // Set the finish time to match.
            println!("{} {}", expected.is_complete(), candidate.is_complete());
            expected.try_finish_testing_only_unsafe(now.clone());
            candidate.try_finish_testing_only_unsafe(now);

            print_diff(&expected, &candidate);
            assert_eq!(expected, candidate);
        }

        Ok(())
    }

    #[test]
    #[serial]
    fn coordinator_initialization() -> anyhow::Result<()> {
        initialize_test_environment(&TEST_ENVIRONMENT_ANOMA);

        let mut coordinator = Coordinator::new(TEST_ENVIRONMENT_ANOMA.clone(), Arc::new(Dummy))?;

        {
            // Run initialization.
            info!("Initializing ceremony");
            let round_height = coordinator.run_initialization(*TEST_STARTED_AT).unwrap();
            info!("Initialized ceremony");

            // Check current round height is now 0.
            assert_eq!(0, round_height);

            // Load the contributors and verifiers.
            let contributors = vec![
                Lazy::force(&TEST_CONTRIBUTOR_ID).clone(),
                Lazy::force(&TEST_CONTRIBUTOR_ID_2).clone(),
            ];

            // Start round 1.
            coordinator.next_round(*TEST_STARTED_AT, contributors).unwrap();
        }

        {
            // Check round 0 is complete.
            assert!(coordinator.get_round(0)?.is_complete());

            // Check current round height is now 1.
            assert_eq!(1, coordinator.current_round_height()?);

            // Check that the coordinator participants are established correctly.
            assert!(coordinator.is_coordinator_contributor(&TEST_CONTRIBUTOR_ID));
            assert!(coordinator.is_coordinator_verifier(&TEST_VERIFIER_ID));

            // Check the current round has a matching height.
            let current_round = coordinator.current_round()?;
            assert_eq!(coordinator.current_round_height()?, current_round.round_height());

            // Check round 1 contributors.
            assert_eq!(2, current_round.number_of_contributors());
            assert!(current_round.is_contributor(&TEST_CONTRIBUTOR_ID));
            assert!(current_round.is_contributor(&TEST_CONTRIBUTOR_ID_2));
            assert!(!current_round.is_contributor(&TEST_CONTRIBUTOR_ID_3));
            assert!(!current_round.is_contributor(&TEST_VERIFIER_ID));

            // Check round 1 verifiers.
            assert_eq!(0, current_round.number_of_verifiers());

            // Check round 1 is NOT complete.
            assert!(!current_round.is_complete());
        }

        Ok(())
    }

    #[test]
    #[serial]
    fn coordinator_contributor_try_lock_chunk() -> anyhow::Result<()> {
        initialize_test_environment(&TEST_ENVIRONMENT_ANOMA);

        let contributor = Lazy::force(&TEST_CONTRIBUTOR_ID);
        let contributor_2 = Lazy::force(&TEST_CONTRIBUTOR_ID_2);

        let mut coordinator = Coordinator::new(TEST_ENVIRONMENT_ANOMA.clone(), Arc::new(Dummy))?;
        initialize_coordinator(&mut coordinator)?;

        {
            // Acquire the lock for chunk 0 as contributor 1.
            assert!(coordinator.try_lock_chunk(0, &contributor).is_ok());

            // Attempt to acquire the lock for chunk 0 as contributor 1 again.
            assert!(coordinator.try_lock_chunk(0, &contributor).is_err());

            // Acquire the lock for chunk 1 as contributor 1.
            assert!(coordinator.try_lock_chunk(1, &contributor).is_ok());

            // Attempt to acquire the lock for chunk 0 as contributor 2.
            assert!(coordinator.try_lock_chunk(0, &contributor_2).is_err());

            // Attempt to acquire the lock for chunk 1 as contributor 2.
            assert!(coordinator.try_lock_chunk(1, &contributor_2).is_err());

            // Acquire the lock for chunk 1 as contributor 2.
            assert!(coordinator.try_lock_chunk(2, &contributor_2).is_ok());
        }

        {
            // Check that chunk 0 is locked.
            let round = coordinator.current_round()?;
            let chunk = round.chunk(0)?;
            assert!(chunk.is_locked());
            assert!(!chunk.is_unlocked());

            // Check that chunk 0 is locked by contributor 1.
            assert!(chunk.is_locked_by(contributor));
            assert!(!chunk.is_locked_by(contributor_2));

            // Check that chunk 1 is locked.
            let chunk = round.chunk(1)?;
            assert!(chunk.is_locked());
            assert!(!chunk.is_unlocked());

            // Check that chunk 1 is locked by contributor 1.
            assert!(chunk.is_locked_by(contributor));
            assert!(!chunk.is_locked_by(contributor_2));

            // Check that chunk 2 is locked.
            let chunk = round.chunk(2)?;
            assert!(chunk.is_locked());
            assert!(!chunk.is_unlocked());

            // Check that chunk 2 is locked by contributor 2.
            assert!(chunk.is_locked_by(contributor_2));
            assert!(!chunk.is_locked_by(contributor));
        }

        Ok(())
    }

    #[test]
    #[serial]
    fn coordinator_contributor_add_contribution() -> anyhow::Result<()> {
        initialize_test_environment(&TEST_ENVIRONMENT_ANOMA);

        let contributor = Lazy::force(&TEST_CONTRIBUTOR_ID).clone();
        let contributor_signing_key: SigningKey = "secret_key".to_string();

        let mut coordinator = Coordinator::new(TEST_ENVIRONMENT_ANOMA.clone(), Arc::new(Dummy))?;
        initialize_coordinator(&mut coordinator)?;

        {
            // Acquire the lock for chunk 0 as contributor 1.
            coordinator.try_lock_chunk(0, &contributor)?;
        }

        // Run computation on round 1 chunk 0 contribution 1.
        {
            // Check current round is 1.
            let round = coordinator.current_round()?;
            let round_height = round.round_height();
            assert_eq!(1, round_height);

            // Check chunk 0 is not verified.
            let chunk_id = 0;
            let chunk = round.chunk(chunk_id)?;
            assert!(!chunk.is_complete(round.expected_number_of_contributions()));

            // Check next contribution is 1.
            let contribution_id = 1;
            assert!(chunk.is_next_contribution_id(contribution_id, round.expected_number_of_contributions()));

            // Run the computation
            let mut seed: Seed = [0; SEED_LENGTH];
            rand::thread_rng().fill_bytes(&mut seed[..]);
            assert!(
                coordinator
                    .run_computation(
                        round_height,
                        chunk_id,
                        contribution_id,
                        &contributor,
                        &contributor_signing_key,
                        &seed
                    )
                    .is_ok()
            );
        }

        // Add contribution for round 1 chunk 0 contribution 1.
        let chunk_id = 0;
        {
            // Add round 1 chunk 0 contribution 1.
            assert!(coordinator.add_contribution(chunk_id, &contributor).is_ok());
        }
        {
            // Check chunk 0 lock is released.
            let round = coordinator.current_round()?;
            let chunk = round.chunk(chunk_id)?;
            assert!(chunk.is_unlocked());
            assert!(!chunk.is_locked());
        }

        Ok(())
    }

    #[test]
    #[serial]
    fn coordinator_verifier_verify_contribution() -> anyhow::Result<()> {
        initialize_test_environment(&TEST_ENVIRONMENT_ANOMA);

        let contributor = Lazy::force(&TEST_CONTRIBUTOR_ID);
        let contributor_signing_key: SigningKey = "secret_key".to_string();

        let mut coordinator = Coordinator::new(TEST_ENVIRONMENT_ANOMA.clone(), Arc::new(Dummy))?;
        initialize_coordinator(&mut coordinator)?;

        // Check current round height is now 1.
        let round_height = coordinator.current_round_height()?;
        assert_eq!(1, round_height);

        // Acquire the lock for chunk 0 as contributor 1.
        let chunk_id = 0;
        let contribution_id = 1;
        {
            assert!(coordinator.try_lock_chunk(chunk_id, &contributor).is_ok());
        }
        {
            // Run computation on round 1 chunk 0 contribution 1.
            let mut seed: Seed = [0; SEED_LENGTH];
            rand::thread_rng().fill_bytes(&mut seed[..]);
            assert!(
                coordinator
                    .run_computation(
                        round_height,
                        chunk_id,
                        contribution_id,
                        contributor,
                        &contributor_signing_key,
                        &seed
                    )
                    .is_ok()
            );

            // Add round 1 chunk 0 contribution 1.
            assert!(coordinator.add_contribution(chunk_id, &contributor).is_ok());
        }

        // Verify round 1 chunk 0 contribution 1.
        {
            let verifier = Lazy::force(&TEST_VERIFIER_ID).clone();
            let verifier_signing_key: SigningKey = "secret_key".to_string();

            // Run verification.
            let task = Task::new(chunk_id, contribution_id);
            let verify = coordinator.run_verification(round_height, &task, &verifier, &verifier_signing_key);
            assert!(verify.is_ok());
            // Verify contribution 1.
            coordinator.verify_contribution(&task, &verifier)?;
        }

        Ok(())
    }

    #[test]
    #[serial]
    // This test runs a round with a single coordinator and single verifier
    // The verifier instances are run on a separate thread to simulate an environment where
    // verification and contribution happen concurrently.
    fn coordinator_concurrent_contribution_verification() -> anyhow::Result<()> {
        initialize_test_environment(&TEST_ENVIRONMENT_3);

        let mut coordinator = Coordinator::new(TEST_ENVIRONMENT_3.clone(), Arc::new(Dummy))?;
        initialize_coordinator_single_contributor(&mut coordinator)?;

        // Check current round height is now 1.
        let round_height = coordinator.current_round_height()?;
        assert_eq!(1, round_height);

        let coordinator = std::sync::Arc::new(std::sync::RwLock::new(coordinator));

        let contributor = Lazy::force(&TEST_CONTRIBUTOR_ID).clone();
        let contributor_signing_key: SigningKey = "secret_key".to_string();

        let verifier = Lazy::force(&TEST_VERIFIER_ID);

        let mut verifier_threads = vec![];

        let contribution_id = 1;

        let mut seed: Seed = [0; SEED_LENGTH];
        rand::thread_rng().fill_bytes(&mut seed[..]);

        for chunk_id in 0..TEST_ENVIRONMENT_3.number_of_chunks() {
            {
                // Acquire the lock as contributor.
                let try_lock = coordinator.write().unwrap().try_lock_chunk(chunk_id, &contributor);
                if try_lock.is_err() {
                    println!(
                        "Failed to acquire lock for chunk {} as contributor {:?}\n{}",
                        chunk_id,
                        &contributor,
                        serde_json::to_string_pretty(&coordinator.read().unwrap().current_round()?)?
                    );
                    try_lock?;
                }
            }
            {
                // Run computation as contributor.
                let contribute = coordinator.write().unwrap().run_computation(
                    round_height,
                    chunk_id,
                    contribution_id,
                    &contributor,
                    &contributor_signing_key,
                    &seed,
                );
                if contribute.is_err() {
                    println!(
                        "Failed to run computation for chunk {} as contributor {:?}\n{}",
                        chunk_id,
                        &contributor,
                        serde_json::to_string_pretty(&coordinator.read().unwrap().current_round()?)?
                    );
                    contribute?;
                }

                // Add the contribution as the contributor.
                let contribute = coordinator.write().unwrap().add_contribution(chunk_id, &contributor);
                if contribute.is_err() {
                    println!(
                        "Failed to add contribution for chunk {} as contributor {:?}\n{}",
                        chunk_id,
                        &contributor,
                        serde_json::to_string_pretty(&coordinator.read().unwrap().current_round()?)?
                    );
                    contribute?;
                }
            }

            // Spawn a thread to concurrently verify the contributions.
            let coordinator_clone = coordinator.clone();

            let verifier_thread = std::thread::spawn(move || {
                let verifier = verifier.clone();
                let verifier_signing_key: SigningKey = "secret_key".to_string();

                {
                    // Run verification as verifier.
                    let task = Task::new(chunk_id, contribution_id);
                    let verify = coordinator_clone.write().unwrap().run_verification(
                        round_height,
                        &task,
                        &verifier,
                        &verifier_signing_key,
                    );
                    if verify.is_err() {
                        error!(
                            "Failed to run verification as verifier {:?}\n{}",
                            &verifier,
                            serde_json::to_string_pretty(&coordinator_clone.read().unwrap().current_round().unwrap())
                                .unwrap()
                        );
                        verify.unwrap();
                    }

                    // Add the verification as the verifier.
                    let verify = coordinator_clone.write().unwrap().verify_contribution(&task, &verifier);
                    if verify.is_err() {
                        println!(
                            "Failed to run verification as verifier {:?}\n{}",
                            verifier.clone(),
                            serde_json::to_string_pretty(&coordinator_clone.read().unwrap().current_round().unwrap())
                                .unwrap()
                        );
                        panic!("{:?}", verify.unwrap())
                    }
                }
            });
            verifier_threads.push(verifier_thread);
        }

        for verifier_thread in verifier_threads {
            verifier_thread.join().expect("Couldn't join on the verifier thread");
        }

        Ok(())
    }

    #[test]
    #[serial]
    fn coordinator_aggregation() {
        initialize_test_environment(&TEST_ENVIRONMENT_3);

        let mut coordinator = Coordinator::new(TEST_ENVIRONMENT_3.clone(), Arc::new(Dummy)).unwrap();
        initialize_coordinator(&mut coordinator).unwrap();

        // Check current round height is now 1.
        let round_height = coordinator.current_round_height().unwrap();
        assert_eq!(1, round_height);

        // Run computation and verification on each contribution in each chunk.
        let contributors = vec![
            Lazy::force(&TEST_CONTRIBUTOR_ID).clone(),
            Lazy::force(&TEST_CONTRIBUTOR_ID_2).clone(),
        ];

        let verifier = Lazy::force(&TEST_VERIFIER_ID).clone();
        let verifier_signing_key: SigningKey = "secret_key".to_string();

        let mut seeds = HashMap::new();

        // Iterate over all chunk IDs.
        for chunk_id in 0..TEST_ENVIRONMENT_3.number_of_chunks() {
            // As contribution ID 0 is initialized by the coordinator, iterate from
            // contribution ID 1 up to the expected number of contributions.
            for contribution_id in 1..coordinator.current_round().unwrap().expected_number_of_contributions() {
                let contributor = &contributors[contribution_id as usize - 1].clone();
                let contributor_signing_key: SigningKey = "secret_key".to_string();

                trace!("{} is processing contribution {}", contributor, contribution_id);
                {
                    // Acquire the lock as contributor.
                    let try_lock = coordinator.try_lock_chunk(chunk_id, &contributor);
                    if try_lock.is_err() {
                        println!(
                            "Failed to acquire lock for chunk {} as contributor {:?}\n{}",
                            chunk_id,
                            &contributor,
                            serde_json::to_string_pretty(&coordinator.current_round().unwrap()).unwrap()
                        );
                        try_lock.unwrap();
                    }
                }
                {
                    let seed = if seeds.contains_key(&contribution_id) {
                        seeds[&contribution_id]
                    } else {
                        let mut seed: Seed = [0; SEED_LENGTH];
                        rand::thread_rng().fill_bytes(&mut seed[..]);
                        seeds.insert(contribution_id.clone(), seed);
                        seed
                    };

                    // Run computation as contributor.
                    let contribute = coordinator.run_computation(
                        round_height,
                        chunk_id,
                        contribution_id,
                        &contributor,
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
                    let contribute = coordinator.add_contribution(chunk_id, &contributor);
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
                    let task = Task::new(chunk_id, contribution_id);
                    let verify = coordinator.run_verification(round_height, &task, &verifier, &verifier_signing_key);
                    if verify.is_err() {
                        error!(
                            "Failed to run verification as verifier {:?}\n{}",
                            &verifier,
                            serde_json::to_string_pretty(&coordinator.current_round().unwrap()).unwrap()
                        );
                        verify.unwrap();
                    }

                    // Add the verification as the verifier.
                    let verify = coordinator.verify_contribution(&task, &verifier);
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
        }

        println!(
            "Starting aggregation with this transcript {}",
            serde_json::to_string_pretty(&coordinator.current_round().unwrap()).unwrap()
        );

        {
            // Run aggregation for round 1.
            coordinator.aggregate_contributions().unwrap();
        }

        // Check that the round is still round 1, as try_advance has not been called.
        assert_eq!(1, coordinator.current_round_height().unwrap());

        println!(
            "Finished aggregation with this transcript {}",
            serde_json::to_string_pretty(&coordinator.current_round().unwrap()).unwrap()
        );
    }

    #[test]
    #[serial]
    fn coordinator_next_round() -> anyhow::Result<()> {
        initialize_test_environment(&TEST_ENVIRONMENT_3);

        let mut coordinator = Coordinator::new(TEST_ENVIRONMENT_3.clone(), Arc::new(Dummy))?;
        initialize_coordinator_single_contributor(&mut coordinator)?;

        let round_height = 1;
        assert_eq!(round_height, coordinator.current_round_height()?);

        let contributor = Lazy::force(&TEST_CONTRIBUTOR_ID).clone();
        let contributor_signing_key: SigningKey = "secret_key".to_string();

        let verifier = Lazy::force(&TEST_VERIFIER_ID);
        let verifier_signing_key: SigningKey = "secret_key".to_string();

        // Run computation and verification on each contribution in each chunk.
        let mut seeds = HashMap::new();
        for chunk_id in 0..TEST_ENVIRONMENT_3.number_of_chunks() {
            // Ensure contribution ID 0 is already verified by the coordinator.
            assert!(
                coordinator
                    .current_round()?
                    .chunk(chunk_id)?
                    .get_contribution(0)?
                    .is_verified()
            );

            // As contribution ID 0 is initialized by the coordinator, iterate from
            // contribution ID 1 up to the expected number of contributions.
            for contribution_id in 1..coordinator.current_round()?.expected_number_of_contributions() {
                {
                    // Acquire the lock as contributor.
                    let try_lock = coordinator.try_lock_chunk(chunk_id, &contributor);
                    if try_lock.is_err() {
                        error!(
                            "Failed to acquire lock as contributor {:?}\n{}",
                            &contributor,
                            serde_json::to_string_pretty(&coordinator.current_round()?)?
                        );
                        try_lock?;
                    }
                }
                {
                    // Run computation as contributor.
                    let seed = if seeds.contains_key(&contribution_id) {
                        seeds[&contribution_id]
                    } else {
                        let mut seed: Seed = [0; SEED_LENGTH];
                        rand::thread_rng().fill_bytes(&mut seed[..]);
                        seeds.insert(contribution_id.clone(), seed);
                        seed
                    };

                    let contribute = coordinator.run_computation(
                        round_height,
                        chunk_id,
                        contribution_id,
                        &contributor,
                        &contributor_signing_key,
                        &seed,
                    );
                    if contribute.is_err() {
                        error!(
                            "Failed to run computation as contributor {:?}\n{}",
                            &contributor,
                            serde_json::to_string_pretty(&coordinator.current_round()?)?
                        );
                        contribute?;
                    }

                    // Add the contribution as the contributor.
                    let contribute = coordinator.add_contribution(chunk_id, &contributor);
                    if contribute.is_err() {
                        error!(
                            "Failed to add contribution as contributor {:?}\n{}",
                            &contributor,
                            serde_json::to_string_pretty(&coordinator.current_round()?)?
                        );
                        contribute?;
                    }
                }
                {
                    // Run verification as verifier.
                    let task = Task::new(chunk_id, contribution_id);
                    let verify = coordinator.run_verification(round_height, &task, &verifier, &verifier_signing_key);
                    if verify.is_err() {
                        error!(
                            "Failed to run verification as verifier {:?}\n{}",
                            &verifier,
                            serde_json::to_string_pretty(&coordinator.current_round()?)?
                        );
                        verify?;
                    }

                    // Add the verification as the verifier.
                    let verify = coordinator.verify_contribution(&task, &verifier);
                    if verify.is_err() {
                        error!(
                            "Failed to run verification as verifier {:?}\n{}",
                            &verifier,
                            serde_json::to_string_pretty(&coordinator.current_round()?)?
                        );
                        verify?;
                    }
                }
            }
        }

        info!(
            "Starting aggregation with this transcript {}",
            serde_json::to_string_pretty(&coordinator.current_round()?)?
        );

        {
            // Run aggregation for round 1.
            coordinator.aggregate_contributions()?;

            // Run aggregation and transition from round 1 to round 2.
            coordinator.next_round(OffsetDateTime::now_utc(), vec![contributor.clone()])?;
        }

        // Check that the ceremony has advanced to round 2.
        assert_eq!(2, coordinator.current_round_height()?);

        info!(
            "Finished aggregation with this transcript {}",
            serde_json::to_string_pretty(&coordinator.current_round()?)?
        );

        Ok(())
    }

    #[test]
    #[serial]
    #[ignore]
    fn coordinator_number_of_chunks() {
        let environment = &*Testing::from(Parameters::TestChunks { number_of_chunks: 4096 });
        initialize_test_environment(environment);

        let mut coordinator = Coordinator::new(environment.clone(), Arc::new(Dummy)).unwrap();
        initialize_coordinator(&mut coordinator).unwrap();

        assert_eq!(
            environment.number_of_chunks(),
            coordinator.get_round(0).unwrap().chunks().len() as u64
        );
    }
}
