use crate::{authentication::KeyPair, objects::Participant, storage::Disk};
pub use phase1::{helpers::CurveKind, ContributionMode, ProvingSystem};
use setup_utils::{CheckForCorrectness, UseCompression};

use rayon::iter::{IntoParallelIterator, ParallelIterator};
use serde::{Deserialize, Serialize};

type BatchSize = usize;
type ChunkSize = usize;
type NumberOfChunks = usize;
type Power = usize;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Settings {
    pub contribution_mode: ContributionMode,
    pub proving_system: ProvingSystem,
    pub curve: CurveKind,
    pub power: Power,
    pub batch_size: BatchSize,
    pub chunk_size: ChunkSize,
}

impl Settings {
    /// Creates a new `Settings`
    ///
    /// + `batch_size` - will panic if this is set to `0`.
    pub fn new(
        contribution_mode: ContributionMode,
        proving_system: ProvingSystem,
        curve: CurveKind,
        power: Power,
        batch_size: BatchSize,
        chunk_size: ChunkSize,
    ) -> Self {
        if batch_size == 0 {
            panic!("batch_size cannot be equal to zero");
        }

        Self {
            contribution_mode,
            proving_system,
            curve,
            power,
            batch_size,
            chunk_size,
        }
    }

    pub fn contribution_mode(&self) -> ContributionMode {
        self.contribution_mode
    }

    pub fn proving_system(&self) -> ProvingSystem {
        self.proving_system
    }

    pub fn curve(&self) -> CurveKind {
        self.curve
    }

    pub fn power(&self) -> Power {
        self.power
    }

    pub fn batch_size(&self) -> BatchSize {
        self.batch_size
    }

    pub fn chunk_size(&self) -> ChunkSize {
        self.chunk_size
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Deployment {
    Testing,
    Development,
    Production,
}

#[derive(Debug, Clone)]
pub enum Parameters {
    AleoInner,
    AleoOuter,
    AleoUniversal,
    Custom(Settings),
    Test3Chunks,
    Test8Chunks,
    TestChunks {
        number_of_chunks: usize,
    },
    TestCustom {
        number_of_chunks: usize,
        power: usize,
        batch_size: usize,
    },
    TestAnoma {
        number_of_chunks: usize,
        power: usize,
        batch_size: usize,
    },
}

impl Parameters {
    /// Returns the corresponding settings for each parameter type.
    fn to_settings(&self) -> Settings {
        match self {
            Parameters::AleoInner => Self::aleo_inner(),
            Parameters::AleoOuter => Self::aleo_outer(),
            Parameters::AleoUniversal => Self::aleo_universal(),
            Parameters::Custom(settings) => settings.clone(),
            Parameters::Test3Chunks => Self::test_3_chunks(),
            Parameters::Test8Chunks => Self::test_8_chunks(),
            Parameters::TestChunks { number_of_chunks } => Self::test_chunks(number_of_chunks),
            Parameters::TestCustom {
                number_of_chunks,
                power,
                batch_size,
            } => Self::test_custom(number_of_chunks, power, batch_size),
            Parameters::TestAnoma {
                number_of_chunks,
                power,
                batch_size,
            } => Self::test_anoma(number_of_chunks, power, batch_size),
        }
    }

    fn aleo_inner() -> Settings {
        Settings::new(
            ContributionMode::Chunked,
            ProvingSystem::Groth16,
            CurveKind::Bls12_377,
            Power::from(19_usize),
            BatchSize::from(2097152_usize),
            ChunkSize::from(32768_usize),
        )
    }

    fn aleo_outer() -> Settings {
        Settings::new(
            ContributionMode::Chunked,
            ProvingSystem::Groth16,
            CurveKind::BW6,
            Power::from(20_usize),
            BatchSize::from(2097152_usize),
            ChunkSize::from(65536_usize),
        )
    }

    fn aleo_universal() -> Settings {
        Settings::new(
            ContributionMode::Chunked,
            ProvingSystem::Marlin,
            CurveKind::Bls12_377,
            Power::from(28_usize),
            BatchSize::from(2097152_usize),
            ChunkSize::from(65536_usize),
        )
    }

    fn test_3_chunks() -> Settings {
        Settings::new(
            ContributionMode::Chunked,
            ProvingSystem::Groth16,
            CurveKind::Bls12_377,
            Power::from(8_usize),
            BatchSize::from(64_usize),
            ChunkSize::from(172_usize),
        )
    }

    fn test_8_chunks() -> Settings {
        Settings::new(
            ContributionMode::Chunked,
            ProvingSystem::Groth16,
            CurveKind::Bls12_377,
            Power::from(14_usize),
            BatchSize::from(64_usize),
            ChunkSize::from(4096_usize),
        )
    }

    fn test_chunks(number_of_chunks: &NumberOfChunks) -> Settings {
        let proving_system = ProvingSystem::Groth16;
        let power = 14_usize;
        let batch_size = 128_usize;
        Settings::new(
            ContributionMode::Chunked,
            proving_system,
            CurveKind::Bls12_377,
            Power::from(power),
            BatchSize::from(batch_size),
            chunk_size!(number_of_chunks, proving_system, power),
        )
    }

    fn test_custom(number_of_chunks: &NumberOfChunks, power: &Power, batch_size: &BatchSize) -> Settings {
        let proving_system = ProvingSystem::Groth16;
        Settings::new(
            ContributionMode::Full,
            proving_system,
            CurveKind::Bls12_381,
            *power,
            *batch_size,
            chunk_size!(number_of_chunks, proving_system, power),
        )
    }

    fn test_anoma(number_of_chunks: &NumberOfChunks, power: &Power, batch_size: &BatchSize) -> Settings {
        let proving_system = ProvingSystem::Groth16;
        Settings::new(
            ContributionMode::Full,
            proving_system,
            CurveKind::Bls12_381,
            *power,
            *batch_size,
            chunk_size!(number_of_chunks, proving_system, power),
        )
    }
}

#[serde_with::serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Environment {
    /// The parameter settings of this coordinator.
    parameters: Settings,
    /// The compressed input setting of the coordinator.
    compressed_inputs: UseCompression,
    /// The compressed output setting of the coordinator.
    compressed_outputs: UseCompression,
    /// The input correctness check preference of the coordinator.
    check_input_for_correctness: CheckForCorrectness,

    /// The minimum number of contributors permitted to participate in a round.
    minimum_contributors_per_round: usize,
    /// The maximum number of contributors permitted to participate in a round.
    maximum_contributors_per_round: usize,
    /// The minimum number of verifiers permitted to participate in a round.
    minimum_verifiers_per_round: usize,
    /// The maximum number of verifiers permitted to participate in a round.
    maximum_verifiers_per_round: usize,
    /// The number of chunks a contributor is authorized to lock in tandem in a round.
    contributor_lock_chunk_limit: usize,
    /// The number of chunks a verifier is authorized to lock in tandem in a round.
    verifier_lock_chunk_limit: usize,
    /// Returns the maximum duration a contributor can go without
    /// being seen by the coordinator before it will be dropped from
    /// the ceremony by the coordinator.
    contributor_seen_timeout: time::Duration,
    /// The maximum duration a verifier can go without being seen by
    /// the coordinator before it will be dropped from the ceremony by
    /// the coordinator.
    verifier_seen_timeout: time::Duration,
    /// The maximum duration a lock can be held by a participant
    /// before it will be dropped from the ceremony by the
    /// coordinator.
    participant_lock_timeout: time::Duration,
    /// The maximum duration a queued contributor can go without a heartbeat.
    queue_seen_timeout: time::Duration,
    /// The number of drops tolerated by a participant before banning them from future rounds.
    participant_ban_threshold: u16,
    /// The setting to allow current contributors to join the queue for the next round.
    allow_current_contributors_in_queue: bool,
    /// The setting to allow current verifiers to join the queue for the next round.
    allow_current_verifiers_in_queue: bool,
    /// The minimum number of seconds to wait after aggregation before starting the next round.
    queue_wait_time: u64,

    /// The contributors managed by the coordinator.
    coordinator_contributors: Vec<Participant>,
    /// The verifiers managed by the coordinator.
    coordinator_verifiers: Vec<Participant>,
    /// The signing key used by the default coordinator's verifier
    default_verifier_signing_key: String,

    /// The software version number of the coordinator.
    software_version: u64,
    /// The deployment environment of this coordinator.
    deployment: Deployment,
    /// The base directory for disk storage of this coordinator.
    local_base_directory: String,

    disable_reliability_zeroing: bool,
}

impl Environment {
    ///
    /// Returns the parameter settings of the coordinator.
    ///
    pub fn parameters(&self) -> Settings {
        self.parameters.clone()
    }

    ///
    /// Returns the compressed input setting of the coordinator.
    ///
    /// The default choice should be `UseCompression::No` to minimize time
    /// spent by contributors on decompressing inputs.
    ///
    pub const fn compressed_inputs(&self) -> UseCompression {
        self.compressed_inputs
    }

    ///
    /// Returns the compressed output setting of the coordinator.
    ///
    /// The default choice should be `UseCompression::Yes` to minimize time
    /// spent by the coordinator and contributors on uploading chunks.
    ///
    pub const fn compressed_outputs(&self) -> UseCompression {
        self.compressed_outputs
    }

    ///
    /// Returns the input correctness check preference of the coordinator.
    ///
    /// The default choice should be `CheckForCorrectness::No` to minimize time
    /// spent by the contributors on reading chunks.
    ///
    pub fn check_input_for_correctness(&self) -> CheckForCorrectness {
        self.check_input_for_correctness
    }

    ///
    /// Returns the minimum number of contributors permitted to
    /// participate in a round.
    ///
    pub const fn minimum_contributors_per_round(&self) -> usize {
        self.minimum_contributors_per_round
    }

    ///
    /// Returns the maximum number of contributors permitted to
    /// participate in a round.
    ///
    pub const fn maximum_contributors_per_round(&self) -> usize {
        self.maximum_contributors_per_round
    }

    ///
    /// Returns the number of chunks a contributor is
    /// authorized to lock in tandem at any point during a round.
    ///
    pub const fn contributor_lock_chunk_limit(&self) -> usize {
        self.contributor_lock_chunk_limit
    }

    ///
    /// Returns the number of chunks a verifier is
    /// authorized to lock in tandem at any point during a round.
    ///
    pub const fn verifier_lock_chunk_limit(&self) -> usize {
        self.verifier_lock_chunk_limit
    }

    ///
    /// Returns the maximum duration a contributor can go without
    /// being seen by the coordinator before it will be dropped from
    /// the ceremony by the coordinator.
    ///
    pub const fn contributor_seen_timeout(&self) -> time::Duration {
        self.contributor_seen_timeout
    }

    ///
    /// Returns the maximum duration a verifier can go without being
    /// seen by the coordinator before it will be dropped from the
    /// ceremony by the coordinator.
    ///
    pub const fn verifier_seen_timeout(&self) -> time::Duration {
        self.verifier_seen_timeout
    }

    ///
    /// Returns the maximum duration that a participant can hold a
    /// lock before being dropped from the ceremony by the
    /// coordinator.
    ///
    pub const fn participant_lock_timeout(&self) -> time::Duration {
        self.participant_lock_timeout
    }

    ///
    /// Returns the maximum duration that a queued contributor can go
    /// without a heartbeat.
    ///
    pub const fn queue_seen_timeout(&self) -> time::Duration {
        self.queue_seen_timeout
    }

    ///
    /// Returns the number of times the coordinator tolerates
    /// a dropped participant before banning them from future rounds.
    ///
    pub const fn participant_ban_threshold(&self) -> u16 {
        self.participant_ban_threshold
    }

    ///
    /// Returns the setting to allow current contributors to
    /// join the queue for the next round.
    ///
    pub const fn allow_current_contributors_in_queue(&self) -> bool {
        self.allow_current_contributors_in_queue
    }

    ///
    /// Returns the setting to allow current verifiers to
    /// join the queue for the next round.
    ///
    pub const fn allow_current_verifiers_in_queue(&self) -> bool {
        self.allow_current_verifiers_in_queue
    }

    ///
    /// Returns the minimum number of seconds to wait after aggregation
    /// before starting the next round.
    ///
    pub const fn queue_wait_time(&self) -> u64 {
        self.queue_wait_time
    }

    ///
    /// Returns the contributors managed by the coordinator.
    ///
    /// The primary purpose of this is to establish an identity for the coordinator
    /// when running initialization of each round.
    ///
    /// This can also be purposed for completing contributions of participants
    /// who may have dropped off and handed over control of their session.
    ///
    pub const fn coordinator_contributors(&self) -> &Vec<Participant> {
        &self.coordinator_contributors
    }

    /// Returns the verifiers managed by the coordinator.
    pub const fn coordinator_verifiers(&self) -> &Vec<Participant> {
        &self.coordinator_verifiers
    }

    /// Returns the default verifiers' signing key.
    pub fn default_verifier_signing_key(&self) -> String {
        self.default_verifier_signing_key.clone()
    }

    ///
    /// Returns the software version number of the coordinator.
    ///
    pub const fn software_version(&self) -> u64 {
        self.software_version
    }

    ///
    /// Returns the deployment environment of the coordinator.
    ///
    pub const fn deployment(&self) -> &Deployment {
        &self.deployment
    }

    ///
    /// Returns the base directory for disk storage of this coordinator.
    ///
    pub fn local_base_directory(&self) -> &str {
        &self.local_base_directory
    }

    ///
    /// Returns the appropriate number of chunks for the coordinator
    /// to run given a proof system, power and chunk size.
    ///
    pub fn number_of_chunks(&self) -> u64 {
        // let proving_system = &self.parameters.proving_system;
        // let power = self.parameters.power;
        // let chunk_size = self.parameters.chunk_size;
        // (total_size_in_g1!(proving_system, power) + chunk_size as u64 - 1) / chunk_size as u64
        1
    }

    /// Returns the storage system of the coordinator.
    pub(crate) fn storage(&self) -> anyhow::Result<Disk> {
        Ok(Disk::load(self)?)
    }

    pub(crate) fn disable_reliability_zeroing(&self) -> bool {
        self.disable_reliability_zeroing
    }
}

impl From<Testing> for Environment {
    fn from(deployment: Testing) -> Environment {
        deployment.environment
    }
}

impl From<Development> for Environment {
    fn from(deployment: Development) -> Environment {
        deployment.environment
    }
}

impl From<Production> for Environment {
    fn from(deployment: Production) -> Environment {
        deployment.environment
    }
}

// TODO (howardwu): Convert the implementation to a procedural macro.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Testing {
    pub environment: Environment,
}

impl Testing {
    pub fn minimum_contributors_per_round(mut self, minimum: usize) -> Self {
        self.environment.minimum_contributors_per_round = minimum;
        self
    }

    pub fn maximum_contributors_per_round(mut self, maximum: usize) -> Self {
        self.environment.maximum_contributors_per_round = maximum;
        self
    }

    pub fn disable_reliability_zeroing(mut self, disable_zeroing: bool) -> Self {
        self.environment.disable_reliability_zeroing = disable_zeroing;
        self
    }

    #[inline]
    pub fn coordinator_contributors(&self, contributors: &[Participant]) -> Self {
        // Check that all participants are contributors.
        if contributors.into_par_iter().filter(|p| !p.is_contributor()).count() > 0 {
            panic!("Specifying to environment a list of coordinator contributors with non-contributors.")
        }

        let mut deployment = self.clone();
        deployment.environment.coordinator_contributors = contributors.to_vec();
        deployment
    }

    #[inline]
    pub fn coordinator_verifiers(&self, verifiers: &[Participant]) -> Self {
        // Check that all participants are verifiers.
        if verifiers.into_par_iter().filter(|p| !p.is_verifier()).count() > 0 {
            panic!("Specifying to environment a list of coordinator verifiers with non-verifiers.")
        }

        let mut deployment = self.clone();
        deployment.environment.coordinator_verifiers = verifiers.to_vec();
        deployment
    }

    pub fn contributor_seen_timeout(&self, contributor_timeout: time::Duration) -> Self {
        let mut deployment = self.clone();
        deployment.environment.contributor_seen_timeout = contributor_timeout;
        deployment
    }

    pub fn participant_lock_timeout(&self, participant_lock_timeout: time::Duration) -> Self {
        let mut deployment = self.clone();
        deployment.environment.participant_lock_timeout = participant_lock_timeout;
        deployment
    }

    pub fn queue_seen_timeout(&self, queue_seen_timeout: time::Duration) -> Self {
        let mut deployment = self.clone();
        deployment.environment.queue_seen_timeout = queue_seen_timeout;
        deployment
    }
}

impl From<Parameters> for Testing {
    fn from(parameters: Parameters) -> Self {
        let mut testing = Self::default();
        testing.environment.parameters = parameters.to_settings();
        testing
    }
}

impl std::ops::Deref for Testing {
    type Target = Environment;

    fn deref(&self) -> &Self::Target {
        &self.environment
    }
}

impl std::default::Default for Testing {
    fn default() -> Self {
        // Generate default verifier of coordinator
        let keypair = KeyPair::new();

        Self {
            environment: Environment {
                parameters: Parameters::Test3Chunks.to_settings(),
                compressed_inputs: UseCompression::No,
                compressed_outputs: UseCompression::Yes,
                check_input_for_correctness: CheckForCorrectness::No,

                minimum_contributors_per_round: 1,
                maximum_contributors_per_round: 1,
                // maximum_contributors_per_round: 5,
                minimum_verifiers_per_round: 1,
                maximum_verifiers_per_round: 5,
                contributor_lock_chunk_limit: 1,
                // contributor_lock_chunk_limit: 5,
                verifier_lock_chunk_limit: 5,
                contributor_seen_timeout: time::Duration::minutes(5),
                verifier_seen_timeout: time::Duration::minutes(15),
                participant_lock_timeout: time::Duration::minutes(20),
                queue_seen_timeout: time::Duration::days(10),
                participant_ban_threshold: 5,
                allow_current_contributors_in_queue: true,
                allow_current_verifiers_in_queue: true,
                queue_wait_time: 0,

                coordinator_contributors: vec![Participant::new_contributor("testing-coordinator-contributor")],
                coordinator_verifiers: vec![Participant::new_verifier(keypair.pubkey().as_ref())],
                default_verifier_signing_key: keypair.sigkey().to_owned(),

                software_version: 1,
                deployment: Deployment::Testing,
                local_base_directory: "./transcript/testing".to_string(),

                disable_reliability_zeroing: false,
            },
        }
    }
}

// TODO (howardwu): Convert the implementation to a procedural macro.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Development {
    environment: Environment,
}

impl Development {
    pub fn minimum_contributors_per_round(mut self, minimum: usize) -> Self {
        self.environment.minimum_contributors_per_round = minimum;
        self
    }

    pub fn maximum_contributors_per_round(mut self, maximum: usize) -> Self {
        self.environment.maximum_contributors_per_round = maximum;
        self
    }

    pub fn contributor_seen_timeout(mut self, timeout: time::Duration) -> Self {
        self.environment.contributor_seen_timeout = timeout;
        self
    }

    pub fn participant_lock_timeout(mut self, timeout: time::Duration) -> Self {
        self.environment.participant_lock_timeout = timeout;
        self
    }

    pub fn disable_reliability_zeroing(mut self, disable_zeroing: bool) -> Self {
        self.environment.disable_reliability_zeroing = disable_zeroing;
        self
    }

    #[inline]
    pub fn coordinator_contributors(&self, contributors: &[Participant]) -> Self {
        // Check that all participants are contributors.
        if contributors.into_par_iter().filter(|p| !p.is_contributor()).count() > 0 {
            panic!("Specifying to environment a list of coordinator contributors with non-contributors.")
        }

        let mut deployment = self.clone();
        deployment.environment.coordinator_contributors = contributors.to_vec();
        deployment
    }

    #[inline]
    pub fn coordinator_verifiers(&self, verifiers: &[Participant]) -> Self {
        // Check that all participants are verifiers.
        if verifiers.into_par_iter().filter(|p| !p.is_verifier()).count() > 0 {
            panic!("Specifying to environment a list of coordinator verifiers with non-verifiers.")
        }

        let mut deployment = self.clone();
        deployment.environment.coordinator_verifiers = verifiers.to_vec();
        deployment
    }
}

impl From<Parameters> for Development {
    fn from(parameters: Parameters) -> Self {
        let mut testing = Self::default();
        testing.environment.parameters = parameters.to_settings();
        testing
    }
}

impl std::ops::Deref for Development {
    type Target = Environment;

    fn deref(&self) -> &Self::Target {
        &self.environment
    }
}

impl std::ops::DerefMut for Development {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.environment
    }
}

impl std::default::Default for Development {
    fn default() -> Self {
        // Generate default verifier of coordinator
        let keypair = KeyPair::new();

        Self {
            environment: Environment {
                parameters: Parameters::AleoInner.to_settings(),
                compressed_inputs: UseCompression::No,
                compressed_outputs: UseCompression::Yes,
                check_input_for_correctness: CheckForCorrectness::No,

                minimum_contributors_per_round: 1,
                maximum_contributors_per_round: 1,
                // maximum_contributors_per_round: 5,
                minimum_verifiers_per_round: 1,
                maximum_verifiers_per_round: 5,
                contributor_lock_chunk_limit: 1,
                // contributor_lock_chunk_limit: 5,
                verifier_lock_chunk_limit: 5,
                contributor_seen_timeout: time::Duration::minutes(1),
                verifier_seen_timeout: time::Duration::minutes(15),
                participant_lock_timeout: time::Duration::minutes(20),
                queue_seen_timeout: time::Duration::minutes(10),
                participant_ban_threshold: 5,
                allow_current_contributors_in_queue: true,
                allow_current_verifiers_in_queue: true,
                queue_wait_time: 60,

                coordinator_contributors: vec![Participant::new_contributor("development-coordinator-contributor")],
                coordinator_verifiers: vec![Participant::new_verifier(keypair.pubkey().as_ref())],
                default_verifier_signing_key: keypair.sigkey().to_owned(),

                software_version: 1,
                deployment: Deployment::Development,
                local_base_directory: "./transcript/development".to_string(),

                disable_reliability_zeroing: false,
            },
        }
    }
}

// TODO (howardwu): Convert the implementation to a procedural macro.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Production {
    environment: Environment,
}

impl Production {
    pub fn minimum_contributors_per_round(mut self, minimum: usize) -> Self {
        self.environment.minimum_contributors_per_round = minimum;
        self
    }

    pub fn maximum_contributors_per_round(mut self, maximum: usize) -> Self {
        self.environment.maximum_contributors_per_round = maximum;
        self
    }

    pub fn contributor_seen_timeout(mut self, timeout: time::Duration) -> Self {
        self.environment.contributor_seen_timeout = timeout;
        self
    }

    pub fn participant_lock_timeout(mut self, timeout: time::Duration) -> Self {
        self.environment.participant_lock_timeout = timeout;
        self
    }

    pub fn queue_seen_timeout(mut self, timeout: time::Duration) -> Self {
        self.environment.queue_seen_timeout = timeout;
        self
    }

    pub fn disable_reliability_zeroing(mut self, disable_zeroing: bool) -> Self {
        self.environment.disable_reliability_zeroing = disable_zeroing;
        self
    }

    #[inline]
    pub fn coordinator_contributors(&self, contributors: &[Participant]) -> Self {
        // Check that all participants are contributors.
        if contributors.into_par_iter().filter(|p| !p.is_contributor()).count() > 0 {
            panic!("Specifying to environment a list of coordinator contributors with non-contributors.")
        }

        let mut deployment = self.clone();
        deployment.environment.coordinator_contributors = contributors.to_vec();
        deployment
    }

    #[inline]
    pub fn coordinator_verifiers(&self, verifiers: &[Participant]) -> Self {
        // Check that all participants are verifiers.
        if verifiers.into_par_iter().filter(|p| !p.is_verifier()).count() > 0 {
            panic!("Specifying to environment a list of coordinator verifiers with non-verifiers.")
        }

        let mut deployment = self.clone();
        deployment.environment.coordinator_verifiers = verifiers.to_vec();
        deployment
    }
}

impl From<Parameters> for Production {
    fn from(parameters: Parameters) -> Self {
        let mut testing = Self::default();
        testing.environment.parameters = parameters.to_settings();
        testing
    }
}

impl std::ops::Deref for Production {
    type Target = Environment;

    fn deref(&self) -> &Self::Target {
        &self.environment
    }
}

impl std::default::Default for Production {
    fn default() -> Self {
        // Generate default verifier of coordinator
        let keypair = KeyPair::new();

        Self {
            environment: Environment {
                parameters: Parameters::AleoInner.to_settings(),
                compressed_inputs: UseCompression::No,
                compressed_outputs: UseCompression::Yes,
                check_input_for_correctness: CheckForCorrectness::No,

                minimum_contributors_per_round: 1,
                maximum_contributors_per_round: 1,
                // maximum_contributors_per_round: 5,
                minimum_verifiers_per_round: 1,
                maximum_verifiers_per_round: 5,
                contributor_lock_chunk_limit: 1,
                // contributor_lock_chunk_limit: 5,
                verifier_lock_chunk_limit: 5,
                contributor_seen_timeout: time::Duration::days(7),
                verifier_seen_timeout: time::Duration::days(7),
                participant_lock_timeout: time::Duration::days(7),
                queue_seen_timeout: time::Duration::days(7),
                participant_ban_threshold: 5,
                allow_current_contributors_in_queue: false,
                allow_current_verifiers_in_queue: true,
                queue_wait_time: 120,

                coordinator_contributors: vec![Participant::new_contributor("coordinator-contributor")],
                coordinator_verifiers: vec![Participant::new_verifier(keypair.pubkey().as_ref())],
                default_verifier_signing_key: keypair.sigkey().to_owned(),

                software_version: 1,
                deployment: Deployment::Production,
                local_base_directory: "./transcript".to_string(),

                disable_reliability_zeroing: false,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::environment::*;

    #[test]
    fn test_aleo_test_3_chunks() {
        let parameters = Parameters::Test3Chunks;
        let power = parameters.to_settings().power;
        assert_eq!(Power::from(8_usize), power);
        assert_eq!(3, Testing::from(parameters).number_of_chunks());
    }

    #[test]
    fn test_aleo_test_8_chunks() {
        let parameters = Parameters::Test8Chunks;
        let power = parameters.to_settings().power;
        assert_eq!(Power::from(14_usize), power);
        assert_eq!(8, Testing::from(parameters).number_of_chunks());
    }

    #[test]
    fn test_custom_chunk_3() {
        let number_of_chunks = 3;

        let parameters = Parameters::TestChunks { number_of_chunks };
        let settings = parameters.to_settings();
        let power = settings.power;
        let chunk_size = settings.chunk_size;

        assert_eq!(Power::from(14_usize), power);
        assert_eq!(ChunkSize::from(10923_usize), chunk_size);
        assert_eq!(number_of_chunks as u64, Testing::from(parameters).number_of_chunks());
    }

    #[test]
    fn test_custom_chunk_8() {
        let number_of_chunks = 8;

        let parameters = Parameters::TestChunks { number_of_chunks };
        let settings = parameters.to_settings();
        let power = settings.power;
        let chunk_size = settings.chunk_size;

        assert_eq!(Power::from(14_usize), power);
        assert_eq!(ChunkSize::from(4096_usize), chunk_size);
        assert_eq!(number_of_chunks as u64, Testing::from(parameters).number_of_chunks());
    }

    #[test]
    fn test_custom_chunk_20() {
        let number_of_chunks = 20;

        let parameters = Parameters::TestChunks { number_of_chunks };
        let settings = parameters.to_settings();
        let power = settings.power;
        let chunk_size = settings.chunk_size;

        assert_eq!(Power::from(14_usize), power);
        assert_eq!(ChunkSize::from(1639_usize), chunk_size);
        assert_eq!(number_of_chunks as u64, Testing::from(parameters).number_of_chunks());
    }
}
