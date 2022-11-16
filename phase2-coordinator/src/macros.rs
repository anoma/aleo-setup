/// Returns an instance for chunked `Phase2Parameters` given an instantiation of `PairingEngine`,
/// an instance of `Settings`, and a chunk ID.
#[macro_export]
macro_rules! phase2_chunked_parameters {
    ($curve:ident, $settings:ident, $chunk_id:ident) => {{
        use phase2::Phase2Parameters;

        Phase2Parameters::<$curve>::new_chunk(
            $settings.contribution_mode(),
            $chunk_id as usize,
            $settings.chunk_size(),
            $settings.proving_system(),
            $settings.power(),
            $settings.batch_size(),
        )
    }};
}

/// Returns an instance for full `Phase2Parameters` given an instantiation of `PairingEngine`,
/// an instance of `Settings`.
#[macro_export]
macro_rules! phase2_full_parameters {
    ($curve:ident, $settings:ident) => {{
        use phase2::Phase2Parameters;

        Phase2Parameters::<$curve>::new_full($settings.proving_system(), $settings.power(), $settings.batch_size())
    }};
}

/// Returns the unverified contribution filesize given an instantiation of `PairingEngine`,
/// an instance of `Settings`, a chunk ID, a compressed setting, and whether
/// this is the initialization round.
#[macro_export]
macro_rules! unverified_contribution_size {
    ($curve:ident, $settings:ident, $chunk_id:ident, $compressed:ident) => {{
        use setup_utils::UseCompression;

        let parameters = phase2_chunked_parameters!($curve, $settings, $chunk_id);
        match $compressed {
            UseCompression::Yes => parameters.contribution_size as u64,
            UseCompression::No => (parameters.accumulator_size + parameters.public_key_size) as u64,
        }
    }};
}

/// Returns the verified contribution filesize given an instantiation of `PairingEngine`,
/// an instance of `Settings`, a chunk ID, a compressed setting, and whether
/// this is the initialization round.
#[macro_export]
macro_rules! verified_contribution_size {
    ($curve:ident, $settings:ident, $chunk_id:ident, $compressed:ident) => {{
        use setup_utils::UseCompression;

        let parameters = phase2_chunked_parameters!($curve, $settings, $chunk_id);
        match $compressed {
            UseCompression::Yes => (parameters.contribution_size - parameters.public_key_size) as u64,
            UseCompression::No => parameters.accumulator_size as u64,
        }
    }};
}

/// Returns the total number of powers of tau G1 given a proving system and the number of powers.
#[macro_export]
macro_rules! total_size_in_g1 {
    ($proving_system:ident, $power:ident) => {{
        use phase2::ProvingSystem;

        match $proving_system {
            ProvingSystem::Groth16 => ((1 << ($power + 1)) - 1),
            ProvingSystem::Marlin => (1 << $power),
        }
    }};
}

/// Returns the chunk size given the desired number of chunks, the proving system,
/// and the number of powers.
#[macro_export]
macro_rules! chunk_size {
    ($num_chunks:ident, $proving_system:ident, $power:ident) => {{ ((total_size_in_g1!($proving_system, $power) + $num_chunks - 1) / $num_chunks) }};
}

/// Returns the final round filesize given an instantiation of `PairingEngine`,
/// an instance of `Settings`, and a compressed setting.
#[macro_export]
macro_rules! round_filesize {
    ($curve:ident, $settings:ident, $compressed:ident) => {{
        let full_parameters = phase2_full_parameters!($curve, $settings);
        full_parameters.get_length($compressed) as u64
    }};
}

/// Returns a pretty print of the given hash bytes for logging.
macro_rules! pretty_hash {
    ($hash:expr) => {{
        let mut output = format!("\n\n");
        for line in $hash.chunks(16) {
            output += "\t";
            for section in line.chunks(4) {
                for b in section {
                    output += &format!("{:02x}", b);
                }
                output += " ";
            }
            output += "\n";
        }
        output
    }};
}

/// Returns an error logging message using `tracing`, then returns the error itself.
#[macro_export]
macro_rules! return_error {
    ($error:ident, $message:ident) => {{
        error!($message);
        return $error;
    }};
}
