use crate::authentication::Signature as SigTrait;
use ed25519_compact::{Error, KeyPair as EdKeyPair, Noise, PublicKey, SecretKey, Seed, Signature};
use serde::{Deserialize, Serialize};
use std::ops::Deref;

/// A private/public key couple encoded in [`base64`]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct KeyPair {
    pubkey: String,
    sigkey: String,
}

impl KeyPair {
    /// Generate a keypair from a seed
    pub fn try_from_seed(seed: &[u8]) -> Result<Self, Error> {
        let seed = Seed::from_slice(&seed[0..32])?;
        let keypair = EdKeyPair::from_seed(seed);

        Ok(KeyPair {
            pubkey: hex::encode(keypair.pk.deref()),
            sigkey: hex::encode(keypair.sk.deref()),
        })
    }

    /// Generate a random keypair
    pub fn new() -> Self {
        let keypair = EdKeyPair::generate();

        KeyPair {
            pubkey: hex::encode(keypair.pk.deref()),
            sigkey: hex::encode(keypair.sk.deref()),
        }
    }

    /// Custom keypair available only in test
    #[cfg(debug_assertions)]
    pub fn custom_new(sigkey: String, pubkey: String) -> Self {
        KeyPair { pubkey, sigkey }
    }

    /// Get a reference to the key pair's pubkey.
    #[must_use]
    pub fn pubkey(&self) -> &str {
        self.pubkey.as_ref()
    }

    /// Get a reference to the key pair's sigkey.
    #[must_use]
    pub fn sigkey(&self) -> &str {
        self.sigkey.as_ref()
    }
}

/// The authentication to be used in production, based on [`ed25519_compact`]
pub struct Production;

impl SigTrait for Production {
    /// Returns the name of the signature scheme.
    fn name(&self) -> String {
        String::from("Production")
    }

    /// Returns `true` if the signature scheme is safe for use in production.
    fn is_secure(&self) -> bool {
        true
    }

    /// Signs the given message using the given signing key,
    /// and returns the signature as a [`hex`] encoded string.
    /// Signing key is expected to be [`hex`] encoded.
    fn sign(&self, signing_key: &str, message: &str) -> anyhow::Result<String> {
        let signing_key_bytes = hex::decode(signing_key)?;
        let signing_key = SecretKey::from_slice(signing_key_bytes.as_ref())?;

        let signature = signing_key.sign(message, Some(Noise::generate()));

        Ok(hex::encode(signature))
    }

    /// Verifies the given signature for the given message and public key,
    /// and returns `true` if the signature is valid.
    /// Public key and signature are expected to be [`hex`] encoded.
    fn verify(&self, public_key: &str, message: &str, signature: &str) -> bool {
        let public_key_bytes = hex::decode(public_key).expect("Invalid public key encoding");
        let public_key = PublicKey::from_slice(public_key_bytes.as_ref()).expect("Invalid public key");

        let signature_bytes = hex::decode(signature).expect("Invalid signature encoding");
        let signature = Signature::from_slice(signature_bytes.as_ref()).expect("Invalid signature");

        public_key.verify(message, &signature).is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sign_and_verify() {
        let sig_scheme = Production;
        let keypair = KeyPair::new();
        let msg = "This is the message to sign";
        let signature = sig_scheme.sign(keypair.sigkey(), msg).unwrap();

        assert!(sig_scheme.verify(keypair.pubkey(), msg, signature.as_ref()));
    }
}
