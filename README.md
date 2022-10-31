![Alt text](./namada-trusted-setup.jpeg?raw=true "Namada Trusted Setup")

<h1 align="center">Namada Trusted Setup</h1>

The Namada Trusted Setup Ceremony generates the public parameters for the Multi-Asset Shielded Pool (MASP) circuit and guarantees its security. Under the hood, a trusted setup ceremony is a multi-party computation (MPC) that lets many participants contribute randomness to the public parameters in a trustless manner. The setup is secure, as long as one participant is honest.

# About Namada

[Namada](https://namada.net/) is a sovereign proof-of-stake blockchain, using Tendermint BFT consensus, that enables multi-asset private transfers for any native or non-native asset using a multi-asset shielded pool derived from the Sapling circuit. 

To learn more about the protocol, we recommend the following resources:

- [Introducing Namada: Shielded Transfers with Any Assets](https://medium.com/anomanetwork/introducing-namada-shielded-transfers-with-any-assets-dce2e579384c)
- [Namada's specifications](https://specs.namada.net)

# Participate in Namada Trusted Setup
If you are interested in participating in the ceremony head over to the [Namada website](https://namada.net/trusted-setup.html) and [sign up to the newsletter](https://dev.us7.list-manage.com/subscribe?u=69adafe0399f0f2a434d8924b&id=9e747afc55) to be notified about the launch.

The Namada Trusted Setup CLI exposes two ways to contribute:

- **default**, performs the entire contribution on the current machine `default` 
- **offline**, computes the contribution on a separate (possibly offline) machine (more details [here](#computation-on-another-machine))

In both cases it is possible to provide an optional `--custom-seed` for the RNG: if this is not provided the CLI will ask you to type a random string as an additional source of entropy (sse [details](#custom-random-seed)).

For a visual overview of the contribution process refer to the [flow chart](#flowchart).

## Incentivized program

Namada will provide a reward for the contributors of the ceremony. When contributing, the CLI will ask if you are interested in taking part in this program: if the answer is yes then you will be asked to provide your name and email address.

The client will then generate a mnemonic that you **NEED** to store safely somewhere. This file should then be passed to the following command

```
namada-ts export-keypair <PATH-TO-MNEMONIC-FILE>
```

which will generate a `wallet.toml` file containing your Namada account. You can then import the content of this file into your wallet.

This address will be added to the Namada genesis file with the predefined amount of tokens. We'll compute the address from the public key that you used during the contribution, you don't need to communicate it to us.

## Building and contributing from source

First, [install Rust](https://www.rust-lang.org/tools/install) by entering the following command:
```
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

If you already have Rust installed, make sure it is the most up-to-date version:
```
rustup update
```

Once Rust is installed, clone the Namada Trusted Setup Ceremony GitHub repository and change directories into `namada-trusted-setup`:
```
git clone https://github.com/anoma/namada-trusted-setup.git
cd namada-trusted-setup && git checkout v1.0.0-beta.8
```

Build the binary:
```
cargo build --release --bin namada-ts --features cli
```

Move binary on `$PATH` (might require sudo)
```
mv target/release/namada-ts /usr/local/bin 
```

Start your contribution
```
namada-ts contribute default https://contribute.namada.net
```

## Contributing from prebuilt binaries (manual setup)
We provide prebuilt `x86_64` binaries for Linux, MacOS and Windows. For this, go to the [Releases page](https://github.com/anoma/namada-trusted-setup/releases) and download the latest version of the client.

After download, you might need to give execution permissions with `chmod +x namada-ts-{distrib}-{version}`.

Finally start the client with:
```
./namada-ts-{distrib}-{version} contribute default https://contribute.namada.net
```

## Contributing from prebuilt binaries (automated setup)
We also provide an install script to automate binary setup. You can run the following command:
```
curl --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/anoma/namada-trusted-setup/main/install.sh | sh
```

and you are ready to contribute:
```
namada-ts contribute default https://contribute.namada.net
```

### Troubleshooting
In MacOS, you might see appearing the warning "cannot be opened because the developer cannot be verified". To solve this, open the "Security & Privacy" control panel from System Preferences. In `general` tab, next to the info that the binary was prevented from running, click `Allow Anyway`. Run the binary again. This time a different prompt is shown. Click `Open` - the binary should run as you expect.

## Advanced features
Advanced features are available to encourage creativity during your contribution.

### Computation on another machine
You can generate the parameters on a machine that is offline or never connected to internet. Some examples are an air-gapped machine, a brand-new computer or a Raspberry PI. You will still need a second machine connected to the internet to carry out the necessary communication with the coordinator.

On the online machine give the following command:

```
cargo run --release --bin namada-ts --features cli contribute another-machine https://contribute.namada.net
```

This will start the communication process to join the ceremony and download/upload the necessary files. On the offline machine use the following command:

```
cargo run --release --bin namada-ts --features cli contribute offline
```

which will compute the contribution itself. This second command expects the file `challenge.params` got from the online machine to be available in the cwd and it will produce a `contribution.params` to be passed back to the online machine for shipment to the coordinator. The user will be responsible for moving these files around.

### Custom random seed
You can provide your own random seed (32 bytes) to initialize the ChaCha RNG. This is useful if you are using an external source of randomness or don't want to use the OS randomness. Some examples are atmospheric noise, radioactive elements or lava lite.

To use this feature, add the `--custom-seed` flag to your command:
```
cargo run --release --bin namada-ts --features cli contribute default --custom-seed https://contribute.namada.net
```

This flag is available also when contributing offline:

```
cargo run --release --bin namada-ts --features cli contribute offline --custom-seed
```

## Understanding the ceremony

This section describes how it feels to contribute to the ceremony.

### Client Contribution Flow 

1. The client will ask you if you want to take part in the incentivized program. If you answer 'yes', it will generate a secret mnemonic that derives your key pair.  Back up your mnemonic and keep it in a safe place! This is the only way to prove your contribution and claim your rewards later.

2. Then, you will need to provide the unique token for your cohort you received by email. If the token is valid, you will join the queue of the ceremony. You will need to wait a bit until it is your turn. Each round lasts between 4 min and 20 min. During the whole ceremony, please neither close your terminal, nor your internet connection. If you stay offline for more than 2 min, the coordinator will kick you out from the queue.

3. When it is your turn, the client will download the challenge from the coordinator and save it to the root folder. You have at most 20 minutes to compute your contribution and send it back to the coordinator. Be creative and good luck!
#### Flowchart
![Alt text](./ceremony-contribution-diagram.png?raw=true "Ceremony Contribution Flow")

# Overview of previous trusted setup ceremonies

Pairing based zk-SNARKs require the generation of certain parameters in order to achieve high efficiency (small proof sizes, fast proving and verifying time). These parameters are generated by another set of parameters which MUST remain secret. We call these secret parameters the "toxic waste". If a prover knows these secrets, [then they can generate valid proofs for invalid statements](https://medium.com/qed-it/how-toxic-is-the-waste-in-a-zksnark-trusted-setup-9b250d59bdb4), breaking soundness. This is undesired!

In order to guarantee that no prover will ever know these secrets, we can generate them in a distributed manner. Each participant in this so-called "ceremony" will contribute to the generation of the parameters with their own secret. If at least 1 participant is honest and destroys their secret, then there should be no way for a malicious prover to create fake proofs.

This repository contains implementations for the [BGM17](https://eprint.iacr.org/2017/1050) multi party computation. The ceremony is split 
in two phases, one which generates the _Powers of Tau_, and one which "specializes" them to the provided arithmetic circuit for the Groth16 zk-SNARK and this is where we will construct our MASP zk-SNARK. 

Note that the generated Powers of Tau can be re-used for any other Phase 2 setup, or for instantiating other mechanisms, such as the [KZG10](https://www.iacr.org/archive/asiacrypt2010/6477178/6477178.pdf) polynomial commitment scheme.

For instructions on how to ensure that the ceremony is executed properly, refer to [`RECOMMENDATIONS.md`](RECOMMENDATIONS.md).


# Directory Structure

This repository contains several Rust crates that implement the different building blocks of the MPC. The high-level structure of the repository is as follows:
- [`phase1-cli`](phase1-cli): Rust crate that provides a HTTP client that communicates with the REST API endpoints of the coordinator and uses the necessary cryptographic functions to contribute to the trusted setup.
- [`phase1-coordinator`](phase1-coordinator): Rust crate that provides a coordinator library and a HTTP REST API that allow contributors to interact with the coordinator. The coordinator handles the operational steps of the ceremony like: adding a new contributor to the queue, authentificating a contributor, sending and receiving challenge files, removing inactive contributors, reattributing challenge file to a new contributor after a contributor dropped, verifying contributions, creating new files, etc.
- [`phase1`](phase1) and [`setup-utils`](setup-utils): contain utils used in both the client and the coordinator.
- The remaining files contain configs for CI and deployment to AWS EC2 and S3 bucket.

# Audits

The original implementation of the coordinator for the [Aleo Trusted Setup](https://github.com/AleoHQ/aleo-setup) was audited by: 

- [Least Authority](https://leastauthority.com/blog/audit-of-aleo-trusted-setup-phase-1/)

# License

All code in this workspace is licensed under either of

 * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in the work by you, as defined in the Apache-2.0
license, shall be dual licensed as above, without any additional terms or
conditions.

# Community support

- [Discord](https://discord.com/invite/anoma) (Questions and discussions on Namada)
- [Twitter](https://twitter.com/namadanetwork)
