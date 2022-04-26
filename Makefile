CARGO := cargo
CARGO_NIGHTLY := $(CARGO) +nightly
CLI_FLAGS := --bin phase1 --features=cli

build:
	$(CARGO) build

check:
	$(CARGO) check

contribution: # Run contributor against a local coordinator (127.0.0.1:8000)
	RUST_LOG=debug $(CARGO) run $(CLI_FLAGS) contribute

close-ceremony: # Stop local coordinator (127.0.0.1:8000)
	RUST_LOG=debug $(CARGO) run $(CLI_FLAGS) close-ceremony

verify: # Verify pending contributions on local coordinator (127.0.0.1:8000)
	RUST_LOG=debug $(CARGO) run $(CLI_FLAGS) verify-contributions

update-coordinator: # Update manually the coordinator
	RUST_LOG=debug $(CARGO) run $(CLI_FLAGS) update-coordinator

update-coordinator: # Update manually the coordinator
	$(CARGO) run --bin phase1 --features=cli update-coordinator

run-coordinator:
	RUST_LOG=debug $(CARGO) run --bin phase1-coordinator

test-coordinator:
	$(CARGO) test --test test_coordinator --features testing -- --test-threads=1

test-e2e:
	$(CARGO) test --test e2e -- --test-threads=1

fmt:
	$(CARGO_NIGHTLY) fmt --all

clippy:
	$(CARGO_NIGHTLY) clippy --all-targets --all-features -- -D warnings

clippy-fix:
	$(CARGO_NIGHTLY) clippy --fix -Z unstable-options --all-targets --allow-dirty --allow-staged

update:
	$(CARGO) update

clean:
	$(CARGO) clean

.PHONY : build check clean clippy close-ceremony contribution fmt contributor run-coordinator test-coordinator test-e2e update