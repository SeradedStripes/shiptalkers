lint:
  cargo clippy --all-targets --all-features -- -D warnings

fmt:
  cargo fmt --all

check:
  cargo check --all-targets --all-features

test:
  cargo test --all-features