lint:
  cargo clippy --all-targets --all-features -- -D warnings
lint-fix:
  cargo clippy --all-targets --all-features --fix --allow-dirty --allow-staged

fmt:
  cargo fmt --all

check:
  cargo check --all-targets --all-features

test:
  cargo test --all-features

formula-test:
  cargo test --all-features --test sessionizer -- --nocapture