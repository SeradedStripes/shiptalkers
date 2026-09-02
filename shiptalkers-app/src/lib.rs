#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::sync::OnceLock;

static TLS_PROVIDER: OnceLock<()> = OnceLock::new();

pub fn init_tls() {
    TLS_PROVIDER.get_or_init(|| {
        rustls::crypto::ring::default_provider()
            .install_default()
            .ok();
    });
}

pub mod auth;
pub mod bot_image;
pub mod db;
pub mod settings;
pub mod slack;
pub mod website;

pub use ship_talkers_lib::sessionize;
pub use ship_talkers_lib::sqlx;
