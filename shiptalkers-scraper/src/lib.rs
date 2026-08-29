use std::sync::OnceLock;

static TLS_PROVIDER: OnceLock<()> = OnceLock::new();

pub fn init_tls() {
    TLS_PROVIDER.get_or_init(|| {
        rustls::crypto::ring::default_provider()
            .install_default()
            .ok();
    });
}

pub mod db;
pub mod hackatime;
pub mod scraper;
pub mod settings;
pub mod slack;

pub use ship_talkers_lib::sessionize;
