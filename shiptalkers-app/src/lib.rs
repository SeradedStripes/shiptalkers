#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

pub mod auth;
pub mod bot_image;
pub mod db;
pub mod sessionize;
pub mod settings;
pub mod slack;
pub mod website;
