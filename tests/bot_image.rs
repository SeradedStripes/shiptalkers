use ship_talkers::bot_image::{NAME_FONT_SIZE, StatsImage, fit_font_size, render_stats_image};

#[test]
fn fit_font_size_fits_long_names() {
    assert_eq!(fit_font_size("Samy"), NAME_FONT_SIZE as u32);
    let long = "A".repeat(50);
    let fs = fit_font_size(&long);
    assert!(fs < NAME_FONT_SIZE as u32);
}

#[test]
fn renders_valid_png() {
    let s = StatsImage {
        user: "Samy",
        percent: 42,
        more: "Slack",
        other: "Coding",
        slack_time: "12h 30m",
        coding_time: "8h 45m",
    };
    let png = render_stats_image(&s).expect("render");
    assert!(png.starts_with(&[0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A]));
    assert!(png.len() > 1000);
    if let Ok(path) = std::env::var("STATS_SAMPLE_OUT") {
        std::fs::write(&path, &png).expect("write sample");
    }
}

#[test]
fn renders_valid_png_with_long_name() {
    let s = StatsImage {
        user: "This is a really quite long Slack display name",
        percent: 42,
        more: "Slack",
        other: "Coding",
        slack_time: "12h 30m",
        coding_time: "8h 45m",
    };
    let png = render_stats_image(&s).expect("render");
    assert!(png.starts_with(&[0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A]));
    assert!(png.len() > 1000);
}
