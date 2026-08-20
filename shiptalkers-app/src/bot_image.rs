use askama::Template;
use resvg::tiny_skia;
use std::sync::OnceLock;

const FONT: &[u8] = include_bytes!("assets/fonts/DejaVuSans.ttf");
const FONT_BOLD: &[u8] = include_bytes!("assets/fonts/DejaVuSans-Bold.ttf");
const CSS: &str = include_str!("website/static/slack_image_stats.css");

fn font_db() -> &'static usvg::fontdb::Database {
    static DB: OnceLock<usvg::fontdb::Database> = OnceLock::new();
    DB.get_or_init(|| {
        let mut db = usvg::fontdb::Database::new();
        db.load_font_data(FONT.to_vec());
        db.load_font_data(FONT_BOLD.to_vec());
        db
    })
}

pub const NAME_FONT_SIZE: f32 = 40.0;
const NAME_MAX_WIDTH: f32 = 580.0;
const IMAGE_SCALE: f32 = 1.2;

pub struct StatsImage<'a> {
    pub user: &'a str,
    pub percent: u64,
    pub more: &'a str,
    pub other: &'a str,
    pub slack_time: &'a str,
    pub coding_time: &'a str,
}

#[derive(Template)]
#[template(path = "slack_image.html")]
struct StatsTemplate<'a> {
    user: &'a str,
    percent: u64,
    more: &'a str,
    other: &'a str,
    slack_time: &'a str,
    coding_time: &'a str,
    user_font_size: u32,
    css: &'static str,
}

fn text_width_px(text: &str, font_size: f32, font_data: &[u8]) -> f32 {
    let Ok(face) = ttf_parser::Face::parse(font_data, 0) else {
        return 0.0;
    };
    let scale = font_size / face.units_per_em() as f32;
    text.chars()
        .map(|c| {
            let gid = face.glyph_index(c).unwrap_or_default();
            face.glyph_hor_advance(gid).unwrap_or_default() as f32 * scale
        })
        .sum()
}

pub fn fit_font_size(text: &str) -> u32 {
    let base = NAME_FONT_SIZE;
    let width = text_width_px(text, base, FONT_BOLD);
    if width <= NAME_MAX_WIDTH {
        return base as u32;
    }
    ((base * NAME_MAX_WIDTH / width) * 0.95) as u32
}

pub fn render_stats_image(s: &StatsImage) -> Result<Vec<u8>, String> {
    let t = StatsTemplate {
        user: s.user,
        percent: s.percent,
        more: s.more,
        other: s.other,
        slack_time: s.slack_time,
        coding_time: s.coding_time,
        user_font_size: fit_font_size(s.user),
        css: CSS,
    };
    render_svg_template(&t)
}

pub struct SlackOnlyImage<'a> {
    pub user: &'a str,
    pub slack_time: &'a str,
}

#[derive(Template)]
#[template(path = "slack_image_slack_only.html")]
struct SlackOnlyTemplate<'a> {
    user: &'a str,
    slack_time: &'a str,
    user_font_size: u32,
    css: &'static str,
}

pub fn render_slack_only_image(s: &SlackOnlyImage) -> Result<Vec<u8>, String> {
    let t = SlackOnlyTemplate {
        user: s.user,
        slack_time: s.slack_time,
        user_font_size: fit_font_size(s.user),
        css: CSS,
    };
    render_svg_template(&t)
}

fn render_svg_template(t: &impl Template) -> Result<Vec<u8>, String> {
    let svg = t
        .render()
        .map_err(|e| format!("template render error: {e}"))?;

    let mut opt = usvg::Options::default();
    *opt.fontdb_mut() = font_db().clone();

    let tree = usvg::Tree::from_str(&svg, &opt).map_err(|e| format!("svg parse error: {e}"))?;
    let size = tree.size();
    let width = (size.width() * IMAGE_SCALE).round() as u32;
    let height = (size.height() * IMAGE_SCALE).round() as u32;

    let mut pixmap = tiny_skia::Pixmap::new(width, height).ok_or("failed to allocate pixmap")?;
    resvg::render(
        &tree,
        tiny_skia::Transform::from_scale(IMAGE_SCALE, IMAGE_SCALE),
        &mut pixmap.as_mut(),
    );
    pixmap
        .encode_png()
        .map_err(|e| format!("png encode error: {e}"))
}
