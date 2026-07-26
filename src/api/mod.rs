use plotters::prelude::*;
use std::path::Path;

use crate::db::sqlite::UserData;

pub fn generate_comparison_chart(
    user: &UserData,
    output_path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let root = BitMapBackend::new(output_path, (800, 600)).into_drawing_area();
    root.fill(&WHITE)?;

    let mut chart = ChartBuilder::on(&root)
        .caption(
            format!("{} - Slack vs Hackatime", user.slack_id),
            ("sans-serif", 20).into_font(),
        )
        .x_label_area_size(35)
        .y_label_area_size(40)
        .build_cartesian_2d(0..30u32, 0..500u32)?;

    chart.configure_mesh().draw()?;

    // TODO: Plot actual data

    root.present()?;
    Ok(())
}
