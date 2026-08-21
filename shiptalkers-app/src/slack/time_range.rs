#[derive(Debug)]
pub enum TimeRange {
    AllTime,
    Since(i64),
    Between(i64, i64),
}

impl TimeRange {
    pub fn start_ts(&self) -> Option<i64> {
        match self {
            TimeRange::AllTime => None,
            TimeRange::Since(ts) => Some(*ts),
            TimeRange::Between(start, _) => Some(*start),
        }
    }

    pub fn end_ts(&self) -> Option<i64> {
        match self {
            TimeRange::Between(_, end) => Some(*end),
            _ => None,
        }
    }

    pub fn start_date(&self) -> Option<String> {
        self.start_ts().map(|ts| {
            let (year, month, day) = crate::auth::civil_from_days(ts / 86400);
            format!("{year:04}-{month:02}-{day:02}")
        })
    }

    pub fn end_date(&self) -> Option<String> {
        self.end_ts().map(|ts| {
            let (year, month, day) = crate::auth::civil_from_days(ts / 86400);
            format!("{year:04}-{month:02}-{day:02}")
        })
    }
}

pub(crate) fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

pub(crate) fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = (if y >= 0 { y } else { y - 399 }) / 400;
    let yoe = y - era * 400;
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

pub(crate) fn start_of_day(ts: i64) -> i64 {
    ts / 86400 * 86400
}

pub(crate) fn start_of_hour(ts: i64) -> i64 {
    ts / 3600 * 3600
}

pub(crate) fn start_of_week(ts: i64) -> i64 {
    let day = ts / 86400;
    let offset = (day + 3).rem_euclid(7);
    (day - offset) * 86400
}

pub(crate) fn start_of_month(ts: i64) -> i64 {
    let days = ts / 86400;
    let (year, month, _) = crate::auth::civil_from_days(days);
    days_from_civil(year as i64, month as i64, 1) * 86400
}

pub(crate) fn start_of_year(ts: i64) -> i64 {
    let days = ts / 86400;
    let (year, _, _) = crate::auth::civil_from_days(days);
    days_from_civil(year as i64, 1, 1) * 86400
}

pub fn parse_time_range_at(text: &str, now: i64) -> Option<TimeRange> {
    let normalized: String = text
        .to_lowercase()
        .chars()
        .filter(|c| !c.is_ascii_punctuation())
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");

    let today_start = start_of_day(now);
    let this_week_start = start_of_week(now);
    let this_month_start = start_of_month(now);
    let this_year_start = start_of_year(now);
    let ranges: Vec<(&str, TimeRange)> = vec![
        ("all time", TimeRange::AllTime),
        ("time all", TimeRange::AllTime),
        ("alltime", TimeRange::AllTime),
        ("48 hours", TimeRange::Since(now - 2 * 86400)),
        ("24 hours", TimeRange::Since(now - 86400)),
        ("12 hours", TimeRange::Since(now - 12 * 3600)),
        ("365 days", TimeRange::Since(now - 365 * 86400)),
        ("90 days", TimeRange::Since(now - 90 * 86400)),
        ("30 days", TimeRange::Since(now - 30 * 86400)),
        ("14 days", TimeRange::Since(now - 14 * 86400)),
        ("7 days", TimeRange::Since(now - 7 * 86400)),
        ("3 months", TimeRange::Since(now - 90 * 86400)),
        ("2 months", TimeRange::Since(now - 60 * 86400)),
        ("2 weeks", TimeRange::Since(now - 14 * 86400)),
        (
            "last year",
            TimeRange::Between(start_of_year(this_year_start - 1), this_year_start),
        ),
        ("this year", TimeRange::Since(this_year_start)),
        (
            "last month",
            TimeRange::Between(start_of_month(this_month_start - 1), this_month_start),
        ),
        ("this month", TimeRange::Since(this_month_start)),
        (
            "last week",
            TimeRange::Between(start_of_week(this_week_start - 86400), this_week_start),
        ),
        ("this week", TimeRange::Since(this_week_start)),
        ("this hour", TimeRange::Since(start_of_hour(now))),
        (
            "yesterday",
            TimeRange::Between(today_start - 86400, today_start),
        ),
        ("today", TimeRange::Since(today_start)),
        ("hour", TimeRange::Since(now - 3600)),
        ("day", TimeRange::Since(now - 86400)),
        ("oneday", TimeRange::Since(now - 86400)),
        ("one second", TimeRange::Since(now - 1)),
    ];

    ranges
        .into_iter()
        .find(|(phrase, _)| normalized.contains(phrase))
        .map(|(_, range)| range)
}
