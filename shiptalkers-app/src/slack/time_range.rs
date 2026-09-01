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

    /// Human-readable label for logging, e.g. "all time".
    pub fn label(&self) -> String {
        match self {
            TimeRange::AllTime => "all time".to_string(),
            TimeRange::Since(ts) => format!("since {}", date_label(*ts)),
            TimeRange::Between(start, end) => {
                format!("{} to {}", date_label(*start), date_label(*end))
            }
        }
    }
}

fn date_label(ts: i64) -> String {
    let (year, month, day) = crate::auth::civil_from_days(ts / 86400);
    format!("{year:04}-{month:02}-{day:02}")
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

fn small_word_number(token: &str) -> Option<u64> {
    match token {
        "zero" => Some(0),
        "one" => Some(1),
        "two" => Some(2),
        "three" => Some(3),
        "four" => Some(4),
        "five" => Some(5),
        "six" => Some(6),
        "seven" => Some(7),
        "eight" => Some(8),
        "nine" => Some(9),
        "ten" => Some(10),
        "eleven" => Some(11),
        "twelve" => Some(12),
        "thirteen" => Some(13),
        "fourteen" => Some(14),
        "fifteen" => Some(15),
        "sixteen" => Some(16),
        "seventeen" => Some(17),
        "eighteen" => Some(18),
        "nineteen" => Some(19),
        "twenty" => Some(20),
        "thirty" => Some(30),
        "forty" => Some(40),
        "fifty" => Some(50),
        "sixty" => Some(60),
        "seventy" => Some(70),
        "eighty" => Some(80),
        "ninety" => Some(90),
        _ => None,
    }
}

fn scale_word_factor(token: &str) -> Option<u64> {
    match token {
        "hundred" => Some(100),
        "thousand" => Some(1_000),
        "million" => Some(1_000_000),
        "billion" => Some(1_000_000_000),
        "trillion" => Some(1_000_000_000_000),
        _ => None,
    }
}

fn unit_nanos(token: &str) -> Option<u64> {
    const SEC: u64 = 1_000_000_000;
    let unit = match token {
        "ms" | "us" | "ns" => token,
        _ => token.strip_suffix('s').unwrap_or(token),
    };
    match unit {
        "second" => Some(SEC),
        "millisecond" | "ms" => Some(SEC / 1_000),
        "microsecond" | "us" => Some(SEC / 1_000_000),
        "nanosecond" | "ns" => Some(1),
        "minute" => Some(60 * SEC),
        "hour" => Some(3600 * SEC),
        "day" => Some(86400 * SEC),
        "week" => Some(7 * 86400 * SEC),
        "month" => Some(30 * 86400 * SEC),
        "year" => Some(365 * 86400 * SEC),
        _ => None,
    }
}

fn parse_number_tokens(tokens: &[&str], start: usize) -> Option<(u64, usize)> {
    let mut total: u64 = 0;
    let mut current: Option<u64> = None;
    let mut next = start;
    while let Some(token) = tokens.get(next).copied() {
        if let Ok(n) = token.parse::<u64>() {
            if current.is_some() || total > 0 {
                break;
            }
            current = Some(n);
        } else if token == "and" && (current.is_some() || total > 0) {
        } else if let Some(n) = small_word_number(token) {
            match current {
                Some(base) if base >= 20 && base % 10 == 0 && n < 100 => current = Some(base + n),
                Some(_) => break,
                None => current = Some(n),
            }
        } else if let Some(factor) = scale_word_factor(token) {
            let base = current.take().unwrap_or(1);
            if factor == 100 {
                current = Some(base.saturating_mul(factor));
            } else {
                total = total.saturating_add(base.saturating_mul(factor));
            }
        } else {
            break;
        }
        next += 1;
    }
    if current.is_none() && total == 0 {
        return None;
    }
    Some((total.saturating_add(current.unwrap_or(0)), next))
}

fn scan_numeric_range(normalized: &str, now: i64) -> Option<TimeRange> {
    let tokens: Vec<&str> = normalized.split_whitespace().collect();
    for i in 0..tokens.len() {
        let Some((count, next)) = parse_number_tokens(&tokens, i) else {
            continue;
        };
        if count == 0 {
            continue;
        }
        let Some(nanos_per_unit) = tokens.get(next).copied().and_then(unit_nanos) else {
            continue;
        };
        let nanos = count.saturating_mul(nanos_per_unit);
        let secs = (nanos / 1_000_000_000).max(1);
        let start = now.saturating_sub(secs as i64).max(0);
        return Some(TimeRange::Since(start));
    }
    None
}

pub fn parse_time_range_at(text: &str, now: i64) -> Option<TimeRange> {
    let normalized: String = text
        .to_lowercase()
        .replace('-', " ")
        .replace("milli second", "millisecond")
        .replace("micro second", "microsecond")
        .replace("nano second", "nanosecond")
        .chars()
        .filter(|c| !c.is_ascii_punctuation())
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");

    if let Some(range) = scan_numeric_range(&normalized, now) {
        return Some(range);
    }

    let today_start = start_of_day(now);
    let this_week_start = start_of_week(now);
    let this_month_start = start_of_month(now);
    let this_year_start = start_of_year(now);
    let ranges: Vec<(&str, TimeRange)> = vec![
        ("all time", TimeRange::AllTime),
        ("time all", TimeRange::AllTime),
        ("alltime", TimeRange::AllTime),
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
        // a = alltime
        ("a", TimeRange::AllTime),
    ];

    ranges
        .into_iter()
        .find(|(phrase, _)| normalized.contains(phrase))
        .map(|(_, range)| range)
}
