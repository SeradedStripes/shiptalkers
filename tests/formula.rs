use ship_talkers::formula::{Formula, Metrics, SLACK_TIME_CALCULATION_FORMULA, sessionize};

fn m(n: u64, s: u64, k: u64) -> Metrics {
    Metrics {
        message_count: n,
        session_seconds: s,
        session_count: k,
        avg_message_length: 0.0,
        total_chars: 0,
    }
}

fn eval(src: &str, metrics: &Metrics) -> f64 {
    Formula::parse(src).unwrap().eval(metrics)
}

#[test]
fn deployed_formula_matches_expected_slack_time() {
    // A synthetic user: 3 messages 100s apart, then a lone message 6h later,
    // ~50 chars each. The expected total pins the exact semantics of the
    // deployed formula, so changing the formula or sessionizer fails here until
    // the expected value is re-derived on purpose.
    let timeline = [1000000u64, 1000100, 1000200, 1000200 + 21600];
    let s = sessionize(&timeline);
    assert_eq!(s.total_seconds, 800);
    assert_eq!(s.session_count, 2);

    let metrics = Metrics {
        message_count: 4,
        session_seconds: s.total_seconds,
        session_count: s.session_count,
        avg_message_length: 50.0,
        total_chars: 200,
    };
    let f = Formula::parse(SLACK_TIME_CALCULATION_FORMULA).unwrap();
    let total = f.eval(&metrics);
    println!(
        "deployed formula: '{}'\n  timeline: {timeline:?}\n  session_seconds={}, chars={}, messages={}\n  -> {total}s ({}h)",
        SLACK_TIME_CALCULATION_FORMULA.trim(),
        s.total_seconds,
        metrics.total_chars,
        metrics.message_count,
        total / 3600.0
    );
    assert_eq!(total, 824.0);
}

#[test]
fn evaluates_named_variables() {
    assert_eq!(eval("MESSAGE_COUNT", &m(5, 0, 0)), 5.0);
    assert_eq!(eval("SESSION_SECONDS", &m(0, 10, 0)), 10.0);
    assert_eq!(eval("SESSION_COUNT", &m(0, 0, 3)), 3.0);
}

#[test]
fn parses_implicit_multiplication() {
    assert_eq!(eval("2MESSAGE_COUNT", &m(5, 0, 0)), 10.0);
    assert_eq!(eval("2MESSAGE_COUNT + SESSION_SECONDS", &m(3, 10, 1)), 16.0);
    assert_eq!(eval("SESSION_SECONDS + 2MESSAGE_COUNT", &m(3, 10, 1)), 16.0);
}

#[test]
fn respects_precedence() {
    assert_eq!(eval("2 + 3 * 4", &m(0, 0, 0)), 14.0);
    assert_eq!(eval("(2 + 3) * 4", &m(0, 0, 0)), 20.0);
}

#[test]
fn supports_division_and_decimals() {
    let metrics = Metrics {
        message_count: 10,
        session_seconds: 0,
        session_count: 2,
        avg_message_length: 25.0,
        total_chars: 0,
    };
    assert_eq!(eval("SESSION_SECONDS / 60", &m(0, 120, 1)), 2.0);
    assert_eq!(eval("MESSAGE_COUNT * 0.5", &metrics), 5.0);
    assert_eq!(eval("MESSAGE_COUNT / SESSION_COUNT", &metrics), 5.0);
    assert_eq!(eval("MESSAGE_COUNT / SESSION_COUNT", &m(10, 0, 0)), 0.0);
    assert_eq!(eval("AVG_MESSAGE_LENGTH * MESSAGE_COUNT", &metrics), 250.0);
}

#[test]
fn supports_functions() {
    assert_eq!(
        eval(
            "log10(TOTAL_CHARS)",
            &Metrics {
                message_count: 0,
                session_seconds: 0,
                session_count: 0,
                avg_message_length: 0.0,
                total_chars: 100,
            }
        ),
        2.0
    );
    assert_eq!(eval("sqrt(SESSION_SECONDS)", &m(0, 16, 0)), 4.0);
    assert_eq!(eval("abs(MESSAGE_COUNT - 10)", &m(3, 0, 0)), 7.0);
    assert_eq!(eval("pow(2, SESSION_COUNT)", &m(0, 0, 3)), 8.0);
}

#[test]
fn rejects_bad_input() {
    assert!(Formula::parse("").is_err());
    assert!(Formula::parse("2+").is_err());
    assert!(Formula::parse("(2+3").is_err());
    assert!(Formula::parse("UNKNOWN_VAR").is_err());
    assert!(Formula::parse("foo(2)").is_err());
    assert!(Formula::parse("sqrt(1, 2)").is_err());
    assert!(Formula::parse("MESSAGE_COUNT(").is_err());
}
