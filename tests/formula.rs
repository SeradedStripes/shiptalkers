use ship_talkers::formula::{Formula, Metrics};

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
