/// Slack Time formula for Top Talkers ranking and the per-user Slack Time report.
///
/// Edit `SLACK_TIME_CALCULATION_FORMULA` to change how Slack Time is calculated
/// everywhere it is displayed. Invalid formulas fail at startup.
///
/// Variables (all per user):
/// - `SESSION_SECONDS`: total active time from the sessionizer: the sum of the gaps
///   between consecutive messages when the gap is at most 5 minutes. A gap longer
///   than 5 minutes means the user stepped away, so it is not counted (no per-session
///   grace or cap). This is the raw "time in Slack" estimate, computed in ClickHouse.
/// - `MESSAGE_COUNT`: total number of messages sent (main channel messages and
///   thread replies, from `slack_messages`).
/// - `SESSION_COUNT`: total number of sessions from the sessionizer (each run of
///   messages with no gap longer than 5 minutes).
/// - `TOTAL_CHARS`: total characters typed across all messages, from
///   `sum(char_length(text))`.
/// - `AVG_MESSAGE_LENGTH`: average characters per message (`TOTAL_CHARS` /
///   `MESSAGE_COUNT`), a rough signal for how much you type per message.
///
/// Functions: `log10`, `ln`, `sqrt`, `exp`, `abs`, `pow`. Supports `+ - * / ()`,
/// decimals, and implicit multiplication, e.g. `2MESSAGE_COUNT` or `log10(TOTAL_CHARS)`.
///
/// Default formula: `SESSION_SECONDS + TOTAL_CHARS / 3 + 2 * MESSAGE_COUNT`.
/// Session time is the core estimate; the second term credits typing at 1/3 second
/// per character (~3 chars/s, deliberately slow so it includes time spent thinking),
/// and the flat 2 seconds per message covers opening the Slack window and reading
/// context. It is linear, so the same amount of text scores the same however it is
/// divided across messages.
pub const SLACK_TIME_CALCULATION_FORMULA: &str = "\
    SESSION_SECONDS \
    + TOTAL_CHARS / 3 \
    + 2 * MESSAGE_COUNT \
";

/// Per-user inputs fed into the formula. Computed by the website from ClickHouse.
pub struct Metrics {
    pub message_count: u64,
    pub session_seconds: u64,
    pub session_count: u64,
    pub avg_message_length: f64,
    pub total_chars: u64,
}

#[derive(Clone)]
pub struct Formula {
    source: String,
    expr: Expr,
}

#[derive(Clone)]
enum Expr {
    Num(f64),
    Var(Var),
    Call(Fn, Vec<Expr>),
    Add(Box<Expr>, Box<Expr>),
    Sub(Box<Expr>, Box<Expr>),
    Mul(Box<Expr>, Box<Expr>),
    Div(Box<Expr>, Box<Expr>),
}

#[derive(Clone, Copy)]
enum Var {
    MessageCount,
    SessionSeconds,
    SessionCount,
    AvgMessageLength,
    TotalChars,
}

#[derive(Clone, Copy)]
enum Fn {
    Log10,
    Ln,
    Sqrt,
    Exp,
    Abs,
    Pow,
}

impl Fn {
    fn from_name(name: &str) -> Option<Fn> {
        match name {
            "log10" => Some(Fn::Log10),
            "ln" => Some(Fn::Ln),
            "sqrt" => Some(Fn::Sqrt),
            "exp" => Some(Fn::Exp),
            "abs" => Some(Fn::Abs),
            "pow" => Some(Fn::Pow),
            _ => None,
        }
    }

    fn arity(self) -> usize {
        match self {
            Fn::Pow => 2,
            _ => 1,
        }
    }

    fn eval(self, args: &[Expr], m: &Metrics) -> f64 {
        let a = args[0].eval(m);
        match self {
            Fn::Log10 => a.log10(),
            Fn::Ln => a.ln(),
            Fn::Sqrt => a.sqrt(),
            Fn::Exp => a.exp(),
            Fn::Abs => a.abs(),
            Fn::Pow => a.powf(args[1].eval(m)),
        }
    }
}

impl Formula {
    pub fn parse(input: &str) -> Result<Formula, String> {
        let mut p = Parser {
            chars: input.chars().peekable(),
        };
        p.skip_ws();
        let expr = p.parse_expr()?;
        p.skip_ws();
        if p.chars.peek().is_some() {
            return Err(format!(
                "unexpected character '{}'",
                p.chars.next().unwrap()
            ));
        }
        Ok(Formula {
            source: input.to_string(),
            expr,
        })
    }

    pub fn source(&self) -> &str {
        &self.source
    }

    pub fn eval(&self, m: &Metrics) -> f64 {
        self.expr.eval(m)
    }
}

impl Expr {
    fn eval(&self, m: &Metrics) -> f64 {
        match self {
            Expr::Num(v) => *v,
            Expr::Var(v) => match v {
                Var::MessageCount => m.message_count as f64,
                Var::SessionSeconds => m.session_seconds as f64,
                Var::SessionCount => m.session_count as f64,
                Var::AvgMessageLength => m.avg_message_length,
                Var::TotalChars => m.total_chars as f64,
            },
            Expr::Call(f, args) => f.eval(args, m),
            Expr::Add(a, b) => a.eval(m) + b.eval(m),
            Expr::Sub(a, b) => a.eval(m) - b.eval(m),
            Expr::Mul(a, b) => a.eval(m) * b.eval(m),
            Expr::Div(a, b) => {
                let d = b.eval(m);
                if d == 0.0 { 0.0 } else { a.eval(m) / d }
            }
        }
    }
}

fn parse_var(name: &str) -> Result<Expr, String> {
    match name {
        "MESSAGE_COUNT" => Ok(Expr::Var(Var::MessageCount)),
        "SESSION_SECONDS" => Ok(Expr::Var(Var::SessionSeconds)),
        "SESSION_COUNT" => Ok(Expr::Var(Var::SessionCount)),
        "AVG_MESSAGE_LENGTH" => Ok(Expr::Var(Var::AvgMessageLength)),
        "TOTAL_CHARS" => Ok(Expr::Var(Var::TotalChars)),
        _ => Err(format!("unknown variable '{}'", name)),
    }
}

struct Parser<I: Iterator<Item = char>> {
    chars: std::iter::Peekable<I>,
}

impl<I: Iterator<Item = char>> Parser<I> {
    fn skip_ws(&mut self) {
        while matches!(self.chars.peek(), Some(' ' | '\t' | '\n' | '\r')) {
            self.chars.next();
        }
    }

    fn peek(&mut self) -> Option<char> {
        self.skip_ws();
        self.chars.peek().copied()
    }

    fn next_char(&mut self) -> Option<char> {
        self.skip_ws();
        self.chars.next()
    }

    fn parse_expr(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_term()?;
        loop {
            match self.peek() {
                Some('+') => {
                    self.next_char();
                    let right = self.parse_term()?;
                    left = Expr::Add(Box::new(left), Box::new(right));
                }
                Some('-') => {
                    self.next_char();
                    let right = self.parse_term()?;
                    left = Expr::Sub(Box::new(left), Box::new(right));
                }
                _ => return Ok(left),
            }
        }
    }

    fn parse_term(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_factor()?;
        loop {
            match self.peek() {
                Some('*') => {
                    self.next_char();
                    let right = self.parse_factor()?;
                    left = Expr::Mul(Box::new(left), Box::new(right));
                }
                Some('/') => {
                    self.next_char();
                    let right = self.parse_factor()?;
                    left = Expr::Div(Box::new(left), Box::new(right));
                }
                _ => return Ok(left),
            }
        }
    }

    fn parse_factor(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_primary()?;
        loop {
            match self.peek() {
                Some(c) if c.is_ascii_alphanumeric() || c == '_' || c == '(' => {
                    let right = self.parse_primary()?;
                    left = Expr::Mul(Box::new(left), Box::new(right));
                }
                _ => return Ok(left),
            }
        }
    }

    fn parse_primary(&mut self) -> Result<Expr, String> {
        match self.next_char() {
            Some(c) if c.is_ascii_digit() || c == '.' => {
                let mut num = String::new();
                num.push(c);
                while matches!(self.chars.peek(), Some(ch) if ch.is_ascii_digit() || *ch == '.') {
                    num.push(self.chars.next().unwrap());
                }
                num.parse::<f64>()
                    .map(Expr::Num)
                    .map_err(|_| format!("invalid number '{}'", num))
            }
            Some(c) if c.is_ascii_alphabetic() || c == '_' => {
                let mut name = String::new();
                name.push(c);
                while matches!(
                    self.chars.peek(),
                    Some(ch) if ch.is_ascii_alphanumeric() || *ch == '_'
                ) {
                    name.push(self.chars.next().unwrap());
                }
                if self.peek() == Some('(') {
                    self.next_char();
                    let args = self.parse_args()?;
                    let f = Fn::from_name(&name)
                        .ok_or_else(|| format!("unknown function '{}'", name))?;
                    if args.len() != f.arity() {
                        return Err(format!(
                            "'{}' expects {} argument(s), got {}",
                            name,
                            f.arity(),
                            args.len()
                        ));
                    }
                    Ok(Expr::Call(f, args))
                } else {
                    parse_var(&name)
                }
            }
            Some('(') => {
                let inner = self.parse_expr()?;
                match self.next_char() {
                    Some(')') => Ok(inner),
                    _ => Err("expected ')'".into()),
                }
            }
            Some(c) => Err(format!("unexpected character '{}'", c)),
            None => Err("unexpected end of formula".into()),
        }
    }

    fn parse_args(&mut self) -> Result<Vec<Expr>, String> {
        let mut args = Vec::new();
        loop {
            if self.peek() == Some(')') {
                self.next_char();
                return Ok(args);
            }
            args.push(self.parse_expr()?);
            match self.next_char() {
                Some(',') => continue,
                Some(')') => return Ok(args),
                Some(c) => return Err(format!("expected ',' or ')' but got '{}'", c)),
                None => return Err("expected ',' or ')' but reached end".into()),
            }
        }
    }
}
