const DIGITS: &[u8; 36] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

/// Encodes arbitrary bytes as unambiguous uppercase Base36.
pub fn encode(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return "0".to_owned();
    }

    let leading = bytes.iter().take_while(|&&byte| byte == 0).count();
    let mut number = bytes.to_vec();
    let mut digits = Vec::new();
    while number.iter().any(|&byte| byte != 0) {
        let mut remainder = 0u16;
        for byte in &mut number {
            let value = (remainder << 8) | u16::from(*byte);
            *byte = (value / 36) as u8;
            remainder = value % 36;
        }
        digits.push(DIGITS[remainder as usize]);
        let first_nonzero = number
            .iter()
            .position(|&byte| byte != 0)
            .unwrap_or(number.len());
        number.drain(..first_nonzero);
    }
    if digits.is_empty() {
        digits.push(b'0');
    } else {
        digits.reverse();
    }

    if leading == 0 {
        String::from_utf8(digits).expect("Base36 digits are ASCII")
    } else {
        let count = encode_usize(leading);
        let mut output = String::with_capacity(count.len() + digits.len() + 2);
        output.push('0');
        output.push_str(&count);
        output.push('Z');
        output.push_str(std::str::from_utf8(&digits).expect("Base36 digits are ASCII"));
        output
    }
}

fn encode_usize(mut value: usize) -> String {
    let mut digits = Vec::new();
    loop {
        digits.push(DIGITS[value % 36]);
        value /= 36;
        if value == 0 {
            break;
        }
    }
    digits.reverse();
    String::from_utf8(digits).expect("Base36 digits are ASCII")
}

#[cfg(test)]
mod tests {
    use super::encode;
    use std::collections::HashSet;

    #[test]
    fn encodes_slack_ids() {
        assert_eq!(encode(b"U012ABCD"), encode(b"U012ABCD"));
        assert_eq!(encode(b"C012ABCD"), encode(b"C012ABCD"));
        assert!(
            encode(b"U012ABCD")
                .chars()
                .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit())
        );
    }

    #[test]
    fn distinguishes_lengths_and_edges() {
        let values = [
            b"".as_slice(),
            b"0",
            b"\0",
            b"\0\0",
            b"A",
            b"AA",
            b"U1",
            b"U01",
        ];
        let encoded: HashSet<_> = values.iter().map(|value| encode(value)).collect();
        assert_eq!(encoded.len(), values.len());
        assert_eq!(encode(b""), "0");
    }
}
