//! Text encodings for 128-bit IDs.
//!
//! Every encoding is a positional numeral system over an alphabet, so it is a
//! bijection with `u128`. [`Codec::encode`] gives the shortest form;
//! [`Codec::encode_padded`] left-pads to a fixed width so that, for alphabets
//! listed in ascending code-point order (decimal, base32, base62), padded
//! strings sort lexicographically in the same order as the IDs.
//!
//! | format    | alphabet                       | max length | sortable |
//! |-----------|--------------------------------|-----------:|:--------:|
//! | `decimal` | `0-9`                          | 39         | yes      |
//! | `base32`  | Crockford (`0-9A-Z` minus ILOU)| 26         | yes      |
//! | `base62`  | `0-9A-Za-z`                    | 22         | yes      |
//! | `fruity`  | 16 fruits (hex)                | 32         | no       |
//! | `mania`   | 95 emoji (default)              | 20         | no       |
//!
//! `fruity` and `mania` come from the old `mania-id` project: same alphabets,
//! now as reversible encodings rather than random strings.

use std::collections::HashMap;
use std::fmt;
use std::sync::LazyLock;

use serde::{Deserialize, Serialize};

use crate::id::Id;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DecodeError {
    #[error("empty ID")]
    Empty,
    #[error("invalid symbol {symbol:?} at position {position} for the {codec} format")]
    InvalidSymbol {
        codec: &'static str,
        symbol: char,
        position: usize,
    },
    #[error("value does not fit in 128 bits")]
    Overflow,
}

pub trait Codec: Send + Sync {
    fn name(&self) -> &'static str;
    /// Shortest representation (no leading zero symbols, except for `0`).
    fn encode(&self, id: Id) -> String;
    /// Fixed-width representation, left-padded with the zero symbol.
    fn encode_padded(&self, id: Id) -> String;
    /// Accepts both shortest and padded forms.
    fn decode(&self, s: &str) -> Result<Id, DecodeError>;
}

/// A positional codec over an arbitrary alphabet of up to 256 `char`s
/// (ASCII or any UTF-8 symbol). Symbol `i` is digit `i`.
pub struct AlphabetCodec {
    name: &'static str,
    symbols: Vec<char>,
    lookup: HashMap<char, u8>,
    width: usize,
}

impl AlphabetCodec {
    /// # Panics
    /// If the alphabet has fewer than 2 or more than 256 symbols, or repeats one.
    pub fn new(name: &'static str, alphabet: &str) -> Self {
        let symbols: Vec<char> = alphabet.chars().collect();
        assert!(
            (2..=256).contains(&symbols.len()),
            "{name}: alphabet must have 2..=256 symbols, got {}",
            symbols.len()
        );
        let mut lookup = HashMap::with_capacity(symbols.len());
        for (digit, &c) in symbols.iter().enumerate() {
            assert!(
                lookup.insert(c, digit as u8).is_none(),
                "{name}: duplicate symbol {c:?}"
            );
        }
        let radix = symbols.len() as u128;
        let mut width = 0;
        let mut rest = u128::MAX;
        while rest > 0 {
            rest /= radix;
            width += 1;
        }
        AlphabetCodec {
            name,
            symbols,
            lookup,
            width,
        }
    }

    /// Additionally accepts `alias` as a spelling of the canonical symbol `of`
    /// when decoding.
    pub fn with_alias(mut self, alias: char, of: char) -> Self {
        let digit = *self
            .lookup
            .get(&of)
            .expect("alias target must be in the alphabet");
        self.lookup.entry(alias).or_insert(digit);
        self
    }

    /// Accepts lowercase spellings of uppercase ASCII symbols when decoding.
    pub fn case_insensitive(self) -> Self {
        let upper: Vec<char> = self
            .symbols
            .iter()
            .copied()
            .filter(char::is_ascii_uppercase)
            .collect();
        upper
            .into_iter()
            .fold(self, |codec, c| codec.with_alias(c.to_ascii_lowercase(), c))
    }

    pub fn radix(&self) -> u128 {
        self.symbols.len() as u128
    }

    /// Length of the padded form (number of symbols needed for `u128::MAX`).
    pub fn width(&self) -> usize {
        self.width
    }

    pub fn alphabet(&self) -> &[char] {
        &self.symbols
    }
}

impl Codec for AlphabetCodec {
    fn name(&self) -> &'static str {
        self.name
    }

    fn encode(&self, mut id: Id) -> String {
        if id == 0 {
            return self.symbols[0].to_string();
        }
        let radix = self.radix();
        let mut digits = Vec::with_capacity(self.width);
        while id > 0 {
            digits.push(self.symbols[(id % radix) as usize]);
            id /= radix;
        }
        digits.iter().rev().collect()
    }

    fn encode_padded(&self, id: Id) -> String {
        let short = self.encode(id);
        let pad = self.width - short.chars().count();
        std::iter::repeat_n(self.symbols[0], pad)
            .chain(short.chars())
            .collect()
    }

    fn decode(&self, s: &str) -> Result<Id, DecodeError> {
        if s.is_empty() {
            return Err(DecodeError::Empty);
        }
        let radix = self.radix();
        s.chars()
            .enumerate()
            .try_fold(0u128, |acc, (position, symbol)| {
                let digit = *self.lookup.get(&symbol).ok_or(DecodeError::InvalidSymbol {
                    codec: self.name,
                    symbol,
                    position,
                })?;
                acc.checked_mul(radix)
                    .and_then(|v| v.checked_add(digit as u128))
                    .ok_or(DecodeError::Overflow)
            })
    }
}

pub const DECIMAL_ALPHABET: &str = "0123456789";
/// Crockford's base32: no I, L, O, U to avoid confusion and accidental words.
pub const BASE32_ALPHABET: &str = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";
pub const BASE62_ALPHABET: &str = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
/// Hex digits `0..f` as fruits (from `mania-id`).
pub const FRUITY_ALPHABET: &str = "🥭🍇🍈🍉🍊🍋🍌🍍🍎🍏🍐🍑🍒🍓🥝🥥";
/// 95 single-code-point emoji (from `mania-id`).
pub const MANIA_ALPHABET: &str = concat!(
    "🤪🤬🤣🤗🤩🤔🤮🤐😤🤫🤧🥳🥵🥶🙊💩",
    "👏🙏🤞🤝👋👊🖖",
    "🥭🍇🍈🍉🍊🍋🍌🍍🍎🍏🍐🍑🍒🍓🥝🥥",
    "🥯🥐🥖🥨🧀🥩🍔🍟🌭🥪🌮🥗🍱🍙🍝🍩🍪🧁🎂🍫🍬🍭🍺🍷🍹🥃🥤",
    "🦊🦀🐳🦄🐴🐧🦎🐇🐙🦏",
    "🎷🎸🎺🎻",
    "🚅🚌🚒🚕🚙🚛🚁🚠🚀🔋📘💼📌📏📐",
);

static DECIMAL: LazyLock<AlphabetCodec> =
    LazyLock::new(|| AlphabetCodec::new("decimal", DECIMAL_ALPHABET));
static BASE32: LazyLock<AlphabetCodec> = LazyLock::new(|| {
    AlphabetCodec::new("base32", BASE32_ALPHABET)
        .case_insensitive()
        .with_alias('I', '1')
        .with_alias('i', '1')
        .with_alias('L', '1')
        .with_alias('l', '1')
        .with_alias('O', '0')
        .with_alias('o', '0')
});
static BASE62: LazyLock<AlphabetCodec> =
    LazyLock::new(|| AlphabetCodec::new("base62", BASE62_ALPHABET));
static FRUITY: LazyLock<AlphabetCodec> =
    LazyLock::new(|| AlphabetCodec::new("fruity", FRUITY_ALPHABET));
static MANIA: LazyLock<AlphabetCodec> =
    LazyLock::new(|| AlphabetCodec::new("mania", MANIA_ALPHABET));

/// The built-in encodings.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    clap::ValueEnum,
)]
#[serde(rename_all = "lowercase")]
pub enum Format {
    Decimal,
    Base32,
    Base62,
    Fruity,
    #[default]
    Mania,
}

impl Format {
    pub const ALL: [Format; 5] = [
        Format::Decimal,
        Format::Base32,
        Format::Base62,
        Format::Fruity,
        Format::Mania,
    ];

    pub fn codec(self) -> &'static AlphabetCodec {
        match self {
            Format::Decimal => &DECIMAL,
            Format::Base32 => &BASE32,
            Format::Base62 => &BASE62,
            Format::Fruity => &FRUITY,
            Format::Mania => &MANIA,
        }
    }

    pub fn encode(self, id: Id, padded: bool) -> String {
        if padded {
            self.codec().encode_padded(id)
        } else {
            self.codec().encode(id)
        }
    }

    pub fn decode(self, s: &str) -> Result<Id, DecodeError> {
        self.codec().decode(s)
    }
}

impl fmt::Display for Format {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.codec().name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn samples() -> Vec<u128> {
        let mut v = vec![
            0,
            1,
            9,
            10,
            61,
            62,
            255,
            256,
            u64::MAX as u128,
            u128::MAX - 1,
            u128::MAX,
        ];
        let mut x: u128 = 0x9E37_79B9_7F4A_7C15_F39C_C060_5CED_C834;
        for _ in 0..200 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            v.push(x >> (x % 128) as u32);
        }
        v
    }

    #[test]
    fn roundtrip_all_formats() {
        for format in Format::ALL {
            for id in samples() {
                let short = format.encode(id, false);
                let padded = format.encode(id, true);
                assert_eq!(format.decode(&short), Ok(id), "{format} short {short:?}");
                assert_eq!(format.decode(&padded), Ok(id), "{format} padded {padded:?}");
                assert_eq!(padded.chars().count(), format.codec().width(), "{format}");
            }
        }
    }

    #[test]
    fn widths() {
        let widths: Vec<usize> = Format::ALL.iter().map(|f| f.codec().width()).collect();
        assert_eq!(widths, [39, 26, 22, 32, 20]);
        assert_eq!(Format::Mania.codec().radix(), 95);
    }

    #[test]
    fn padded_ascii_formats_sort_like_numbers() {
        let mut ids = samples();
        ids.sort_unstable();
        for format in [Format::Decimal, Format::Base32, Format::Base62] {
            let encoded: Vec<String> = ids.iter().map(|&id| format.encode(id, true)).collect();
            assert!(
                encoded.windows(2).all(|w| w[0] <= w[1]),
                "{format} not sortable"
            );
        }
    }

    #[test]
    fn known_values() {
        assert_eq!(
            Format::Decimal.encode(u128::MAX, false),
            u128::MAX.to_string()
        );
        assert_eq!(Format::Base62.encode(61, false), "z");
        assert_eq!(Format::Base62.encode(62, false), "10");
        assert_eq!(Format::Base32.encode(0, true), "0".repeat(26));
        // Same expectation as mania-id's `fruityfy` test: hex digits -> fruits.
        assert_eq!(
            Format::Fruity.encode(0xab12cd345fff, false),
            "🍐🍑🍇🍈🍒🍓🍉🍊🍋🥥🥥🥥"
        );
    }

    #[test]
    fn crockford_is_forgiving() {
        let id = Format::Base32.decode("1A2B3C").unwrap();
        assert_eq!(Format::Base32.decode("1a2b3c"), Ok(id));
        assert_eq!(Format::Base32.decode("IO"), Format::Base32.decode("10"));
        assert_eq!(Format::Base32.decode("lo"), Format::Base32.decode("10"));
        assert!(Format::Base32.decode("U").is_err());
    }

    #[test]
    fn decode_errors() {
        assert_eq!(Format::Base62.decode(""), Err(DecodeError::Empty));
        assert_eq!(
            Format::Base62.decode("ab-c"),
            Err(DecodeError::InvalidSymbol {
                codec: "base62",
                symbol: '-',
                position: 2
            })
        );
        let too_big = format!("{}0", u128::MAX);
        assert_eq!(Format::Decimal.decode(&too_big), Err(DecodeError::Overflow));
        assert!(Format::Mania.decode("🍐x").is_err());
    }

    #[test]
    #[should_panic(expected = "duplicate symbol")]
    fn duplicate_symbols_rejected() {
        AlphabetCodec::new("bad", "aba");
    }

    #[test]
    #[should_panic(expected = "alphabet must have 2..=256 symbols, got 1")]
    fn tiny_alphabets_rejected() {
        AlphabetCodec::new("bad", "a");
    }

    #[test]
    fn names_and_alphabets() {
        let names: Vec<String> = Format::ALL.iter().map(Format::to_string).collect();
        assert_eq!(names, ["decimal", "base32", "base62", "fruity", "mania"]);
        assert_eq!(Format::Mania.codec().name(), "mania");
        assert_eq!(
            Format::Decimal
                .codec()
                .alphabet()
                .iter()
                .collect::<String>(),
            DECIMAL_ALPHABET
        );
        assert_eq!(Format::default(), Format::Mania);
    }
}
