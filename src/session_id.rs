use std::fmt;

use anyhow::bail;
use rand::Rng;

const FIRST_WORDS: &[&str] = &[
    "amber", "apple", "apricot", "autumn", "bamboo", "blue", "bright", "cedar", "cherry", "cobalt",
    "coral", "crimson", "dawn", "elm", "emerald", "fern", "forest", "frost", "golden", "green",
    "hazel", "indigo", "ivory", "jade", "juniper", "lake", "lilac", "lime", "maple", "mint",
    "misty", "navy", "olive", "orange", "peach", "pearl", "pine", "plum", "rainy", "red", "river",
    "rose", "ruby", "sage", "scarlet", "silver", "sky", "snowy", "soft", "solar", "spring",
    "stone", "sunny", "teal", "violet", "warm", "willow", "winter", "yellow", "young", "zen",
    "quiet", "swift", "clear",
];

const SECOND_WORDS: &[&str] = &[
    "badger", "bear", "beaver", "bird", "bison", "candy", "comet", "crane", "deer", "dolphin",
    "dove", "eagle", "falcon", "finch", "fox", "gecko", "heron", "horse", "ibis", "koala", "lark",
    "lemur", "lion", "lynx", "marten", "moose", "moth", "mouse", "newt", "otter", "owl", "panda",
    "parrot", "penguin", "pika", "quail", "rabbit", "raven", "robin", "seal", "shark", "sparrow",
    "squid", "stork", "swan", "tiger", "toad", "turtle", "viper", "whale", "wolf", "wren", "yak",
    "zebra", "acorn", "cloud", "grove", "island", "meadow", "moon", "star", "sun", "wave", "wind",
];

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SessionId(String);

impl SessionId {
    pub fn parse(value: &str) -> anyhow::Result<Self> {
        let mut words = value.split('-');
        let valid = matches!((words.next(), words.next(), words.next()),
            (Some(first), Some(second), None)
                if !first.is_empty()
                    && !second.is_empty()
                    && first.bytes().all(|byte| byte.is_ascii_lowercase())
                    && second.bytes().all(|byte| byte.is_ascii_lowercase()));
        if !valid {
            bail!("invalid session_id `{value}`; expected lowercase word-word");
        }
        Ok(Self(value.to_owned()))
    }

    pub fn generate_unique(mut is_active: impl FnMut(&SessionId) -> bool) -> anyhow::Result<Self> {
        let mut rng = rand::rng();
        Self::generate_unique_with(
            || {
                (
                    rng.random_range(0..FIRST_WORDS.len()),
                    rng.random_range(0..SECOND_WORDS.len()),
                )
            },
            &mut is_active,
        )
    }

    fn generate_unique_with(
        mut pair: impl FnMut() -> (usize, usize),
        mut is_active: impl FnMut(&SessionId) -> bool,
    ) -> anyhow::Result<Self> {
        for _ in 0..256 {
            let (first, second) = pair();
            let candidate = Self(format!(
                "{}-{}",
                FIRST_WORDS[first % FIRST_WORDS.len()],
                SECOND_WORDS[second % SECOND_WORDS.len()]
            ));
            if !is_active(&candidate) {
                return Ok(candidate);
            }
        }
        bail!("could not allocate a unique session_id")
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SessionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_id_has_memorable_format() {
        let id = SessionId::generate_unique(|_| false).unwrap();
        assert_eq!(id.as_str().split('-').count(), 2);
        assert!(
            id.as_str()
                .bytes()
                .all(|byte| byte == b'-' || byte.is_ascii_lowercase())
        );
    }

    #[test]
    fn collision_is_regenerated() {
        let active = SessionId::generate_unique_with(|| (0, 0), |_| false).unwrap();
        let mut pairs = [(0, 0), (1, 1)].into_iter();
        let id = SessionId::generate_unique_with(
            || pairs.next().unwrap(),
            |candidate| candidate == &active,
        )
        .unwrap();
        assert_ne!(id, active);
        assert!(SessionId::parse(id.as_str()).is_ok());
    }

    #[test]
    fn malformed_external_id_is_rejected() {
        assert!(SessionId::parse("Maple-Comet").is_err());
        assert!(SessionId::parse("three-word-id").is_err());
        assert!(SessionId::parse("123").is_err());
    }
}
