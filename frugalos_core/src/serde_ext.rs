//! Extensions for serde.

/// A module for serializing/deserializing a `Duration` as milliseconds.
pub mod duration_millis {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::time::Duration;

    pub fn serialize<S>(value: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        to_millis(value).serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        u64::deserialize(deserializer).map(from_millis)
    }

    pub(crate) fn to_millis(duration: &Duration) -> u64 {
        duration.as_secs() * 1000 + u64::from(duration.subsec_millis())
    }

    pub(crate) fn from_millis(millis: u64) -> Duration {
        Duration::from_millis(millis)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn to_millis_works() {
        assert_eq!(0, duration_millis::to_millis(&Duration::from_millis(0)));
        assert_eq!(10, duration_millis::to_millis(&Duration::from_millis(10)));
        assert_eq!(999, duration_millis::to_millis(&Duration::from_millis(999)));
        assert_eq!(0, duration_millis::to_millis(&Duration::from_nanos(3000)));
        assert_eq!(
            0,
            duration_millis::to_millis(&Duration::from_nanos(999_999))
        );
        assert_eq!(
            1,
            duration_millis::to_millis(&Duration::from_nanos(1_000_000))
        );
        assert_eq!(
            1,
            duration_millis::to_millis(&Duration::from_nanos(1_000_001))
        );
        assert_eq!(1000, duration_millis::to_millis(&Duration::from_secs(1)));
    }

    #[derive(PartialEq, Debug, Serialize, Deserialize)]
    struct TestStruct {
        #[serde(with = "duration_millis")]
        pub duration: Duration,

        #[serde(default = "default_duration", with = "duration_millis")]
        pub default_duration: Duration,
    }
    fn default_duration() -> Duration {
        Duration::from_secs(42)
    }

    #[test]
    fn serialize_and_deserialize_work() -> Result<(), Box<std::error::Error>> {
        let test: TestStruct = TestStruct {
            duration: Duration::from_millis(42),
            default_duration: default_duration(),
        };
        let yaml = serde_yaml::to_string(&test)?;
        let deserialized = serde_yaml::from_str(&yaml)?;

        assert_eq!(test, deserialized);

        Ok(())
    }

    #[test]
    fn default_and_deserialize_work() -> Result<(), Box<std::error::Error>> {
        let yaml = r#"---
duration: 24
"#;
        let deserialized: TestStruct = serde_yaml::from_str(&yaml)?;
        assert_eq!(deserialized.duration, Duration::from_millis(24));
        assert_eq!(deserialized.default_duration, default_duration());

        Ok(())
    }
}
