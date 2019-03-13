//! Extensions for serde.

/// A module for serializing/deserializing a `Duration` as milliseconds.
pub mod duration_millis {
    use serde::de;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::num::ParseIntError;
    use std::time::Duration;

    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub fn serialize<S>(value: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let duration = to_millis(value);
        duration.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let millis = String::deserialize(deserializer)?;
        from_millis(&millis).map_err(de::Error::custom)
    }

    pub(crate) fn to_millis(duration: &Duration) -> String {
        (duration.as_secs() * 1000 + u64::from(duration.subsec_millis())).to_string()
    }

    pub(crate) fn from_millis(millis: &str) -> Result<Duration, ParseIntError> {
        millis.parse::<u64>().map(Duration::from_millis)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn to_millis_works() {
        assert_eq!("0", duration_millis::to_millis(&Duration::from_millis(0)));
        assert_eq!("10", duration_millis::to_millis(&Duration::from_millis(10)));
        assert_eq!(
            "999",
            duration_millis::to_millis(&Duration::from_millis(999))
        );
        assert_eq!("0", duration_millis::to_millis(&Duration::from_nanos(3000)));
        assert_eq!(
            "0",
            duration_millis::to_millis(&Duration::from_nanos(999999))
        );
        assert_eq!(
            "1",
            duration_millis::to_millis(&Duration::from_nanos(1000000))
        );
        assert_eq!(
            "1",
            duration_millis::to_millis(&Duration::from_nanos(1000001))
        );
        assert_eq!("1000", duration_millis::to_millis(&Duration::from_secs(1)));
    }

    #[test]
    fn from_millis_works() {
        assert_eq!(
            Ok(Duration::from_millis(0)),
            duration_millis::from_millis("0")
        );
        assert_eq!(
            Ok(Duration::from_millis(1)),
            duration_millis::from_millis("1")
        );
        assert_eq!(
            Ok(Duration::from_millis(1000)),
            duration_millis::from_millis("1000")
        );
        assert!(duration_millis::from_millis("0.1").is_err());
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
