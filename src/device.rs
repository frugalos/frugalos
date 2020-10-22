#[derive(Serialize, Deserialize, Debug)]
pub struct DeviceState {
    pub(crate) state: RunningState,
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone)]
#[serde(rename_all = "lowercase")]
pub(crate) enum RunningState {
    Started,
    Stopped,
}
