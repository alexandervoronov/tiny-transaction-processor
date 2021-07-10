pub trait LogError {
    type Value;

    fn log_and_discard_err(self) -> Option<Self::Value>;
}

impl<V, E: std::fmt::Debug> LogError for Result<V, E> {
    type Value = V;
    fn log_and_discard_err(self) -> Option<Self::Value> {
        self.map_err(|err| eprintln!("Error: {:?}", &err)).ok()
    }
}
