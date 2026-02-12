use std::time::Instant;

pub fn elapsed_ms(start: Instant) -> u64 {
    let duration = start.elapsed();
    let ms = duration.as_millis();
    u64::try_from(ms).map_or(u64::MAX, |value| value)
}
