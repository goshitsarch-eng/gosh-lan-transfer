//! Send-side bandwidth throttling.
//!
//! A simple cumulative-pacing throttle: it tracks how many bytes have been
//! sent since the start of the current window and sleeps just long enough
//! that cumulative throughput never exceeds the configured rate.

use std::time::Duration;
use tokio::time::Instant;

/// How long the stream must be idle before the pacing window resets.
/// Without this, a long stall would be followed by an unthrottled burst.
const IDLE_RESET: Duration = Duration::from_secs(2);

pub(crate) struct Throttle {
    rate_bps: u64,
    window_start: Instant,
    bytes_in_window: u64,
}

impl Throttle {
    pub fn new(rate_bps: u64) -> Self {
        Self {
            rate_bps,
            window_start: Instant::now(),
            bytes_in_window: 0,
        }
    }

    /// Account for `len` bytes and sleep as needed to keep cumulative
    /// throughput at or below `rate_bps`.
    pub async fn pace(&mut self, len: usize) {
        if self.rate_bps == 0 {
            return;
        }

        if self.window_start.elapsed() > IDLE_RESET {
            self.window_start = Instant::now();
            self.bytes_in_window = 0;
        }

        self.bytes_in_window += len as u64;
        let expected = Duration::from_secs_f64(self.bytes_in_window as f64 / self.rate_bps as f64);
        let elapsed = self.window_start.elapsed();
        if expected > elapsed {
            tokio::time::sleep(expected - elapsed).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn test_pace_enforces_rate() {
        let mut throttle = Throttle::new(50_000); // 50 KB/s
        let start = Instant::now();

        // 100 KB in 4 KB chunks should take ~2 seconds at 50 KB/s
        for _ in 0..25 {
            throttle.pace(4096).await;
        }

        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(1900) && elapsed <= Duration::from_millis(2200),
            "expected ~2s, got {:?}",
            elapsed
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_no_sleep_under_rate() {
        let mut throttle = Throttle::new(u64::MAX);
        let start = Instant::now();
        throttle.pace(1_000_000).await;
        assert!(start.elapsed() < Duration::from_millis(10));
    }

    #[tokio::test(start_paused = true)]
    async fn test_zero_rate_is_noop() {
        let mut throttle = Throttle::new(0);
        let start = Instant::now();
        throttle.pace(1_000_000).await;
        assert_eq!(start.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn test_idle_reset_prevents_burst() {
        let mut throttle = Throttle::new(50_000);
        throttle.pace(50_000).await; // fills 1s worth of budget

        // Long idle period: window should reset instead of granting a burst
        tokio::time::sleep(Duration::from_secs(10)).await;

        let start = Instant::now();
        throttle.pace(50_000).await; // a fresh 1s worth
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(900),
            "expected ~1s pacing after idle reset, got {:?}",
            elapsed
        );
    }
}
