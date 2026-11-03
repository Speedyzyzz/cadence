//! src/lib.rs
//! Cadence Real-Time Voice AI Failover Proxy, DSP Splicer, Backchannel Masking & High-Density Scaling Plane.

pub mod alignment;
pub mod backchannel;
pub mod drain;
pub mod engine;
pub mod flight_recorder;
pub mod inspector_tap;
pub mod multiplexer;
pub mod protocol;
pub mod server;
pub mod splicer;
pub mod telemetry;
pub mod trunk_pool;
pub mod watchdog;
pub mod wav;
pub mod weather;
