//! Connection-pool saturation strategies: opening and holding raw sockets at scale.

mod tcp_connection_pool_saturator;

pub use tcp_connection_pool_saturator::{
    SaturationCounters, SaturationReport, SaturatorConfig, TcpConnectionPoolSaturator,
};
