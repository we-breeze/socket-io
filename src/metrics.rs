//! Metrics handles registered once per declared namespace/event.
#![allow(missing_docs)]
#[cfg(feature = "metrics")]
use brz_metrics::{ConnMetric, CounterMetric, MetricType, ResourceMetric, TimingMetric};
use std::time::Instant;

#[derive(Clone, Copy)]
pub struct Operation {
    #[cfg(feature = "metrics")]
    metric: TimingMetric,
}
impl Operation {
    fn new(_name: &str) -> Self {
        Self {
            #[cfg(feature = "metrics")]
            metric: TimingMetric::new(MetricType::SocketIo(ResourceMetric::Req), _name),
        }
    }
    pub fn start(self) -> Observation {
        Observation {
            operation: self,
            started: Instant::now(),
            success: false,
        }
    }
}
pub struct Observation {
    operation: Operation,
    started: Instant,
    success: bool,
}
impl Observation {
    pub fn finish(mut self, success: bool) {
        self.success = success;
    }
}
impl Drop for Observation {
    fn drop(&mut self) {
        #[cfg(feature = "metrics")]
        self.operation
            .metric
            .record(self.started.elapsed(), self.success);
        #[cfg(not(feature = "metrics"))]
        let _ = (self.operation, self.started, self.success);
    }
}
#[derive(Clone, Copy)]
pub struct NamespaceMetrics {
    pub admission: Operation,
    pub actions: Operation,
    #[cfg(feature = "metrics")]
    connections: ConnMetric,
    #[cfg(feature = "metrics")]
    disconnects: CounterMetric,
}
impl NamespaceMetrics {
    pub fn new(namespace: &'static str) -> Self {
        Self {
            admission: Operation::new(&format!("namespace:{namespace}:admission")),
            actions: Operation::new(&format!("namespace:{namespace}:rooms")),
            #[cfg(feature = "metrics")]
            connections: ConnMetric::new(MetricType::SocketIo(ResourceMetric::Conn), namespace),
            #[cfg(feature = "metrics")]
            disconnects: CounterMetric::new(
                MetricType::SocketIo(ResourceMetric::Req),
                &format!("namespace:{namespace}:disconnect"),
            ),
        }
    }
    pub fn connected(self) -> SessionMetric {
        #[cfg(feature = "metrics")]
        self.connections.connected();
        SessionMetric(self)
    }
}
#[cfg_attr(not(feature = "metrics"), allow(dead_code))]
pub struct SessionMetric(NamespaceMetrics);
impl Drop for SessionMetric {
    fn drop(&mut self) {
        #[cfg(feature = "metrics")]
        {
            self.0.connections.disconnected();
            self.0.disconnects.inc();
        }
    }
}
#[derive(Clone, Copy)]
pub struct EventMetrics {
    pub handler: Operation,
    pub actions: Operation,
    pub ack: Operation,
}
impl EventMetrics {
    pub fn new(namespace: &'static str, event: Option<&'static str>) -> Self {
        // Length prefixes make this identity unambiguous, even for ':' in names.
        let name = match event {
            Some(event) => format!(
                "event:{}:{namespace}:{}:{event}",
                namespace.len(),
                event.len()
            ),
            None => format!("fallback:{}:{namespace}", namespace.len()),
        };
        Self {
            handler: Operation::new(&format!("{name}:handler")),
            actions: Operation::new(&format!("{name}:rooms")),
            ack: Operation::new(&format!("{name}:ack")),
        }
    }
}

#[derive(Clone, Copy)]
pub struct OutgoingMetrics {
    pub publish: Operation,
    pub wait: Operation,
    #[cfg(feature = "metrics")]
    timeouts: CounterMetric,
}
impl OutgoingMetrics {
    pub fn new(namespace: &'static str, event: &'static str) -> Self {
        let name = format!(
            "outgoing:{}:{namespace}:{}:{event}",
            namespace.len(),
            event.len()
        );
        Self {
            publish: Operation::new(&format!("{name}:publish")),
            wait: Operation::new(&format!("{name}:ack_wait")),
            #[cfg(feature = "metrics")]
            timeouts: CounterMetric::new(
                MetricType::SocketIo(ResourceMetric::Req),
                &format!("{name}:ack_timeout"),
            ),
        }
    }
    pub fn timed_out(self) {
        #[cfg(feature = "metrics")]
        self.timeouts.inc();
    }
}
