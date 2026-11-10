//! Metrics handles, registered on first use per declared namespace/event.
//!
//! Names are formatted once during startup, but the underlying `brz-metrics`
//! slots are only created when an operation is first observed. Declared but
//! unused namespaces and events therefore never appear in a scrape.
#![allow(missing_docs)]
#[cfg(feature = "metrics")]
use brz_metrics::{ConnMetric, CounterMetric, MetricType, ResourceMetric, TimingMetric};
use std::borrow::Cow;
#[cfg(feature = "metrics")]
use std::sync::{Arc, OnceLock};
use std::time::Instant;

/// Escape the separator used to join a namespace with the event/phase, so
/// `namespace + ":" + event` stays unambiguous even when either part contains
/// `:`. Names without `:` or `\` are returned unchanged.
fn encode_namespace(namespace: &str) -> Cow<'_, str> {
    if !namespace.contains(':') && !namespace.contains('\\') {
        return Cow::Borrowed(namespace);
    }
    let mut escaped = String::with_capacity(namespace.len() + 4);
    for character in namespace.chars() {
        match character {
            '\\' => escaped.push_str("\\\\"),
            ':' => escaped.push_str("\\:"),
            character => escaped.push(character),
        }
    }
    Cow::Owned(escaped)
}

/// A start-up formatted metric name whose registration is deferred until the
/// first observation. Clones share one registration.
#[cfg(feature = "metrics")]
struct Lazy<T> {
    name: Box<str>,
    value: OnceLock<T>,
}
#[cfg(feature = "metrics")]
impl<T> Lazy<T> {
    fn new(name: String) -> Arc<Self> {
        Arc::new(Self {
            name: name.into_boxed_str(),
            value: OnceLock::new(),
        })
    }
    fn get(&self, init: impl FnOnce(&str) -> T) -> &T {
        self.value.get_or_init(|| init(&self.name))
    }
}

#[derive(Clone)]
pub struct Operation {
    #[cfg(feature = "metrics")]
    metric: Arc<Lazy<TimingMetric>>,
}
impl Operation {
    fn new(_name: &str) -> Self {
        Self {
            #[cfg(feature = "metrics")]
            metric: Lazy::new(_name.to_owned()),
        }
    }
    /// Resolve the metric before taking the start timestamp so registration
    /// cost is never charged to the first observation.
    pub fn start(&self) -> Observation {
        Observation {
            #[cfg(feature = "metrics")]
            metric: *self
                .metric
                .get(|name| TimingMetric::new(MetricType::SocketIo(ResourceMetric::Req), name)),
            started: Instant::now(),
            success: false,
        }
    }
}
pub struct Observation {
    #[cfg(feature = "metrics")]
    metric: TimingMetric,
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
        self.metric.record(self.started.elapsed(), self.success);
        #[cfg(not(feature = "metrics"))]
        let _ = (self.started, self.success);
    }
}
#[derive(Clone)]
pub struct NamespaceMetrics {
    pub admission: Operation,
    pub actions: Operation,
    #[cfg(feature = "metrics")]
    connections: Arc<Lazy<ConnMetric>>,
    #[cfg(feature = "metrics")]
    disconnects: Arc<Lazy<CounterMetric>>,
}
impl NamespaceMetrics {
    pub fn new(namespace: &'static str) -> Self {
        let name = encode_namespace(namespace);
        Self {
            admission: Operation::new(&format!("namespace:{name}:admission")),
            actions: Operation::new(&format!("namespace:{name}:rooms")),
            #[cfg(feature = "metrics")]
            connections: Lazy::new(name.to_string()),
            #[cfg(feature = "metrics")]
            disconnects: Lazy::new(format!("namespace:{name}:disconnect")),
        }
    }
    /// Registers the connection metric on first connection. The disconnect
    /// counter is registered on the first disconnect (`SessionMetric::drop`).
    pub fn connected(&self) -> SessionMetric {
        #[cfg(feature = "metrics")]
        {
            let connections = *self
                .connections
                .get(|name| ConnMetric::new(MetricType::SocketIo(ResourceMetric::Conn), name));
            let disconnects = Arc::clone(&self.disconnects);
            connections.connected();
            SessionMetric {
                connections,
                disconnects,
            }
        }
        #[cfg(not(feature = "metrics"))]
        SessionMetric {}
    }
}
#[cfg_attr(not(feature = "metrics"), allow(dead_code))]
pub struct SessionMetric {
    #[cfg(feature = "metrics")]
    connections: ConnMetric,
    #[cfg(feature = "metrics")]
    disconnects: Arc<Lazy<CounterMetric>>,
}
impl Drop for SessionMetric {
    fn drop(&mut self) {
        #[cfg(feature = "metrics")]
        {
            self.connections.disconnected();
            self.disconnects
                .get(|name| CounterMetric::new(MetricType::SocketIo(ResourceMetric::Req), name))
                .inc();
        }
    }
}
#[derive(Clone)]
pub struct EventMetrics {
    pub handler: Operation,
    pub actions: Operation,
    pub ack: Operation,
}
impl EventMetrics {
    pub fn new(namespace: &'static str, event: Option<&'static str>) -> Self {
        let namespace = encode_namespace(namespace);
        let name = match event {
            Some(event) => format!("event:{namespace}:{event}"),
            None => format!("fallback:{namespace}"),
        };
        Self {
            handler: Operation::new(&format!("{name}:handler")),
            actions: Operation::new(&format!("{name}:rooms")),
            ack: Operation::new(&format!("{name}:ack")),
        }
    }
}

#[derive(Clone)]
pub struct OutgoingMetrics {
    pub publish: Operation,
    pub wait: Operation,
    #[cfg(feature = "metrics")]
    timeouts: Arc<Lazy<CounterMetric>>,
}
impl OutgoingMetrics {
    pub fn new(namespace: &'static str, event: &'static str) -> Self {
        let namespace = encode_namespace(namespace);
        let name = format!("outgoing:{namespace}:{event}");
        Self {
            publish: Operation::new(&format!("{name}:publish")),
            wait: Operation::new(&format!("{name}:ack_wait")),
            #[cfg(feature = "metrics")]
            timeouts: Lazy::new(format!("{name}:ack_timeout")),
        }
    }
    pub fn timed_out(&self) {
        #[cfg(feature = "metrics")]
        self.timeouts
            .get(|name| CounterMetric::new(MetricType::SocketIo(ResourceMetric::Req), name))
            .inc();
    }
}

#[cfg(all(test, feature = "metrics"))]
mod tests {
    use super::*;

    #[test]
    fn namespace_encoding_keeps_concatenated_names_unambiguous() {
        assert_eq!(encode_namespace("/chat"), "/chat");
        assert_eq!(encode_namespace("/a:b"), "/a\\:b");
        assert_eq!(encode_namespace("/a\\b"), "/a\\\\b");

        let first = EventMetrics::new("/a", Some("b:c"));
        let second = EventMetrics::new("/a:b", Some("c"));
        assert_eq!(first.handler.metric.name.as_ref(), "event:/a:b:c:handler");
        assert_eq!(
            second.handler.metric.name.as_ref(),
            "event:/a\\:b:c:handler"
        );
        assert_ne!(
            first.handler.metric.name.as_ref(),
            second.handler.metric.name.as_ref()
        );
    }
}
