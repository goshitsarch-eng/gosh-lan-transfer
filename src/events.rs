// SPDX-License-Identifier: MIT
//! Event handling infrastructure for gosh-lan-transfer
//!
//! This module provides the `EventHandler` trait and implementations
//! for receiving engine events. The event types themselves are defined
//! in the `protocol` module.

use crate::protocol::EngineEvent;
use std::sync::Arc;
use tokio::sync::broadcast;

/// Trait for receiving engine events
///
/// Implement this trait to handle events in your application.
/// The engine will call `on_event` whenever an event occurs.
///
/// # Example
///
/// ```ignore
/// use gosh_lan_transfer::{EventHandler, EngineEvent};
///
/// struct MyHandler;
///
/// impl EventHandler for MyHandler {
///     fn on_event(&self, event: EngineEvent) {
///         match event {
///             EngineEvent::TransferProgress(p) => {
///                 let percent = if p.total_bytes > 0 {
///                     (p.bytes_transferred * 100) / p.total_bytes
///                 } else {
///                     100
///                 };
///                 println!("Progress: {}%", percent);
///             }
///             _ => {}
///         }
///     }
/// }
/// ```
pub trait EventHandler: Send + Sync + 'static {
    /// Called when an engine event occurs
    fn on_event(&self, event: EngineEvent);
}

/// Channel-based event handler for async consumers
///
/// This handler uses a broadcast channel to distribute events,
/// allowing multiple receivers to subscribe.
pub struct ChannelEventHandler {
    sender: broadcast::Sender<EngineEvent>,
}

impl ChannelEventHandler {
    /// Create a new channel-based event handler
    ///
    /// # Arguments
    /// * `capacity` - The capacity of the broadcast channel
    ///
    /// # Returns
    /// A tuple of (handler, receiver) where receiver can be used to receive events
    pub fn new(capacity: usize) -> (Self, broadcast::Receiver<EngineEvent>) {
        let (sender, receiver) = broadcast::channel(capacity);
        (Self { sender }, receiver)
    }

    /// Subscribe to receive events
    ///
    /// Multiple subscribers can receive the same events.
    pub fn subscribe(&self) -> broadcast::Receiver<EngineEvent> {
        self.sender.subscribe()
    }
}

impl EventHandler for ChannelEventHandler {
    fn on_event(&self, event: EngineEvent) {
        // Ignore send errors (no receivers)
        let _ = self.sender.send(event);
    }
}

/// Callback-based event handler for simpler use cases
///
/// This handler invokes a callback function for each event.
pub struct CallbackEventHandler<F>
where
    F: Fn(EngineEvent) + Send + Sync + 'static,
{
    callback: F,
}

impl<F> CallbackEventHandler<F>
where
    F: Fn(EngineEvent) + Send + Sync + 'static,
{
    /// Create a new callback-based event handler
    ///
    /// # Arguments
    /// * `callback` - The function to call for each event
    pub fn new(callback: F) -> Self {
        Self { callback }
    }
}

impl<F> EventHandler for CallbackEventHandler<F>
where
    F: Fn(EngineEvent) + Send + Sync + 'static,
{
    fn on_event(&self, event: EngineEvent) {
        (self.callback)(event);
    }
}

/// No-op event handler that discards all events
///
/// Useful for testing or when events are not needed.
pub struct NoopEventHandler;

impl EventHandler for NoopEventHandler {
    fn on_event(&self, _event: EngineEvent) {
        // Discard event
    }
}

/// Helper function to create an Arc-wrapped channel event handler
pub fn channel_handler(
    capacity: usize,
) -> (Arc<ChannelEventHandler>, broadcast::Receiver<EngineEvent>) {
    let (handler, receiver) = ChannelEventHandler::new(capacity);
    (Arc::new(handler), receiver)
}

/// Helper function to create an Arc-wrapped callback event handler
pub fn callback_handler<F>(callback: F) -> Arc<CallbackEventHandler<F>>
where
    F: Fn(EngineEvent) + Send + Sync + 'static,
{
    Arc::new(CallbackEventHandler::new(callback))
}

/// Helper function to create an Arc-wrapped no-op event handler
pub fn noop_handler() -> Arc<NoopEventHandler> {
    Arc::new(NoopEventHandler)
}
