//! The WebSockets this copy holds, by user.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;
use uuid::Uuid;

/// Each user's sockets: (socket ID, its wake-up).
type Sockets = HashMap<Uuid, Vec<(u64, Arc<Notify>)>>;

#[derive(Default)]
pub struct Hub {
    sockets: Mutex<Sockets>,
    next_id: AtomicU64,
    /// Whether the Redis subscription is up (the fallback poll runs while
    /// it is not).
    pub live: AtomicBool,
    /// Clients closed for not taking a message in time
    /// (`websocket_dropped_slow_total`).
    pub dropped_slow: AtomicU64,
}

/// One socket's place in the hub; dropping it removes the socket.
pub struct Registration {
    hub: Arc<Hub>,
    user: Uuid,
    id: u64,
    /// Fires when this user may have new notifications.
    pub wake: Arc<Notify>,
}

impl Hub {
    pub fn register(self: &Arc<Self>, user: Uuid) -> Registration {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let wake = Arc::new(Notify::new());
        self.lock()
            .entry(user)
            .or_default()
            .push((id, wake.clone()));
        Registration {
            hub: self.clone(),
            user,
            id,
            wake,
        }
    }

    /// Tells this user's sockets, if this copy holds any, to check for new
    /// rows. A wake-up that arrives mid-check is kept, never lost.
    pub fn wake(&self, user: Uuid) {
        if let Some(sockets) = self.lock().get(&user) {
            for (_, w) in sockets {
                w.notify_one();
            }
        }
    }

    pub fn wake_all(&self) {
        for sockets in self.lock().values() {
            for (_, w) in sockets {
                w.notify_one();
            }
        }
    }

    /// Connected sockets (`websocket_clients`).
    pub fn clients(&self) -> usize {
        self.lock().values().map(Vec::len).sum()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Sockets> {
        // Nothing panics while holding it, and the map stays valid if
        // something ever does.
        self.sockets.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        let mut sockets = self.hub.lock();
        if let Some(list) = sockets.get_mut(&self.user) {
            list.retain(|(id, _)| *id != self.id);
            if list.is_empty() {
                sockets.remove(&self.user);
            }
        }
    }
}
