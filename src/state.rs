use kube::{
    client::APIClient,
    config::Configuration,
    api::{Informer, WatchEvent, Object, Api, Void},
};
use chrono::prelude::*;
use std::{
    env,
    collections::BTreeMap,
    sync::{Arc, RwLock},
};
use crate::*;

/// Approximation of the CRD we want to work with
/// Replace with own struct.
/// Add serialize for returnability.
#[derive(Deserialize, Serialize, Clone)]
pub struct FooSpec {
  name: String,
  info: String,
}
/// Type alias for the kubernetes object
type Foo = Object<FooSpec, Void>;

/// Data for actix to expose on localhost:8080/
///
/// It's all the services it saw events for as keys, and the last event as value.
pub type ExposedData = BTreeMap<String, (DateTime<Utc>, String)>;

/// User state for Actix
#[derive(Clone)]
pub struct State {
    /// An informer for Foo
    info: Informer<Foo>,
    /// Internal state built up by reconciliation loop
    data: Arc<RwLock<ExposedData>>,
    /// A kube client for performing cluster actions based on Foo events
    client: APIClient,
}

/// Example State machine that watches
///
/// This only deals with a single CRD, and it takes the NAMESPACE from an evar.
impl State {
    fn new(client: APIClient) -> Result<Self> {
        let namespace = env::var("NAMESPACE").unwrap_or("default".into());
        let foos : Api<Foo> = Api::customResource(client.clone(), "foos")
            .version("v1")
            .group("clux.dev")
            .within(&namespace);
        let info = Informer::new(foos)
            .timeout(15)
            .init()?;
        let data = Arc::new(RwLock::new(BTreeMap::new()));
        Ok(State { info, data, client })
    }
    /// Internal poll for internal thread
    fn poll(&self) -> Result<()> {
        self.info.poll()?;
        // in this example we always just handle all the events as they happen:
        while let Some(event) = self.info.pop() {
            self.handle_event(event)?;
        }
        Ok(())
    }

    fn handle_event(&self, ev: WatchEvent<Foo>) -> Result<()> {
        // This example only builds some debug data based on events
        // You can use self.client here to make the necessary kube api calls
        match ev {
            WatchEvent::Added(o) => {
                let name = o.metadata.name.clone();
                info!("Added Foo: {} ({})", name, o.spec.info);
                self.data.write().unwrap()
                    .entry(name).or_insert_with(|| (Utc::now(), "Add".into()));
            },
            WatchEvent::Modified(o) => {
                let name = o.metadata.name.clone();
                info!("Modified Foo: {} ({})", name, o.spec.info);
                self.data.write().unwrap()
                    .entry(name).and_modify(|e| *e = (Utc::now(), "Modified".into()));
            },
            WatchEvent::Deleted(o) => {
                let name = o.metadata.name.clone();
                info!("Deleted Foo: {}", name);
                self.data.write().unwrap()
                    .entry(name).or_insert_with(|| (Utc::now(), "Deleted".into()));
            },
            WatchEvent::Error(e) => {
                warn!("Error event: {:?}", e); // we could refresh here
            }
        }
        Ok(())
    }
    /// Exposed getters for read access to data for app
    pub fn foos(&self) -> Result<ExposedData> {
        // unwrap for users because Poison errors are not great to deal with atm
        // rather just have the handler 500 above in this case
        let res = self.data.read().unwrap().clone();
        Ok(res)
    }
}

/// Lifecycle initialization interface for app
///
/// This returns a `State` and calls `poll` on it continuously.
pub fn init(cfg: Configuration) -> Result<State> {
    let state = State::new(APIClient::new(cfg))?; // for app to read
    let state_clone = state.clone(); // for poll thread to write
    std::thread::spawn(move || {
        loop {
            let _ = state_clone.poll().map_err(|e| {
                error!("Kube state failed to recover: {}", e);
                // rely on kube's crash loop backoff to retry sensibly:
                std::process::exit(1);
            });
        }
    });
    Ok(state)
}
