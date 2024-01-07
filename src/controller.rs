use crate::nes_config::{self, CoordinatorConfig, KafkaConfiguration, LogicalSourceConfig, WorkerConfig};
use crate::{nes_config::LogLevel, telemetry, Error, Metrics, Result};
use ::futures::future;
use ::futures::StreamExt;
use async_recursion::async_recursion;
use chrono::{DateTime, Utc};
use itertools::Itertools;
use k8s_openapi::api::apps::v1::{Deployment, DeploymentSpec};
use k8s_openapi::api::core::v1::{
    ConfigMap, ConfigMapVolumeSource, Container, ContainerPort, EnvVar, PersistentVolumeClaim,
    PersistentVolumeClaimSpec, PersistentVolumeClaimVolumeSource, PodSpec, PodTemplateSpec, Service,
    ServicePort, ServiceSpec, Volume, VolumeMount,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::api::{DeleteParams, PostParams};
use kube::core::ObjectMeta;
use kube::error::ErrorResponse;
use kube::{
    api::{Api, ListParams, Patch, PatchParams, ResourceExt},
    client::Client,
    runtime::{
        controller::{Action, Controller},
        events::{Event, EventType, Recorder, Reporter},
        finalizer::{finalizer, Event as Finalizer},
    },
    CustomResource, Resource,
};
use schemars::JsonSchema;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet};
use std::{collections::HashMap, sync::Arc};
use std::{iter, usize, vec};
use tokio::sync::RwLock;
use tracing::{info, *};

pub static TOPLOGY_FINALIZER: &str = "topology.nebula.stream";
const LABEL_PREFIX: &str = "dima.tu.berlin";
const OWNED_BY: &str = "owned-by";

const RPC_PORT: i32 = 8433;
const REST_PORT: i32 = 8081;
const DATA_PORT: i32 = 8432;

fn merge_yaml(a: &mut serde_yaml::Value, b: serde_yaml::Value) {
    match (a, b) {
        (a @ &mut serde_yaml::Value::Mapping(_), serde_yaml::Value::Mapping(b)) => {
            let a = a.as_mapping_mut().unwrap();
            for (k, v) in b {
                if v.is_sequence() && a.contains_key(&k) && a[&k].is_sequence() {
                    let _v = v.as_sequence().unwrap();
                    let mut _a = a.get(&k).unwrap().as_sequence().unwrap();
                    if _a.len() == _v.len() {
                        let vec_of_values: Vec<serde_yaml::Value> = _a
                            .iter()
                            .zip(_v.into_iter())
                            .map(|(a, v)| {
                                let mut a = a.to_owned();
                                merge_yaml(&mut a, v.clone());
                                a
                            })
                            .collect::<Vec<_>>();
                        a[&k] = serde_yaml::Value::from(vec_of_values);
                    } else {
                        let mut _b = a.get(&k).unwrap().as_sequence().unwrap().to_owned();
                        _b.append(&mut v.as_sequence().unwrap().to_owned());
                        let _b = _b.into_iter().unique().collect::<Vec<_>>();
                        a[&k] = serde_yaml::Value::from(_b);
                    }

                    continue;
                }

                if !a.contains_key(&k) {
                    a.insert(k.to_owned(), v.to_owned());
                } else {
                    merge_yaml(&mut a[&k], v);
                }
            }
        }
        (a, b) => *a = b,
    }
}

#[derive(Hash, Debug, PartialEq, Eq)]
enum ResourceType {
    PVC(Option<String>),
    Deploymnet,
    ConfigMap,
    Service,
    ApiService,
}
#[derive(Hash, Debug, PartialEq, Eq)]
struct SpecChange {
    observed: serde_yaml::Value,
    expected: serde_yaml::Value,
}

#[derive(Hash, Debug, PartialEq, Eq)]
enum ChangeDetected {
    Change(ResourceType, SpecChange),
    Nested(String, Box<ChangeDetected>),
    LeftOver(String, ResourceType),
    Missing(String, ResourceType),
}

fn detect_spec_change<T: std::fmt::Debug + serde::Serialize + DeserializeOwned + std::cmp::PartialEq>(
    observed: &T,
    desired: &T,
) -> Result<(), SpecChange> {
    let mut observed_value = serde_yaml::to_value(observed).unwrap();
    let desired_value = serde_yaml::to_value(desired).unwrap();

    merge_yaml(&mut observed_value, desired_value);
    let merged = serde_yaml::from_value::<T>(observed_value.clone()).unwrap();
    if observed != &merged {
        return Err(SpecChange {
            observed: serde_yaml::to_value(observed).unwrap(),
            expected: serde_yaml::to_value(merged).unwrap(),
        });
    }

    Ok(())
}

#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[cfg_attr(test, derive(Default))]
#[kube(kind = "Topology", group = "kube.rs", version = "v1", namespaced)]
#[kube(status = "TopologyStatus", shortname = "doc")]
#[serde(rename_all = "camelCase")]
pub struct TopologySpec {
    pub coordinator: Option<CoordinatorTopologyNode>,
    pub nodes: HashMap<String, TopologyNode>,
}

#[derive(Deserialize, Serialize, Clone, Default, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct TopologyStatus {
    deployment_created: bool,
    config_maps_created: bool,
    deployment_pending: usize,
    deployment_ready: usize,
    status_string: String,
}

type SourceSchema = Vec<nes_config::LogicalSourceConfigField>;

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[cfg_attr(test, derive(Default))]
#[serde(rename_all = "camelCase")]
pub struct CoordinatorTopologyNode {
    #[serde(flatten)]
    pub worker: TopologyNode,
    pub rest_port: Option<i32>,
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(test, derive(Default))]
pub struct TopologyNode {
    pub downstream: Option<String>,
    pub sources: Option<HashMap<String, NodeSource>>,
    pub resources: Option<usize>,
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct NodeSource {
    pub logical_source: Option<String>,
    pub schema: SourceSchema,
    #[serde(flatten)]
    pub implementation: NodeSourceImplementation,
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum NodeSourceImplementation {
    #[serde(rename = "kafka")]
    KafkaSource {
        brokers: String,
        topic: String,
        group_id: String,
    },
    #[serde(rename = "tcp")]
    TCPSource { host: String, port: usize },
    #[serde(rename = "csv")]
    #[serde(rename_all = "camelCase")]
    CSVSource {
        pv_name: String,
        path: String,
        skip_header: bool,
    },
}

fn get_log_level() -> nes_config::LogLevel {
    return LogLevel::LogDebug;
}

fn create_logical_sources(
    logical_sources: &HashMap<String, &SourceSchema>,
) -> Vec<nes_config::LogicalSourceConfig> {
    logical_sources
        .iter()
        .sorted_by(|a, b| Ord::cmp(&a.0, &b.0))
        .map(|(name, source)| LogicalSourceConfig {
            logical_source_name: name.to_string(),
            fields: source.iter().map(|f| f.clone()).collect(),
        })
        .collect()
}
fn get_source_config(logical_source_name: String, source: &NodeSource) -> nes_config::PhysicalSourceConfig {
    let logical_source_name = source.logical_source.clone().unwrap_or(logical_source_name);
    let physical_source_name = format!("{logical_source_name}_phys");
    let source_type = match &source.implementation {
        NodeSourceImplementation::KafkaSource {
            brokers,
            topic,
            group_id,
        } => nes_config::PhysicalSourceTypeConfig::KafkaConfig {
            configuration: KafkaConfiguration {
                brokers: brokers.clone(),
                auto_commit: 1,
                group_id: group_id.clone(),
                topic: topic.clone(),
                offset_mode: "".to_string(),
                connection_timeout: 10,
                batch_size: 10,
                input_format: "CSV".to_string(),
            },
        },
        NodeSourceImplementation::TCPSource { host, port } => {
            nes_config::PhysicalSourceTypeConfig::TCPConfig {
                configuration: nes_config::TCPConfiguration {
                    socket_host: host.clone(),
                    socket_port: *port,
                    socket_domain: "AF_INET".to_string(),
                    socket_type: "SOCK_STREAM".to_string(),
                    flush_interval_ms: 100,
                    input_format: "CSV".to_string(),
                    decide_message_size: "TUPLE_SEPARATOR".to_string(),
                    tuple_separator: None,
                },
            }
        }
        NodeSourceImplementation::CSVSource {
            path,
            skip_header,
            pv_name,
        } => nes_config::PhysicalSourceTypeConfig::CSVConfig {
            configuration: nes_config::CSVConfiguration {
                file_path: format!("/mnt/{pv_name}/{path}"),
                skip_header: *skip_header,
            },
        },
    };

    return nes_config::PhysicalSourceConfig {
        source_type,
        logical_source_name,
        physical_source_name,
    };
}

pub fn get_worker_host(node_name: &str, owner_refernce: &OwnerReference) -> String {
    format!("{node_name}-{}", owner_refernce.name)
}

pub fn get_coordinator_host(owner_refernce: &OwnerReference) -> String {
    format!("coordinator-{}", owner_refernce.name)
}

pub fn create_coordinator_config(topology: &TopologySpec, owner_refernce: &OwnerReference) -> NESConfig {
    let node = &topology.coordinator;
    let logical_sources: HashMap<String, &SourceSchema> = topology
        .nodes
        .values()
        .chain(topology.coordinator.iter().map(|c| &c.worker))
        .flat_map(|w| w.sources.iter().flat_map(|s| s.iter()))
        .map(|(name, s)| (s.logical_source.as_ref().unwrap_or(name).clone(), &s.schema))
        .collect();

    NESConfig::Coordinator(
        CoordinatorConfig {
            logical_sources: create_logical_sources(&logical_sources),
            coordinator_ip: "REPLACE_WITH_IP".to_string(),
            rest_ip: "REPLACE_WITH_IP".to_string(),
            rest_port: node.as_ref().and_then(|c| c.rest_port).unwrap_or(8081),
            log_level: get_log_level(),
            rpc_port: RPC_PORT,
        },
        WorkerConfig {
            log_level: get_log_level(),
            local_worker_ip: Some("REPLACE_WITH_IP".to_string()),
            coordinator_ip: get_coordinator_host(owner_refernce),
            data_port: DATA_PORT,
            coordinator_port: RPC_PORT,
            rpc_port: 9000,
            num_worker_threads: Some(1),
            number_of_slots: node
                .as_ref()
                .and_then(|n| n.worker.resources)
                .unwrap_or(i32::MAX as usize),
            worker_id: None,
            parent_id: None,
            physical_sources: node
                .as_ref()
                .map(|c| {
                    c.worker
                        .sources
                        .as_ref()
                        .unwrap_or(&Default::default())
                        .iter()
                        .map(|(name, sc)| get_source_config(name.to_string(), sc))
                        .collect()
                })
                .unwrap_or(vec![]),
        },
    )
}

pub fn create_configurations(
    topology: &TopologySpec,
    owner_refernce: &OwnerReference,
) -> HashMap<String, NESConfig> {
    let mut name_to_id: HashMap<&str, usize> = HashMap::new();
    name_to_id.insert("coordinator", 1);
    let mut current_idx = 2;
    for (name, _) in topology.nodes.iter().sorted_by(|a, b| Ord::cmp(&a.0, &b.0)) {
        name_to_id.insert(name.as_str(), current_idx);
        current_idx = current_idx + 1;
    }

    topology
        .nodes
        .iter()
        .map(|(name, node)| {
            (
                name.clone(),
                NESConfig::Worker(WorkerConfig {
                    log_level: get_log_level(),
                    local_worker_ip: Some("REPLACE_WITH_IP".to_string()),
                    coordinator_ip: get_coordinator_host(owner_refernce),
                    number_of_slots: node.resources.unwrap_or(i32::MAX as usize),
                    worker_id: Some(name_to_id[name.as_str()]),
                    data_port: DATA_PORT,
                    coordinator_port: RPC_PORT,
                    num_worker_threads: Some(1),
                    rpc_port: RPC_PORT,
                    parent_id: Some(
                        node.downstream
                            .as_ref()
                            .map(|ds| name_to_id[ds.as_str()])
                            .unwrap_or(0),
                    ),
                    physical_sources: node
                        .sources
                        .as_ref()
                        .unwrap_or(&Default::default())
                        .iter()
                        .map(|(name, sc)| get_source_config(name.to_string(), sc))
                        .collect(),
                }),
            )
        })
        .collect()
}

pub fn create_config_map(node_name: &str, config: &NESConfig, owner_refernce: &OwnerReference) -> ConfigMap {
    let configs = iter::once((
        "workerConfig.yaml".to_string(),
        serde_yaml::to_string(config.get_worker()).unwrap(),
    ))
    .chain(
        config
            .get_coordinator()
            .map(|cc| {
                (
                    "coordinatorConfig.yaml".to_string(),
                    serde_yaml::to_string(cc).unwrap(),
                )
            })
            .into_iter(),
    )
    .map(|(n, yaml)| (n, fix_yaml_indentation(&yaml)))
    .collect::<BTreeMap<String, String>>();

    ConfigMap {
        data: Some(configs),
        metadata: ObjectMeta {
            name: Some(get_config_map_name(node_name, owner_refernce)),
            labels: Some(BTreeMap::from([
                ("topology".to_string(), owner_refernce.name.clone()),
                ("topology-node-name".to_string(), node_name.to_string()),
                (format!("{LABEL_PREFIX}/{OWNED_BY}"), owner_refernce.name.clone()),
            ])),
            owner_references: Some(vec![owner_refernce.clone()]),
            ..Default::default()
        },
        ..Default::default()
    }
}
pub fn get_nes_executable_image() -> String {
    return "localhost:5000/nebulastream/nes-executable-image:local".to_string();
}

pub fn get_coordinator_command_args() -> Option<Vec<String>> {
    Some(
        [
            "sed \"s/REPLACE_WITH_IP/${POD_IP}/g\" /config/workerConfig.yaml > /tmp/workerConfig.yaml &&\
             sed \"s/REPLACE_WITH_IP/${POD_IP}/g\" /config/coordinatorConfig.yaml > /tmp/coordinatorConfig.yaml &&\
             nesCoordinator --configPath=/tmp/coordinatorConfig.yaml --workerConfigPath=/tmp/workerConfig.yaml",
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect(),
    )
}
pub fn get_coordinator_command() -> Option<Vec<String>> {
    return Some(
        // ["./nesWorker", "--configPath=/config/config.yaml"]
        ["/bin/sh", "-c"].into_iter().map(|s| s.to_string()).collect(),
    );
}

pub fn get_worker_command_args(config: &WorkerConfig) -> Option<Vec<String>> {
    Some(
        [format!(
            "sed \"s/REPLACE_WITH_IP/${{POD_IP}}/g\" /config/workerConfig.yaml > /tmp/workerConfig.yaml &&\
             sleep {} && \
             nesWorker --configPath=/tmp/workerConfig.yaml",
            3 + config.worker_id.unwrap_or(5) * 2
        )]
        .into_iter()
        .collect(),
    )
}
pub fn get_worker_command() -> Option<Vec<String>> {
    return Some(
        ["/bin/sh", "-c"]
            // ["/usr/bin/sleep", "1000"]
            .into_iter()
            .map(|s| s.to_string())
            .collect(),
    );
}

struct K8STopology {
    coordinator: K8SNode,
    nodes: HashMap<String, K8SNode>,
}

fn handle_error<T>(result: kube::Result<T>) -> Result<Option<T>> {
    match result {
        Err(e) => match e {
            kube::Error::Api(ErrorResponse { reason, .. }) if reason == "AlreadyExists" => Ok(None),
            _ => {
                error!("{e}");
                Err(Error::KubeError(e))
            }
        },
        Ok(v) => return Ok(Some(v)),
    }
}

impl K8STopology {
    async fn post_to_api(
        &self,
        deployments: Api<Deployment>,
        service: Api<Service>,
        config_maps: Api<ConfigMap>,
        pvcs: Api<PersistentVolumeClaim>,
    ) -> Result<()> {
        let pp = PostParams::default();
        for (node_name, node) in self
            .nodes
            .iter()
            .chain(std::iter::once((&"coordinator".to_string(), &self.coordinator)))
        {
            for pvc in node.pvcs.iter() {
                debug!("Creating PVC \n{}", serde_yaml::to_string(pvc).unwrap());
                handle_error(pvcs.create(&pp, &pvc).await)?;
            }

            debug!(
                "Creating ConfigMap \n{}",
                serde_yaml::to_string(&node.config_map).unwrap()
            );
            handle_error(config_maps.create(&pp, &node.config_map).await)?;
            debug!(
                "Creating Deployment \n{}",
                serde_yaml::to_string(&node.deployment).unwrap()
            );
            handle_error(deployments.create(&pp, &node.deployment).await)?;

            debug!(
                "Creating Service \n {}",
                serde_yaml::to_string(&node.service).unwrap()
            );
            handle_error(service.create(&pp, &node.service).await)?;
            if let Some(api_service) = &node.api_service {
                debug!(
                    "Creating Api Service \n {}",
                    serde_yaml::to_string(&api_service).unwrap()
                );
                handle_error(service.create(&pp, api_service).await)?;
            }

            info!("Created Deployment/ConfigMap/Pvc for Node {node_name}");
        }

        return Ok(());
    }

    fn default_coordinator_spec() -> TopologyNode {
        TopologyNode {
            downstream: None,
            sources: None,
            resources: None,
        }
    }
    fn create(spec: &TopologySpec, owner_refernce: &OwnerReference) -> Result<Self> {
        let config = create_configurations(spec, owner_refernce);
        let coordinator_config = create_coordinator_config(&spec, owner_refernce);
        let name_to_node: HashMap<String, &TopologyNode> =
            spec.nodes.iter().map(|(name, n)| (name.clone(), n)).collect();

        let nodes = config
            .into_iter()
            .map(|(name, config)| {
                (
                    name.clone(),
                    K8SNode::lower_to_k8s(&name_to_node[&name], &config, &name, owner_refernce).unwrap(),
                )
            })
            .collect();

        Ok(K8STopology {
            coordinator: K8SNode::lower_to_k8s(
                &spec
                    .coordinator
                    .as_ref()
                    .map(|c| &c.worker)
                    .unwrap_or(&Self::default_coordinator_spec()),
                &coordinator_config,
                &"coordinator".to_string(),
                owner_refernce,
            )
            .unwrap(),
            nodes,
        })
    }

    fn diff(
        &self,
        observed_deployments: &Vec<Deployment>,
        observed_config_maps: &Vec<ConfigMap>,
        observed_pvcs: &Vec<PersistentVolumeClaim>,
        observed_services: &Vec<Service>,
    ) -> Vec<ChangeDetected> {
        let mut observed_deplyoments = observed_deployments
            .into_iter()
            .map(|d| (d.labels().get("topology-node-name").unwrap().clone(), d))
            .collect::<HashMap<String, _>>();

        let mut observed_services = observed_services
            .into_iter()
            .map(|d| (d.labels().get("topology-node-name").unwrap().clone(), d))
            .into_group_map();

        let mut observed_config_maps = observed_config_maps
            .into_iter()
            .map(|d| (d.labels().get("topology-node-name").unwrap().clone(), d))
            .collect::<HashMap<String, _>>();

        let mut observed_pvcs = observed_pvcs
            .into_iter()
            .map(|d| (d.labels().get("topology-node-name").unwrap().clone(), d))
            .into_group_map();

        // missing / outdated

        let to_delete = self
            .nodes
            .iter()
            .chain(std::iter::once((&"coordinator".to_string(), &self.coordinator)))
            .filter_map(|(node_name, node)| {
                let deployment = observed_deplyoments.remove(node_name);
                let config_map = observed_config_maps.remove(node_name);
                let pvcs = observed_pvcs.remove(node_name);
                let services = observed_services.remove(node_name);
                if let Err(e) = node.diff(deployment, config_map, pvcs.as_ref(), services.as_ref()) {
                    return Some(ChangeDetected::Nested(node_name.clone(), Box::from(e)));
                }
                None
            })
            .collect::<HashSet<ChangeDetected>>();

        observed_pvcs
            .into_keys()
            .map(|n| ChangeDetected::LeftOver(n.clone(), ResourceType::PVC(None)))
            .chain(
                observed_config_maps
                    .into_keys()
                    .map(|n| ChangeDetected::Missing(n, ResourceType::ConfigMap)),
            )
            .chain(
                observed_deplyoments
                    .into_keys()
                    .map(|n| ChangeDetected::Missing(n, ResourceType::Deploymnet)),
            )
            .chain(
                observed_services
                    .into_keys()
                    .map(|n| ChangeDetected::Missing(n, ResourceType::Service)),
            )
            .chain(to_delete.into_iter())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
    }
}

#[derive(Debug)]
struct K8SNode {
    name: String,
    pvcs: Vec<PersistentVolumeClaim>,
    deployment: Deployment,
    config_map: ConfigMap,
    service: Service,
    api_service: Option<Service>,
}

impl K8SNode {
    fn diff(
        &self,
        deployment: Option<&Deployment>,
        config_map: Option<&ConfigMap>,
        pvcs: Option<&Vec<&PersistentVolumeClaim>>,
        services: Option<&Vec<&Service>>,
    ) -> Result<(), ChangeDetected> {
        let deployment =
            deployment.ok_or_else(|| ChangeDetected::Missing(self.name.clone(), ResourceType::Deploymnet))?;
        let config_map =
            config_map.ok_or_else(|| ChangeDetected::Missing(self.name.clone(), ResourceType::ConfigMap))?;
        let default_vec = Vec::default();
        let pvcs = pvcs.unwrap_or(&default_vec);
        let default_vec2 = Vec::default();
        let services = services.unwrap_or(&default_vec2);
        let data_service = services
            .iter()
            .filter(|s| !s.metadata.labels.as_ref().unwrap().contains_key("api-service"))
            .next()
            .ok_or_else(|| ChangeDetected::Missing(self.name.clone(), ResourceType::Service))?;
        let api_service = services
            .iter()
            .filter(|s| s.metadata.labels.as_ref().unwrap().contains_key("api-service"))
            .next();

        if let Some(desired_api_service) = &self.api_service {
            match api_service {
                Some(api_service) => detect_spec_change(*api_service, desired_api_service)
                    .map_err(|e| ChangeDetected::Change(ResourceType::ApiService, e))?,
                None => {
                    return Err(ChangeDetected::Missing(
                        self.name.clone(),
                        ResourceType::ApiService,
                    ))
                }
            };
        } else {
            if api_service.is_some() {
                return Err(ChangeDetected::LeftOver(self.name.clone(), ResourceType::Service));
            }
        }

        detect_spec_change(&data_service.spec, &self.service.spec)
            .map_err(|e| ChangeDetected::Change(ResourceType::Service, e))?;
        detect_spec_change(&deployment.spec, &self.deployment.spec)
            .map_err(|e| ChangeDetected::Change(ResourceType::Deploymnet, e))?;
        detect_spec_change(&config_map.data, &self.config_map.data)
            .map_err(|e| ChangeDetected::Change(ResourceType::ConfigMap, e))?;
        let mut observed_pvcs_by_name = pvcs
            .into_iter()
            .map(|p| (p.metadata.name.clone(), p))
            .collect::<HashMap<_, _>>();

        for pvc in self.pvcs.iter() {
            if let Some(observed_pvc) = observed_pvcs_by_name.remove(&pvc.metadata.name) {
                detect_spec_change(*observed_pvc, &pvc).map_err(|e| {
                    ChangeDetected::Nested(
                        self.deployment.name_any().clone(),
                        Box::from(ChangeDetected::Change(
                            ResourceType::PVC(Some(observed_pvc.name_any().clone())),
                            e,
                        )),
                    )
                })?;
            } else {
                return Err(ChangeDetected::Missing(
                    self.name.clone(),
                    ResourceType::PVC(Some(pvc.metadata.name.as_ref().unwrap().clone())),
                ));
            }
        }

        if !observed_pvcs_by_name.is_empty() {
            return Err(ChangeDetected::LeftOver(
                self.name.clone(),
                ResourceType::PVC(observed_pvcs_by_name.into_iter().next().unwrap().0.clone()),
            ));
        }

        Ok(())
    }

    fn create_pvc(pv_name: String, node_name: &str, owner_refernce: OwnerReference) -> PersistentVolumeClaim {
        return PersistentVolumeClaim {
            metadata: ObjectMeta {
                name: Some(pv_name),
                owner_references: Some(vec![owner_refernce.clone()]),
                labels: Some(BTreeMap::from([
                    ("topology".to_string(), owner_refernce.name.clone()),
                    ("topology-node-name".to_string(), node_name.to_string()),
                    (format!("{LABEL_PREFIX}/{OWNED_BY}"), owner_refernce.name.clone()),
                ])),
                ..Default::default()
            },
            spec: Some(PersistentVolumeClaimSpec {
                access_modes: Some(vec!["ReadWriteOnce".to_string()]),
                resources: Some(k8s_openapi::api::core::v1::ResourceRequirements {
                    requests: Some(BTreeMap::from([(
                        "storage".to_string(),
                        Quantity("5Gi".to_string()),
                    )])),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
    }

    fn create_pv_mount(pv_name: String, path: String, source_name: String) -> (Volume, VolumeMount) {
        return (
            Volume {
                name: format!("{source_name}-pv"),
                persistent_volume_claim: Some(PersistentVolumeClaimVolumeSource {
                    claim_name: pv_name,
                    ..Default::default()
                }),
                ..Default::default()
            },
            VolumeMount {
                name: format!("{source_name}-pv"),
                mount_path: format!("/mnt/{source_name}"),
                ..Default::default()
            },
        );
    }

    pub fn lower_to_k8s(
        node: &TopologyNode,
        config: &NESConfig,
        node_name: &str,
        owner_refernce: &OwnerReference,
    ) -> Result<Self> {
        let worker_config = config.get_worker();
        let (pvcs, volumes): (Vec<PersistentVolumeClaim>, Vec<(Volume, VolumeMount)>) = node
            .sources
            .as_ref()
            .unwrap_or(&HashMap::default())
            .iter()
            .filter_map(|(n, ps)| match &ps.implementation {
                NodeSourceImplementation::CSVSource { pv_name, path, .. } => Some((n, pv_name, path)),
                _ => None,
            })
            .map(|(source_name, pv_name, path)| {
                (
                    Self::create_pvc(pv_name.clone(), node_name, owner_refernce.clone()),
                    Self::create_pv_mount(pv_name.clone(), path.clone(), source_name.to_string()),
                )
            })
            .unzip();

        let config_map = create_config_map(node_name, config, owner_refernce);
        let deployment = create_deployment(node_name, config, volumes, owner_refernce);
        let service = create_service(node_name, owner_refernce);
        let api_service = config
            .get_coordinator()
            .map(|cc| create_api_service(node_name, cc, owner_refernce));

        Ok(K8SNode {
            name: node_name.to_string(),
            pvcs,
            config_map,
            deployment,
            service,
            api_service,
        })
    }
}

#[test]
fn test_yaml_indention_fix() {
    let fixed = fix_yaml_indentation(
        "\
array:
- name: hello
  name2: world
  array2:
  - yourname: a
  - bname: 3
",
    );

    assert_eq!(
        fixed,
        "\
array:
 - name: hello
   name2: world
   array2:
    - yourname: a
    - bname: 3
",
    );

    let fixed = fix_yaml_indentation(
        "\
array:
- name: hello
- name2: world
- array2:
- yourname: a
- bname: 3
",
    );

    assert_eq!(
        fixed,
        "\
array:
 - name: hello
 - name2: world
 - array2:
 - yourname: a
 - bname: 3
",
    );

    let fixed = fix_yaml_indentation(
        "\
array:
- name: hello
  list2:     
  - list3:
    - listend: yes
new_value:3
",
    );

    assert_eq!(
        fixed,
        "\
array:
 - name: hello
   list2:     
    - list3:
       - listend: yes
new_value:3
",
    );
}

fn fix_yaml_indentation(yaml_conent: &str) -> String {
    let mut additional_indention = 0;
    let mut prev_indention = 0;
    let mut buffer: Vec<u8> = Vec::with_capacity(yaml_conent.len());

    for line in yaml_conent.lines() {
        let rest_of_line = line.trim_start();
        let current_indention = line.len() - rest_of_line.len();
        let start_new_array = rest_of_line.chars().next().map(|c| c == '-').unwrap_or(false);

        additional_indention = additional_indention + (current_indention as i32 - prev_indention as i32) / 2;

        prev_indention = current_indention;

        buffer.extend_from_slice(" ".repeat(additional_indention as usize).as_bytes());
        if start_new_array {
            buffer.extend_from_slice(b" ");
        }
        buffer.extend_from_slice(line.as_bytes());
        buffer.push(b'\n');
    }

    return String::from_utf8(buffer).unwrap();
}

fn get_service_name(node_name: &str, owner_reference: &OwnerReference) -> String {
    return format!("{node_name}-{}", owner_reference.name);
}

pub fn create_api_service(
    node_name: &str,
    coordinator_config: &nes_config::CoordinatorConfig,
    owner_refernce: &OwnerReference,
) -> Service {
    Service {
        metadata: ObjectMeta {
            name: Some(get_service_name("api", owner_refernce)),
            labels: Some(BTreeMap::from([
                ("topology".to_string(), owner_refernce.name.clone()),
                ("api-service".to_string(), "true".to_string()),
                ("topology-node-name".to_string(), node_name.to_string()),
                (format!("{LABEL_PREFIX}/{OWNED_BY}"), owner_refernce.name.clone()),
            ])),
            owner_references: Some(vec![owner_refernce.clone()]),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            selector: Some(BTreeMap::from([
                ("coordinator".to_string(), "true".to_string()),
                ("topology".to_string(), owner_refernce.name.clone()),
                ("topology-node-name".to_string(), node_name.to_string()),
            ])),
            ports: Some(vec![ServicePort {
                name: Some("api-port".to_string()),
                protocol: Some("TCP".to_string()),
                port: 80,
                target_port: Some(IntOrString::String("api-port".to_string())),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        ..Default::default()
    }
}

pub fn create_service(node_name: &str, owner_refernce: &OwnerReference) -> Service {
    Service {
        metadata: ObjectMeta {
            name: Some(get_service_name(node_name, owner_refernce)),
            labels: Some(BTreeMap::from([
                ("topology".to_string(), owner_refernce.name.clone()),
                ("topology-node-name".to_string(), node_name.to_string()),
                (format!("{LABEL_PREFIX}/{OWNED_BY}"), owner_refernce.name.clone()),
            ])),
            owner_references: Some(vec![owner_refernce.clone()]),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            selector: Some(BTreeMap::from([
                ("topology".to_string(), owner_refernce.name.clone()),
                ("topology-node-name".to_string(), node_name.to_string()),
            ])),
            ports: Some(vec![
                ServicePort {
                    name: Some("rpc-port".to_string()),
                    protocol: Some("TCP".to_string()),
                    port: RPC_PORT,
                    target_port: Some(IntOrString::String("rpc-port".to_string())),
                    ..Default::default()
                },
                ServicePort {
                    name: Some("data-port".to_string()),
                    protocol: Some("TCP".to_string()),
                    port: DATA_PORT,
                    target_port: Some(IntOrString::String("data-port".to_string())),
                    ..Default::default()
                },
            ]),
            ..Default::default()
        }),
        ..Default::default()
    }
}

pub fn get_config_map_name(node_name: &str, owner_reference: &OwnerReference) -> String {
    return format!("{node_name}-{}", owner_reference.name);
}
pub fn get_deployment_name(node_name: &str, owner_reference: &OwnerReference) -> String {
    return format!("{node_name}-{}", owner_reference.name);
}

#[derive(Serialize)]
enum NESConfig {
    Worker(WorkerConfig),
    Coordinator(CoordinatorConfig, WorkerConfig),
}

impl NESConfig {
    fn get_coordinator(&self) -> Option<&CoordinatorConfig> {
        match self {
            NESConfig::Worker(_) => None,
            NESConfig::Coordinator(cc, _) => Some(cc),
        }
    }

    fn get_worker(&self) -> &WorkerConfig {
        match self {
            NESConfig::Worker(wc) => wc,
            NESConfig::Coordinator(cc, wc) => &wc,
        }
    }
}

pub fn create_deployment(
    node_name: &str,
    config: &NESConfig,
    volumes: Vec<(Volume, VolumeMount)>,
    owner_refernce: &OwnerReference,
) -> Deployment {
    let coordinator_config = config.get_coordinator();
    let config = config.get_worker();

    let (mut volumes, mut mounts): (Vec<Volume>, Vec<VolumeMount>) = volumes.into_iter().unzip();
    mounts.push(VolumeMount {
        name: "config-volume".to_string(),
        mount_path: "/config/".to_string(),
        ..Default::default()
    });

    volumes.push(Volume {
        name: "config-volume".to_string(),
        config_map: Some(ConfigMapVolumeSource {
            name: Some(get_config_map_name(node_name, owner_refernce)),
            default_mode: Some(420),
            ..Default::default()
        }),
        ..Default::default()
    });

    let mut deployment = Deployment {
        metadata: ObjectMeta {
            name: Some(get_deployment_name(node_name, owner_refernce)),
            labels: Some(BTreeMap::from([
                ("topology".to_string(), owner_refernce.name.clone()),
                ("topology-node-name".to_string(), node_name.to_string()),
                (format!("{LABEL_PREFIX}/{OWNED_BY}"), owner_refernce.name.clone()),
            ])),
            owner_references: Some(vec![owner_refernce.clone()]),
            ..Default::default()
        },
        spec: Some(DeploymentSpec {
            selector: k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector {
                match_labels: Some(BTreeMap::from([
                    ("topology".to_string(), owner_refernce.name.clone()),
                    ("topology-node-name".to_string(), node_name.to_string()),
                ])),
                ..Default::default()
            },

            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(BTreeMap::from([
                        ("topology".to_string(), owner_refernce.name.clone()),
                        ("topology-node-name".to_string(), node_name.to_string()),
                    ])),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    termination_grace_period_seconds: Some(1),
                    containers: vec![Container {
                        image: Some(get_nes_executable_image()),
                        image_pull_policy: Some("Always".to_string()),
                        ports: Some(vec![
                            ContainerPort {
                                container_port: RPC_PORT,
                                name: Some("rpc-port".to_string()),
                                ..Default::default()
                            },
                            ContainerPort {
                                container_port: DATA_PORT,
                                name: Some("data-port".to_string()),
                                ..Default::default()
                            },
                        ]),
                        command: get_worker_command(),
                        args: get_worker_command_args(&config),
                        env: Some(vec![EnvVar {
                            name: "POD_IP".to_string(),
                            value_from: Some(k8s_openapi::api::core::v1::EnvVarSource {
                                field_ref: Some(k8s_openapi::api::core::v1::ObjectFieldSelector {
                                    field_path: "status.podIP".to_string(),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            }),
                            ..Default::default()
                        }]),
                        name: "executable".to_string(),
                        volume_mounts: Some(mounts),
                        ..Default::default()
                    }],
                    volumes: Some(volumes),
                    ..Default::default()
                }),
                ..Default::default()
            },
            ..Default::default()
        }),
        ..Default::default()
    };

    if let Some(coordinator_config) = &coordinator_config {
        deployment
            .metadata
            .labels
            .as_mut()
            .unwrap()
            .insert("coordinator".to_string(), "true".to_string());

        deployment
            .spec
            .as_mut()
            .unwrap()
            .template
            .metadata
            .as_mut()
            .unwrap()
            .labels
            .as_mut()
            .unwrap()
            .insert("coordinator".to_string(), "true".to_string());

        deployment
            .spec
            .as_mut()
            .unwrap()
            .template
            .spec
            .as_mut()
            .unwrap()
            .containers[0]
            .command = get_coordinator_command();

        deployment
            .spec
            .as_mut()
            .unwrap()
            .template
            .spec
            .as_mut()
            .unwrap()
            .containers[0]
            .args = get_coordinator_command_args();

        deployment
            .spec
            .as_mut()
            .unwrap()
            .template
            .spec
            .as_mut()
            .unwrap()
            .containers[0]
            .ports
            .as_mut()
            .unwrap()
            .push(ContainerPort {
                container_port: coordinator_config.rest_port,
                name: Some("api-port".to_string()),
                ..Default::default()
            });
    }

    return deployment;
}

// Context for our reconciler
#[derive(Clone)]
pub struct Context {
    /// Kubernetes client
    pub client: Client,
    /// Diagnostics read by the web server
    pub diagnostics: Arc<RwLock<Diagnostics>>,
    /// Prometheus metrics
    pub metrics: Metrics,
}

enum ObservedState {
    Submitted,
    Pending { deployments: Vec<Deployment> },
    Running { deployments: Vec<Deployment> },
}

#[instrument(skip(ctx, doc), fields(trace_id))]
async fn reconcile(doc: Arc<Topology>, ctx: Arc<Context>) -> Result<Action> {
    let trace_id = telemetry::get_trace_id();
    Span::current().record("trace_id", &field::display(&trace_id));
    let _timer = ctx.metrics.count_and_measure();
    ctx.diagnostics.write().await.last_event = Utc::now();
    let ns = doc.namespace().unwrap(); // doc is namespace scoped
    let docs: Api<Topology> = Api::namespaced(ctx.client.clone(), &ns);

    info!("Reconciling Topology \"{}\" in {}", doc.name_any(), ns);
    finalizer(&docs, TOPLOGY_FINALIZER, doc, |event| async {
        match event {
            Finalizer::Apply(doc) => doc.reconcile(ctx.clone()).await,
            Finalizer::Cleanup(doc) => doc.cleanup(ctx.clone()).await,
        }
    })
    .await
    .map_err(|e| Error::FinalizerError(Box::new(e)))
}

fn error_policy(doc: Arc<Topology>, error: &Error, ctx: Arc<Context>) -> Action {
    warn!("reconcile failed: {:?}", error);
    ctx.metrics.reconcile_failure(&doc, error);
    Action::requeue(std::time::Duration::from_secs(5 * 60))
}

impl Topology {
    // Reconcile (for non-finalizer related changes)
    async fn reconcile(&self, ctx: Arc<Context>) -> Result<Action> {
        let client = ctx.client.clone();
        // let recorder = ctx.diagnostics.read().await.recorder(client.clone(), self);
        let ns = self.namespace().unwrap();
        let name = self.name_any();
        let docs: Api<Topology> = Api::namespaced(client, &ns);

        if name == "illegal" {
            return Err(Error::IllegalTopology); // error names show up in metrics
        }

        let status = match self.observe(ctx.clone()).await? {
            ObservedState::Submitted => self.create_topology(ctx.clone()).await?,
            ObservedState::Running { deployments } => self.update_status(deployments).await?,
            ObservedState::Pending { deployments } => self.update_status(deployments).await?,
        };

        // always overwrite status object with what we saw
        let new_status = Patch::Apply(json!({
            "apiVersion": "kube.rs/v1",
            "kind": "Topology",
            "status":status       }));
        let ps = PatchParams::apply("cntrlr").force();
        let _o = docs
            .patch_status(&name, &ps, &new_status)
            .await
            .map_err(Error::KubeError)?;

        // If no events were received, check back every 5 minutes
        Ok(Action::requeue(std::time::Duration::from_secs(5 * 60)))
    }

    async fn update_status(&self, deployments: Vec<Deployment>) -> Result<TopologyStatus> {
        return Ok(TopologyStatus {
            status_string: "Pending".to_string(),
            deployment_created: true,
            config_maps_created: true,
            deployment_pending: deployments.len(),
            deployment_ready: 0,
        });
    }

    async fn create_topology(&self, ctx: Arc<Context>) -> Result<TopologyStatus> {
        let ns = self.namespace().unwrap(); // doc is namespace scoped
        let deployments: Api<Deployment> = Api::namespaced(ctx.client.clone(), &ns);
        let services: Api<Service> = Api::namespaced(ctx.client.clone(), &ns);
        let config_maps: Api<ConfigMap> = Api::namespaced(ctx.client.clone(), &ns);
        let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(ctx.client.clone(), &ns);
        let owner_reference = self.controller_owner_ref(&()).unwrap();

        info!("Creating Topology");
        let k8s_topology = K8STopology::create(&self.spec, &owner_reference)?;
        k8s_topology
            .post_to_api(deployments, services, config_maps, pvcs)
            .await?;

        info!("Creation done!");

        return Ok(TopologyStatus {
            status_string: "Submitted".to_string(),
            deployment_created: true,
            config_maps_created: true,
            deployment_pending: self.spec.nodes.len(),
            deployment_ready: 0,
        });
    }

    #[async_recursion]
    async fn handle_change_detection(
        change: &ChangeDetected,
        deployments: &Api<Deployment>,
        services: &Api<Service>,
        config_maps: &Api<ConfigMap>,
        pvcs: &Api<PersistentVolumeClaim>,
        owner_reference: &OwnerReference,
        nest_context: Option<String>,
    ) {
        match change {
            ChangeDetected::Change(rt, ..) => {
                match rt {
                    ResourceType::Deploymnet => {
                        info!("Deleting Deploymenten for node");
                        let _ = deployments
                            .delete(
                                &get_deployment_name(&nest_context.unwrap(), owner_reference),
                                &DeleteParams::default(),
                            )
                            .await
                            .unwrap();
                    }
                    ResourceType::PVC(pvc_name) => {
                        info!("Deleting PVC and Deploymenten for node");
                        let _ = deployments
                            .delete(
                                &get_deployment_name(&nest_context.unwrap(), owner_reference),
                                &DeleteParams::default(),
                            )
                            .await
                            .unwrap();

                        let _ = pvcs
                            .delete(
                                &pvc_name.as_ref().expect("pvc name was missing"),
                                &DeleteParams::default(),
                            )
                            .await
                            .unwrap();
                    }
                    ResourceType::ConfigMap => {
                        info!("Deleting ConfigMap for node");
                        let _ = config_maps
                            .delete(
                                &get_config_map_name(&nest_context.as_ref().unwrap(), owner_reference),
                                &DeleteParams::default(),
                            )
                            .await
                            .unwrap();
                        // this refreshes the config maps
                        info!("Deleting Deploymenten for node");
                        let _ = deployments
                            .delete(
                                &get_deployment_name(&nest_context.unwrap(), owner_reference),
                                &DeleteParams::default(),
                            )
                            .await
                            .unwrap();
                    }
                    ResourceType::ApiService => {
                        info!("Deleting Api Service for node");
                        let _ = services
                            .delete(
                                &get_deployment_name("api", owner_reference),
                                &DeleteParams::default(),
                            )
                            .await
                            .unwrap();
                    }
                    ResourceType::Service => {
                        info!("Deleting Service for node {}", nest_context.as_ref().unwrap());
                        let _ = services
                            .delete(
                                &get_deployment_name(&nest_context.unwrap(), owner_reference),
                                &DeleteParams::default(),
                            )
                            .await
                            .unwrap();
                    }
                };
            }
            ChangeDetected::Nested(node_name, change) => {
                info!("Nested changed: {node_name}: {change:?}");
                Self::handle_change_detection(
                    change,
                    deployments,
                    services,
                    config_maps,
                    pvcs,
                    owner_reference,
                    Some(node_name.to_string()),
                )
                .await;
            }
            ChangeDetected::LeftOver(node_name, rt) => {
                info!("LeftOver {rt:?}: {node_name}");
                match rt {
                    ResourceType::Service => {
                        info!("Deleting Service {node_name}");
                        let _ = services
                            .delete(
                                &get_service_name(node_name, owner_reference),
                                &DeleteParams::default(),
                            )
                            .await;
                    }
                    ResourceType::ApiService => {
                        info!("Deleting Api Service");
                        let _ = services
                            .delete(
                                &get_service_name("api", owner_reference),
                                &DeleteParams::default(),
                            )
                            .await;
                    }
                    ResourceType::PVC(Some(pvc_name)) => {
                        info!("Deleting PVC for {node_name}: {pvc_name}");
                        let _ = pvcs.delete(&pvc_name, &DeleteParams::default()).await.unwrap();
                    }
                    ResourceType::PVC(None) => {
                        error!("Left over PVC, with unspecifed name");
                    }
                    ResourceType::Deploymnet => {
                        info!("Deleting Deployment {node_name}");
                        let _ = deployments
                            .delete(
                                &get_deployment_name(node_name, owner_reference),
                                &DeleteParams::default(),
                            )
                            .await
                            .unwrap();
                    }
                    ResourceType::ConfigMap => {
                        info!("Deleting Config Map {node_name}");
                        let _ = config_maps
                            .delete(&node_name, &DeleteParams::default())
                            .await
                            .unwrap();
                    }
                }
            }
            ChangeDetected::Missing(node_name, rt) => {
                info!("Missing {rt:?}: {node_name}");
            }
        }
    }

    async fn observe(&self, ctx: Arc<Context>) -> Result<ObservedState> {
        let name = self.name_any();
        let ns = self.namespace().unwrap(); // doc is namespace scoped
        let deployment_api: Api<Deployment> = Api::namespaced(ctx.client.clone(), &ns);
        let service_api: Api<Service> = Api::namespaced(ctx.client.clone(), &ns);
        let config_map_api: Api<ConfigMap> = Api::namespaced(ctx.client.clone(), &ns);
        let pvcs_api: Api<PersistentVolumeClaim> = Api::namespaced(ctx.client.clone(), &ns);
        let owner_reference = self.controller_owner_ref(&()).unwrap();
        let mut find_owned_resources_options = ListParams::default();
        find_owned_resources_options
            .label_selector
            .replace(format!("{LABEL_PREFIX}/{OWNED_BY}={name}"));

        let (services, deployments, config_maps, pvcs) = tokio::join!(
            service_api.list(&find_owned_resources_options),
            deployment_api.list(&find_owned_resources_options),
            config_map_api.list(&find_owned_resources_options),
            pvcs_api.list(&find_owned_resources_options)
        );

        let services = services.map_err(|e| Error::KubeError(e))?;
        let deployments = deployments.map_err(|e| Error::KubeError(e))?;
        let config_maps = config_maps.map_err(|e| Error::KubeError(e))?;
        let pvcs = pvcs.map_err(|e| Error::KubeError(e))?;

        let k8s_topology = K8STopology::create(&self.spec, &owner_reference)?;
        let to_delete = k8s_topology.diff(
            &deployments.items,
            &config_maps.items,
            &pvcs.items,
            &services.items,
        );

        if !to_delete.is_empty() {
            for change in to_delete.into_iter() {
                Self::handle_change_detection(
                    &change,
                    &deployment_api,
                    &service_api,
                    &config_map_api,
                    &pvcs_api,
                    &owner_reference,
                    None,
                )
                .await
            }

            return Ok(ObservedState::Submitted);
        }

        let deployments_ready = deployments.iter().all(|pod| {
            if let Some(status) = &pod.status {
                let mut container_ready = false;
                let mut error = false;
                for condition in status.conditions.as_ref().unwrap_or(&Vec::default()).iter() {
                    container_ready = container_ready || condition.type_ == "ContainerReady";
                    error = error || condition.type_ == "ContainerErro";
                }

                if container_ready && !error {
                    return true;
                }
            }
            return false;
        });

        if !deployments_ready {
            return Ok(ObservedState::Pending {
                deployments: deployments.into_iter().map(|p| p).collect(),
            });
        }

        return Ok(ObservedState::Running {
            deployments: deployments.into_iter().map(|p| p).collect(),
        });
    }

    // Finalizer cleanup (the object was deleted, ensure nothing is orphaned)
    async fn cleanup(&self, ctx: Arc<Context>) -> Result<Action> {
        let name = self.name_any();
        let recorder = ctx.diagnostics.read().await.recorder(ctx.client.clone(), self);
        // Topology doesn't have any real cleanup, so we just publish an event

        let mut find_owned_resources_options = ListParams::default();
        find_owned_resources_options
            .label_selector
            .replace(format!("{LABEL_PREFIX}/{OWNED_BY}={name}"));

        let ns = self.namespace().unwrap(); // doc is namespace scoped
        let deployments: Api<Deployment> = Api::namespaced(ctx.client.clone(), &ns);
        let config_maps: Api<ConfigMap> = Api::namespaced(ctx.client.clone(), &ns);

        deployments
            .delete_collection(&DeleteParams::default(), &find_owned_resources_options)
            .await
            .map_err(|e| Error::KubeError(e))?;
        config_maps
            .delete_collection(&DeleteParams::default(), &find_owned_resources_options)
            .await
            .map_err(|e| Error::KubeError(e))?;

        recorder
            .publish(Event {
                type_: EventType::Normal,
                reason: "DeleteRequested".into(),
                note: Some(format!("Delete `{}`", self.name_any())),
                action: "Deleting".into(),
                secondary: None,
            })
            .await
            .map_err(Error::KubeError)?;
        Ok(Action::await_change())
    }
}

/// Diagnostics to be exposed by the web server
#[derive(Clone, Serialize)]
pub struct Diagnostics {
    #[serde(deserialize_with = "from_ts")]
    pub last_event: DateTime<Utc>,
    #[serde(skip)]
    pub reporter: Reporter,
}
impl Default for Diagnostics {
    fn default() -> Self {
        Self {
            last_event: Utc::now(),
            reporter: "doc-controller".into(),
        }
    }
}
impl Diagnostics {
    fn recorder(&self, client: Client, doc: &Topology) -> Recorder {
        Recorder::new(client, self.reporter.clone(), doc.object_ref(&()))
    }
}

/// State shared between the controller and the web server
#[derive(Clone, Default)]
pub struct State {
    /// Diagnostics populated by the reconciler
    diagnostics: Arc<RwLock<Diagnostics>>,
    /// Metrics registry
    registry: prometheus::Registry,
}

/// State wrapper around the controller outputs for the web server
impl State {
    /// Metrics getter
    pub fn metrics(&self) -> Vec<prometheus::proto::MetricFamily> {
        self.registry.gather()
    }

    /// State getter
    pub async fn diagnostics(&self) -> Diagnostics {
        self.diagnostics.read().await.clone()
    }

    // Create a Controller Context that can update State
    pub fn to_context(&self, client: Client) -> Arc<Context> {
        Arc::new(Context {
            client,
            metrics: Metrics::default().register(&self.registry).unwrap(),
            diagnostics: self.diagnostics.clone(),
        })
    }
}

/// Initialize the controller and shared state (given the crd is installed)
pub async fn run(state: State) {
    let mut config = kube::Config::infer().await.expect("could not infer config");
    let client = Client::try_from(config).expect("could not create client");
    let docs = Api::<Topology>::all(client.clone());
    let deployment = Api::<Deployment>::all(client.clone());
    let service = Api::<Service>::all(client.clone());
    let config_maps = Api::<ConfigMap>::all(client.clone());
    if let Err(e) = docs.list(&ListParams::default().limit(1)).await {
        error!("CRD is not queryable; {e:?}. Is the CRD installed?");
        info!("Installation: cargo run --bin crdgen | kubectl apply -f -");
        std::process::exit(1);
    }
    Controller::new(docs, kube::runtime::watcher::Config::default().any_semantic())
        .shutdown_on_signal()
        .owns(service, kube::runtime::watcher::Config::default().any_semantic())
        .owns(
            deployment,
            kube::runtime::watcher::Config::default().any_semantic(),
        )
        .owns(
            config_maps,
            kube::runtime::watcher::Config::default().any_semantic(),
        )
        .run(reconcile, error_policy, state.to_context(client))
        .filter_map(|x| async move { std::result::Result::ok(x) })
        .for_each(|_| future::ready(()))
        .await;
}

// Mock tests relying on fixtures.rs and its primitive apiserver mocks
#[cfg(test)]
mod test {
    use super::{error_policy, reconcile, Context, Topology};
    use crate::fixtures::{timeout_after_1s, Scenario};
    use std::sync::Arc;

    #[tokio::test]
    async fn documents_without_finalizer_gets_a_finalizer() {
        let (testctx, fakeserver, _) = Context::test();
        let doc = Topology::test();
        let mocksrv = fakeserver.run(Scenario::FinalizerCreation(doc.clone()));
        reconcile(Arc::new(doc), testctx).await.expect("reconciler");
        timeout_after_1s(mocksrv).await;
    }

    #[tokio::test]
    async fn finalized_doc_causes_status_patch() {
        let (testctx, fakeserver, _) = Context::test();
        let doc = Topology::test().finalized();
        let mocksrv = fakeserver.run(Scenario::StatusPatch(doc.clone()));
        reconcile(Arc::new(doc), testctx).await.expect("reconciler");
        timeout_after_1s(mocksrv).await;
    }

    #[tokio::test]
    async fn finalized_doc_with_delete_timestamp_causes_delete() {
        let (testctx, fakeserver, _) = Context::test();
        let doc = Topology::test().finalized().needs_delete();
        let mocksrv = fakeserver.run(Scenario::Cleanup("DeleteRequested".into(), doc.clone()));
        reconcile(Arc::new(doc), testctx).await.expect("reconciler");
        timeout_after_1s(mocksrv).await;
    }

    #[tokio::test]
    async fn illegal_doc_reconcile_errors_which_bumps_failure_metric() {
        let (testctx, fakeserver, _registry) = Context::test();
        let doc = Arc::new(Topology::illegal().finalized());
        let mocksrv = fakeserver.run(Scenario::RadioSilence);
        let res = reconcile(doc.clone(), testctx.clone()).await;
        timeout_after_1s(mocksrv).await;
        assert!(res.is_err(), "apply reconciler fails on illegal doc");
        let err = res.unwrap_err();
        assert!(err.to_string().contains("IllegalTopology"));
        // calling error policy with the reconciler error should cause the correct metric to be set
        error_policy(doc.clone(), &err, testctx.clone());
        //dbg!("actual metrics: {}", registry.gather());
        let failures = testctx
            .metrics
            .failures
            .with_label_values(&["illegal", "finalizererror(applyfailed(illegaldocument))"])
            .get();
        assert_eq!(failures, 1);
    }

    // Integration test without mocks
    use kube::api::{Api, ListParams, Patch, PatchParams};
    #[tokio::test]
    #[ignore = "uses k8s current-context"]
    async fn integration_reconcile_should_set_status_and_send_event() {
        let client = kube::Client::try_default().await.unwrap();
        let ctx = super::State::default().to_context(client.clone());

        // create a test doc
        let doc = Topology::test().finalized();
        let docs: Api<Topology> = Api::namespaced(client.clone(), "default");
        let ssapply = PatchParams::apply("ctrltest");
        let patch = Patch::Apply(doc.clone());
        docs.patch("test", &ssapply, &patch).await.unwrap();

        // reconcile it (as if it was just applied to the cluster like this)
        reconcile(Arc::new(doc), ctx).await.unwrap();

        // verify side-effects happened
        let output = docs.get_status("test").await.unwrap();
        assert!(output.status.is_some());
        // verify hide event was found
        let events: Api<k8s_openapi::api::core::v1::Event> = Api::all(client.clone());
        let opts = ListParams::default().fields("involvedObject.kind=Topology,involvedObject.name=test");
        let event = events
            .list(&opts)
            .await
            .unwrap()
            .into_iter()
            .filter(|e| e.reason.as_deref() == Some("HideRequested"))
            .last()
            .unwrap();
        assert_eq!(event.action.as_deref(), Some("Hiding"));
    }
}
