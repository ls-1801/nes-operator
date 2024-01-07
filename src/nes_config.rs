use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tracing::warn;

#[derive(Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum LogLevel {
    LogError,
    LogInfo,
    LogDebug,
    LogTrace,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PhysicalSourceConfig {
    #[serde(flatten)]
    pub(crate) source_type: PhysicalSourceTypeConfig,
    pub(crate) logical_source_name: String,
    pub(crate) physical_source_name: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CSVConfiguration {
    pub(crate) file_path: String,
    pub(crate) skip_header: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TCPConfiguration {
    pub(crate) socket_host: String,
    pub(crate) socket_port: usize,
    pub(crate) socket_domain: String,
    pub(crate) socket_type: String,
    #[serde(rename = "flushIntervalMS")]
    pub(crate) flush_interval_ms: usize,
    pub(crate) input_format: String,
    pub(crate) decide_message_size: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) tuple_separator: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct KafkaConfiguration {
    pub(crate) brokers: String,
    pub(crate) auto_commit: usize,
    pub(crate) group_id: String,
    pub(crate) topic: String,
    pub(crate) offset_mode: String,
    pub(crate) connection_timeout: usize,
    pub(crate) batch_size: usize,
    pub(crate) input_format: String,
}

#[derive(Serialize)]
#[serde(tag = "type")]
pub(crate) enum PhysicalSourceTypeConfig {
    #[serde(rename = "TCP_SOURCE")]
    TCPConfig { configuration: TCPConfiguration },
    #[serde(rename = "CSV_SOURCE")]
    CSVConfig { configuration: CSVConfiguration },
    #[serde(rename = "KAFKA_SOURCE")]
    KafkaConfig { configuration: KafkaConfiguration },
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WorkerConfig {
    pub(crate) log_level: LogLevel,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) local_worker_ip: Option<String>,
    pub(crate) coordinator_ip: String,
    pub(crate) number_of_slots: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) worker_id: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) parent_id: Option<usize>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) physical_sources: Vec<PhysicalSourceConfig>,
    pub(crate) data_port: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) num_worker_threads: Option<usize>,
    pub(crate) rpc_port: i32,
    pub(crate) coordinator_port: i32,
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
pub(crate) enum NESSchemaType {
    FLOAT64,
    FLOAT32,
    INT64,
    INT32,
    INT16,
    INT8,
    UINT64,
    UINT32,
    UINT16,
    UINT8,
    TEXT,
    CHAR,
}
#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub(crate) struct LogicalSourceConfigField {
    pub(crate) name: String,
    #[serde(rename = "type")]
    pub(crate) type_: NESSchemaType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) length: Option<usize>,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct LogicalSourceConfig {
    pub(crate) logical_source_name: String,
    pub(crate) fields: Vec<LogicalSourceConfigField>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CoordinatorConfig {
    pub(crate) logical_sources: Vec<LogicalSourceConfig>,
    pub(crate) rest_port: i32,
    pub(crate) rest_ip: String,
    pub(crate) coordinator_ip: String,
    pub(crate) log_level: LogLevel,
    pub(crate) rpc_port: i32,
}

#[test]
fn test_serialization() {
    let config = WorkerConfig {
        log_level: LogLevel::LogError,
        local_worker_ip: Some("worker".to_string()),
        coordinator_port: 8333,
        data_port: 5333,
        rpc_port: 5333,
        num_worker_threads: Some(433),
        coordinator_ip: "coordinator".to_string(),
        physical_sources: vec![PhysicalSourceConfig {
            source_type: PhysicalSourceTypeConfig::TCPConfig {
                configuration: TCPConfiguration {
                    socket_host: "source1".to_string(),
                    socket_port: 8080,
                    socket_domain: "AF_INET".to_string(),
                    socket_type: "AF_STREAM".to_string(),
                    flush_interval_ms: 100,
                    input_format: "JSON".to_string(),
                    decide_message_size: "TUPLE_SEPARATOR".to_string(),
                    tuple_separator: "|".to_string(),
                },
            },
            logical_source_name: "source1".to_string(),
            physical_source_name: "source1_phys".to_string(),
        }],
        number_of_slots: 5,
        worker_id: Some(1),
        parent_id: Some(0),
    };

    // assert_eq!(serde_yaml::to_string(&config).unwrap(), "");

    let config = LogicalSourceConfig {
        logical_source_name: "Yes".to_string(),
        fields: vec![LogicalSourceConfigField {
            name: "name".to_string(),
            type_: NESSchemaType::CHAR { length: 50 },
        }],
    };

    assert_eq!(serde_yaml::to_string(&config).unwrap(), "");
}
