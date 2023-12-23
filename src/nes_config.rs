use serde::Serialize;

#[derive(Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum LogLevel {
    LogError,
    LogInfo,
    LogDebug,
    LogTrace,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PhysicalSourceConfig {
    #[serde(flatten)]
    source_type: PhysicalSourceTypeConfig,
    logical_source_name: String,
    physical_source_name: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TCPConfiguration {
    socket_host: String,
    socket_port: usize,
    socket_domain: String,
    socket_type: String,
    #[serde(rename = "flushIntervalMS")]
    flush_interval_ms: usize,
    input_format: String,
    decide_message_size: String,
    tuple_separator: String,
}

#[derive(Serialize)]
#[serde(tag = "type")]
enum PhysicalSourceTypeConfig {
    TCPConfig { configuration: TCPConfiguration },
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Config {
    log_level: LogLevel,
    local_worker_ip: String,
    coordinator_ip: String,
    number_of_slots: usize,
    worker_id: usize,
    parent_id: usize,
    physical_source: Option<PhysicalSourceConfig>,
}

#[test]
fn test_serialization() {
    let config = Config {
        log_level: LogLevel::LogError,
        local_worker_ip: "worker".to_string(),
        coordinator_ip: "coordinator".to_string(),
        physical_source: Some(PhysicalSourceConfig {
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
        }),
        number_of_slots: 5,
        worker_id: 1,
        parent_id: 0,
    };

    assert_eq!(serde_yaml::to_string(&config).unwrap(), "");
}
