package mario.tfm.parameters

case class Config(bootstrap_servers: String = "", application_id: String = "", client_id: String = "",
                  commit_interval_ms: String = "", num_stream_threads: String = "", az_account_name: String = "",
                  az_account_key: String = "", az_container_name: String = "", az_path: String = "",
                  topic_name: String = "", errors_topic: String = "", format: String = "")
