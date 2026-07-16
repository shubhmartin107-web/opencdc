use opencdc_core::connector_type::ConnectorType;

pub struct DebeziumNaming;

impl DebeziumNaming {
    pub fn topic_name(prefix: &str, schema: Option<&str>, table: &str) -> String {
        match schema {
            Some(s) => format!("{}.{}.{}", prefix, s, table),
            None => format!("{}.{}", prefix, table),
        }
    }

    pub fn table_schema_name(
        connector: &ConnectorType,
        db: &str,
        schema: Option<&str>,
        table: &str,
    ) -> String {
        let prefix = format!("{}.{}", connector.as_str(), db);
        match schema {
            Some(s) => format!("{}.{}.{}", prefix, s, table),
            None => format!("{}.{}", prefix, table),
        }
    }

    pub fn envelope_name(
        connector: &ConnectorType,
        db: &str,
        schema: Option<&str>,
        table: &str,
    ) -> String {
        format!(
            "{}.Envelope",
            Self::table_schema_name(connector, db, schema, table)
        )
    }

    pub fn key_name(
        connector: &ConnectorType,
        db: &str,
        schema: Option<&str>,
        table: &str,
    ) -> String {
        format!(
            "{}.Key",
            Self::table_schema_name(connector, db, schema, table)
        )
    }

    pub fn source_name(connector: &ConnectorType) -> String {
        format!("io.debezium.connector.{}.Source", connector.as_str())
    }

    pub fn subject_name(prefix: &str, schema: Option<&str>, table: &str, suffix: &str) -> String {
        let base = match schema {
            Some(s) => format!("{}.{}.{}", prefix, s, table),
            None => format!("{}.{}", prefix, table),
        };
        format!("{}-{}", base, suffix)
    }

    pub fn value_subject(prefix: &str, schema: Option<&str>, table: &str) -> String {
        Self::subject_name(prefix, schema, table, "value")
    }

    pub fn key_subject(prefix: &str, schema: Option<&str>, table: &str) -> String {
        Self::subject_name(prefix, schema, table, "key")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_name() {
        assert_eq!(
            DebeziumNaming::topic_name("opencdc", Some("public"), "users"),
            "opencdc.public.users"
        );
        assert_eq!(
            DebeziumNaming::topic_name("opencdc", None, "events"),
            "opencdc.events"
        );
    }

    #[test]
    fn test_envelope_and_key_names() {
        let ct = ConnectorType::Postgres;
        assert_eq!(
            DebeziumNaming::envelope_name(&ct, "mydb", Some("public"), "users"),
            "postgresql.mydb.public.users.Envelope"
        );
        assert_eq!(
            DebeziumNaming::key_name(&ct, "mydb", Some("public"), "users"),
            "postgresql.mydb.public.users.Key"
        );
    }

    #[test]
    fn test_source_name() {
        assert_eq!(
            DebeziumNaming::source_name(&ConnectorType::Postgres),
            "io.debezium.connector.postgresql.Source"
        );
        assert_eq!(
            DebeziumNaming::source_name(&ConnectorType::Mysql),
            "io.debezium.connector.mysql.Source"
        );
    }

    #[test]
    fn test_subject_names() {
        assert_eq!(
            DebeziumNaming::value_subject("opencdc", Some("public"), "users"),
            "opencdc.public.users-value"
        );
        assert_eq!(
            DebeziumNaming::key_subject("opencdc", Some("public"), "users"),
            "opencdc.public.users-key"
        );
    }
}
