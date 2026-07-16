use opencdc_core::schema::{DebeziumField, DebeziumSchema, DebeziumSchemaType};

pub struct SchemaBuilder {
    schema: DebeziumSchema,
}

impl SchemaBuilder {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            schema: DebeziumSchema {
                schema_type: DebeziumSchemaType::Struct,
                fields: Some(Vec::new()),
                name: Some(name.into()),
                version: Some(1),
                ..Default::default()
            },
        }
    }

    pub fn new_envelope(connector: &str, catalog: &str, schema: Option<&str>, table: &str) -> Self {
        let namespace = match schema {
            Some(s) => format!("{}.{}.{}", connector, catalog, s),
            None => format!("{}.{}", connector, catalog),
        };
        let envelope_name = format!("{}.{}.Envelope", namespace, table);

        let mut builder = Self::new(&envelope_name);
        builder = builder
            .add_field(
                DebeziumField::struct_field("before", Vec::new())
                    .optional()
                    .named(format!("{}.{}", namespace, table)),
            )
            .add_field(
                DebeziumField::struct_field("after", Vec::new())
                    .optional()
                    .named(format!("{}.{}", namespace, table)),
            )
            .add_field(
                DebeziumField::struct_field("source", Self::source_fields())
                    .named(format!("io.debezium.connector.{}.Source", connector)),
            )
            .add_field(DebeziumField::string("op"))
            .add_field(DebeziumField::int64("ts_ms").optional())
            .add_field(
                DebeziumField::struct_field(
                    "transaction",
                    vec![
                        DebeziumField::string("id"),
                        DebeziumField::int64("total_order"),
                        DebeziumField::int64("data_collections_order"),
                    ],
                )
                .optional(),
            );

        builder
    }

    pub fn add_field(mut self, field: DebeziumField) -> Self {
        self.schema.fields.get_or_insert_with(Vec::new).push(field);
        self
    }

    pub fn with_column(mut self, column: DebeziumField) -> Self {
        // Add to both 'before' and 'after' struct fields
        if let Some(fields) = &mut self.schema.fields {
            for f in fields.iter_mut() {
                if f.field_name == "before" || f.field_name == "after" {
                    f.fields.get_or_insert_with(Vec::new).push(column.clone());
                }
            }
        }
        self
    }

    pub fn with_version(mut self, version: u32) -> Self {
        self.schema.version = Some(version);
        self
    }

    pub fn build(self) -> DebeziumSchema {
        self.schema
    }

    fn source_fields() -> Vec<DebeziumField> {
        vec![
            DebeziumField::string("version"),
            DebeziumField::string("connector"),
            DebeziumField::string("name"),
            DebeziumField::int64("ts_ms"),
            DebeziumField::string("snapshot").optional(),
            DebeziumField::string("db"),
            DebeziumField::string("schema").optional(),
            DebeziumField::string("table"),
            DebeziumField::int64("txId").optional(),
            DebeziumField::int64("lsn").optional(),
            DebeziumField::string("file").optional(),
            DebeziumField::int64("pos").optional(),
            DebeziumField::string("gtid").optional(),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_basic() {
        let schema = SchemaBuilder::new("io.opencdc.test.public.users.Envelope")
            .add_field(DebeziumField::int32("id"))
            .add_field(DebeziumField::string("name").optional())
            .build();

        assert_eq!(
            schema.name.as_deref(),
            Some("io.opencdc.test.public.users.Envelope")
        );
        let fields = schema.fields.as_ref().unwrap();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].field_name, "id");
    }

    #[test]
    fn test_builder_envelope() {
        let schema = SchemaBuilder::new_envelope("opencdc", "testdb", Some("public"), "users")
            .with_column(DebeziumField::int32("id"))
            .with_column(DebeziumField::string("name").optional())
            .build();

        assert_eq!(
            schema.name.as_deref(),
            Some("opencdc.testdb.public.users.Envelope")
        );
        let fields = schema.fields.as_ref().unwrap();
        assert_eq!(fields.len(), 6);
        assert_eq!(fields[0].field_name, "before");
        assert_eq!(fields[3].field_name, "op");

        let before_fields = fields[0].fields.as_ref().unwrap();
        assert_eq!(before_fields.len(), 2);
        assert_eq!(before_fields[0].field_name, "id");
    }
}
