use opencdc_core::schema::{DebeziumField, DebeziumSchema, DebeziumSchemaType};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub enum SchemaChange {
    FieldAdded(DebeziumField),
    FieldRemoved(DebeziumField),
    FieldTypeChanged {
        field: String,
        old_type: DebeziumSchemaType,
        new_type: DebeziumSchemaType,
    },
    FieldNullabilityChanged {
        field: String,
        was_optional: bool,
        now_optional: bool,
    },
}

#[derive(Debug, Clone)]
pub struct SchemaDiff {
    pub changes: Vec<SchemaChange>,
    pub is_breaking: bool,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub enum EvolutionPolicy {
    #[default]
    AutoAdd,
    Warn,
    Fail,
}

pub struct SchemaEvolution {
    policy: EvolutionPolicy,
}

impl SchemaEvolution {
    pub fn new(policy: EvolutionPolicy) -> Self {
        Self { policy }
    }

    pub fn diff(&self, source: &DebeziumSchema, target: &DebeziumSchema) -> SchemaDiff {
        let mut changes = Vec::new();
        let mut is_breaking = false;

        let source_fields = Self::field_map(source);
        let target_fields = Self::field_map(target);

        for (name, field) in &target_fields {
            if !source_fields.contains_key(name) {
                changes.push(SchemaChange::FieldAdded(field.clone()));
            }
        }

        for (name, field) in &source_fields {
            match target_fields.get(name) {
                None => {
                    changes.push(SchemaChange::FieldRemoved(field.clone()));
                    is_breaking = true;
                }
                Some(target_field) => {
                    let source_type = field.resolve_type();
                    let target_type = target_field.resolve_type();

                    if source_type != target_type {
                        changes.push(SchemaChange::FieldTypeChanged {
                            field: name.clone(),
                            old_type: source_type,
                            new_type: target_type,
                        });
                        is_breaking = true;
                    }

                    if field.optional != target_field.optional {
                        changes.push(SchemaChange::FieldNullabilityChanged {
                            field: name.clone(),
                            was_optional: field.optional.unwrap_or(false),
                            now_optional: target_field.optional.unwrap_or(false),
                        });
                        if field.optional == Some(false) && target_field.optional == Some(true) {
                            is_breaking = true;
                        }
                    }
                }
            }
        }

        SchemaDiff { changes, is_breaking }
    }

    pub fn evolve(&self, current: &DebeziumSchema, source: &DebeziumSchema) -> Result<DebeziumSchema, Vec<SchemaChange>> {
        let diff = self.diff(current, source);

        if diff.is_breaking && self.policy == EvolutionPolicy::Fail {
            return Err(diff.changes);
        }

        if diff.changes.is_empty() {
            return Ok(current.clone());
        }

        match self.policy {
            EvolutionPolicy::AutoAdd | EvolutionPolicy::Warn => {
                let mut evolved = current.clone();
                let current_fields = evolved.fields.get_or_insert_with(Vec::new);

                for change in &diff.changes {
                    if let SchemaChange::FieldAdded(field) = change {
                        current_fields.push(field.clone());
                    }
                }

                if self.policy == EvolutionPolicy::Warn && !diff.changes.is_empty() {
                    tracing::warn!(
                        "schema evolution detected: {:?}",
                        diff.changes
                    );
                }

                Ok(evolved)
            }
            EvolutionPolicy::Fail => Err(diff.changes),
        }
    }

    fn field_map(schema: &DebeziumSchema) -> HashMap<String, DebeziumField> {
        schema
            .fields
            .as_ref()
            .map(|fields| {
                fields
                    .iter()
                    .map(|f| (f.field_name.clone(), f.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }
}

impl Default for SchemaEvolution {
    fn default() -> Self {
        Self::new(EvolutionPolicy::AutoAdd)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_field_evolution() {
        let current = DebeziumSchema {
            schema_type: DebeziumSchemaType::Struct,
            fields: Some(vec![DebeziumField::int32("id")]),
            ..Default::default()
        };

        let source = DebeziumSchema {
            schema_type: DebeziumSchemaType::Struct,
            fields: Some(vec![
                DebeziumField::int32("id"),
                DebeziumField::string("name").optional(),
            ]),
            ..Default::default()
        };

        let evolution = SchemaEvolution::new(EvolutionPolicy::AutoAdd);
        let evolved = evolution.evolve(&current, &source).unwrap();
        let fields = evolved.fields.as_ref().unwrap();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[1].field_name, "name");
    }

    #[test]
    fn test_remove_field_is_breaking() {
        let current = DebeziumSchema {
            schema_type: DebeziumSchemaType::Struct,
            fields: Some(vec![
                DebeziumField::int32("id"),
                DebeziumField::string("name"),
            ]),
            ..Default::default()
        };

        let source = DebeziumSchema {
            schema_type: DebeziumSchemaType::Struct,
            fields: Some(vec![DebeziumField::int32("id")]),
            ..Default::default()
        };

        let evolution = SchemaEvolution::new(EvolutionPolicy::Fail);
        let result = evolution.evolve(&current, &source);
        assert!(result.is_err());
    }

    #[test]
    fn test_no_changes() {
        let schema = DebeziumSchema {
            schema_type: DebeziumSchemaType::Struct,
            fields: Some(vec![DebeziumField::int32("id")]),
            ..Default::default()
        };

        let evolution = SchemaEvolution::default();
        let evolved = evolution.evolve(&schema, &schema).unwrap();
        assert_eq!(
            evolved.fields.as_ref().unwrap().len(),
            1
        );
    }

    #[test]
    fn test_diff_detects_type_change() {
        let current = DebeziumSchema {
            schema_type: DebeziumSchemaType::Struct,
            fields: Some(vec![DebeziumField::int32("id")]),
            ..Default::default()
        };

        let source = DebeziumSchema {
            schema_type: DebeziumSchemaType::Struct,
            fields: Some(vec![DebeziumField::int64("id")]),
            ..Default::default()
        };

        let evolution = SchemaEvolution::default();
        let diff = evolution.diff(&current, &source);
        assert!(diff.is_breaking);
        assert!(matches!(
            diff.changes.first().unwrap(),
            SchemaChange::FieldTypeChanged { .. }
        ));
    }
}
