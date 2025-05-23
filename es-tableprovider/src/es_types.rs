use std::collections::HashMap;
use serde::Deserialize;

pub type MappingResponse = HashMap<String, MappingRoot>;

#[derive(Deserialize)]
pub struct MappingRoot {
    pub mappings: Mapping
}

#[derive(Deserialize)]
pub struct Mapping {
    pub properties: HashMap<String, FieldMapping>,
}

#[derive(Deserialize)]
pub struct FieldMapping {
    #[serde(rename = "type")]
    pub type_: String,
    pub meta: Option<FieldMetadata>
}

impl FieldMapping {
    /// A `"multivalued": "true"` metadata property in the field mapping indicates that this field is multivalued
    pub fn is_multivalued(&self) -> bool {
        let Some(meta) = &self.meta else {
            return false
        };

        let Some(multivalued) = &meta.multivalued else {
            return false;
        };

        multivalued == "true"
    }
}

#[derive(Deserialize)]
pub struct FieldMetadata {
    pub multivalued: Option<String>
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_mapping() {
        let json = r#"
        {"employees":{"mappings":{"properties":{"avg_worked_seconds":{"type":"long"},"birth_date":{"type":"date"},"emp_no":{"type":"integer"},"first_name":{"type":"keyword"},"gender":{"type":"keyword"},"height":{"type":"double","fields":{"float":{"type":"float"},"half_float":{"type":"half_float"},"scaled_float":{"type":"scaled_float","scaling_factor":100.0}}},"hire_date":{"type":"date"},"is_rehired":{"type":"boolean"},"job_positions":{"type":"keyword"},"languages":{"type":"integer","fields":{"byte":{"type":"byte"},"long":{"type":"long"},"short":{"type":"short"}}},"last_name":{"type":"keyword"},"salary":{"type":"integer"},"salary_change":{"type":"double","fields":{"int":{"type":"integer"},"keyword":{"type":"keyword"},"long":{"type":"long"}}},"still_hired":{"type":"boolean"}}}}}
        "#;

        let _mappings: MappingResponse = serde_json::from_str(json).unwrap();

    }
}
