use serde::{Serialize, Deserialize};
use crate::{element::Element, type_part::TypePart};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DatabaseTable {
    pub name: String,
    tp1: Vec<TypePart>,
    tp2: Vec<TypePart>,
    names: (String,String,String),
    property: String,
}

impl DatabaseTable {
    pub fn new(s: Element, p: Element, o: Element) -> Self {
        let prop_label = p.get_table_name();
        let subject_label = s.get_table_name();
        let object_label = o.get_table_name();
        let name = format!("data__{prop_label}__{subject_label}__{object_label}");
        if name.len()>64 { // Paranoia
            panic!("DatabaseTable::new: Table name `{name}` has more than 64 characters");
        }
        Self {
            name,
            tp1: s.get_type_parts(),
            tp2: o.get_type_parts(),
            names: (s.name().to_string(),p.name().to_string(),o.name().to_string()),
            property: prop_label,
        }
    }

    pub fn property(&self) -> &str {
        &self.property
    }

    pub fn names(&self) -> &(std::string::String, std::string::String, std::string::String) {
        &self.names
    }

    pub fn create_statement(&self) -> String {
        let mut parts = vec![];
        parts.push(format!("CREATE TABLE IF NOT EXISTS `{}` (",&self.name));
        parts.push(format!("`id` INT(11) NOT NULL AUTO_INCREMENT,"));
        let mut index_k = vec![];
        let mut index_v = vec![];
        for (num,tp) in self.tp1.iter().enumerate() {
            if let Some(sql) = tp.create_sql() {
                parts.push(format!("`k{num}` {sql},"));
                index_k.push(format!("`k{num}`"));
            }
        }
        for (num,tp) in self.tp2.iter().enumerate() {
            if let Some(sql) = tp.create_sql() {
                parts.push(format!("`v{num}` {sql},"));
                index_v.push(format!("`v{num}`"));
            }
        }
        // Create separate key and value indices
        if true {
            if !index_k.is_empty() {
                parts.push(format!("INDEX `index_k` ({}),",index_k.join(",")));
            }
            if !index_v.is_empty() {
                parts.push(format!("INDEX `index_v` ({}),",index_v.join(",")));
            }
        }

        // Create single unique index
        if true {
            let mut unique_index = index_k;
            unique_index.append(&mut index_v);
            if !unique_index.is_empty() {
                parts.push(format!("UNIQUE INDEX `index_u` ({}),",unique_index.join(",")));
            }
        }

        parts.push(format!("PRIMARY KEY (`id`)"));
        parts.push(format!(") ENGINE=InnoDB"));
        parts.join("\n")
    }
}