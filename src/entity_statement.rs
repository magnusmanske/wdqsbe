use crate::{element_type::ElementType, db_operation_cache::DbOperationCacheValue, entity::Entity, uuid::UUID32};

#[derive(Clone, Debug)]
pub struct EntityStatement {
    entity: Entity,
    uuid: UUID32,
}

impl From<String> for EntityStatement {
    fn from(s: String) -> Self {
        *EntityStatement::from_str(&s).unwrap()
    }
}

impl ElementType for EntityStatement {
    fn from_str(s: &str) -> Option<Box<Self>> {
        let (first,second) = s.split_once('-')?;
        let ret = EntityStatement {
            entity: *Entity::from_str(first)?,
            uuid: *UUID32::from_str(second)?,
        };
        Some(Box::new(ret))
    }

    fn from_sql_values(name:&str, _value: &Vec<String>) -> Option<Box<Self>> {
        match name {
            "EntityStatement" => todo!(),//EntityStatement::from_str(&value[0].parse::<String>().unwrap()),
            _ => None,
        }
    }

    fn get_type_parts(&self) -> Vec<crate::type_part::TypePart>  {
        let mut ret = self.entity.get_type_parts();
        ret.append(&mut self.uuid.get_type_parts());
        ret
    }

    fn values(&self) -> Vec<DbOperationCacheValue> {
        let mut ret = self.entity.values();
        ret.append(&mut self.uuid.values());
        ret
    }

    fn to_string(&self) -> String  {
        format!("{}-{}",self.entity.to_string(),self.uuid.to_string())
    }

    fn name(&self) -> &str  {
        "EntityStatement"
    }

    fn table_name(&self) -> String  {
        self.name().to_string()
    }

    fn to_url(&self) -> String  {
        self.to_string() // TODO CHECKME FIXME
    }

    fn sql_var_from_name(name: &str, prefix: &str) -> Option<Vec<String>>  {
        if name=="EntityStatement" {
            let mut ret = Entity::sql_var_from_name(name, prefix).unwrap_or(vec![]);
            ret.append(&mut UUID32::sql_var_from_name(name, prefix).unwrap_or(vec![]));
            if ret.is_empty() {
                None
            } else {
                Some(ret)
            }
        } else {
            None
        }
    }
}