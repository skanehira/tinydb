use super::{constant::Constant, expression::Expression, scan::Scan};
use crate::{plan::Plan, record::schema::Schema};
use anyhow::Result;
use std::{cmp, fmt::Display, sync::Arc};

#[derive(Debug, Clone)]
pub struct Term {
    lhs: Expression,
    rhs: Expression,
}

impl Term {
    pub fn new(lhs: Expression, rhs: Expression) -> Self {
        Self { lhs, rhs }
    }

    pub fn is_satisfied(&self, scan: &mut impl Scan) -> Result<bool> {
        let lhs_value = self.lhs.evaluate(scan)?;
        let rhs_value = self.rhs.evaluate(scan)?;
        Ok(lhs_value == rhs_value)
    }

    pub fn reduction_factor(&self, plan: &mut impl Plan) -> i32 {
        match (&self.lhs, &self.rhs) {
            (Expression::FieldName(l), Expression::FieldName(r)) => {
                let l_values = plan.distinct_values(l);
                let r_values = plan.distinct_values(r);
                cmp::min(l_values, r_values)
            }
            (Expression::FieldName(l), _) => plan.distinct_values(l),
            (_, Expression::FieldName(r)) => plan.distinct_values(r),
            (Expression::Value(l), Expression::Value(r)) => {
                if l == r {
                    1
                } else {
                    i32::MAX
                }
            }
        }
    }

    pub fn equates_with_constant(&self, field_name: &str) -> Option<Constant> {
        match (&self.lhs, &self.rhs) {
            (Expression::FieldName(l), Expression::Value(v)) => {
                if *l == field_name {
                    Some(v.clone())
                } else {
                    None
                }
            }
            (Expression::Value(v), Expression::FieldName(r)) => {
                if *r == field_name {
                    Some(v.clone())
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    pub fn equates_with_field(&self, field_name: &str) -> Option<String> {
        match (&self.lhs, &self.rhs) {
            (Expression::FieldName(l), Expression::FieldName(r)) => {
                if *l == field_name {
                    Some(r.clone())
                } else if *r == field_name {
                    Some(l.clone())
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    pub fn applies_to(&self, schema: Arc<Schema>) -> bool {
        self.lhs.applies_to(schema.clone()) && self.rhs.applies_to(schema)
    }
}

impl Display for Term {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} = {}", self.lhs, self.rhs)
    }
}
