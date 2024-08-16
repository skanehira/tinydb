use super::{constant::Constant, scan::Scan, term::Term};
use crate::{plan::Plan, record::schema::Schema};
use anyhow::Result;
use std::{fmt::Display, sync::Arc};

#[derive(Default, Debug)]
pub struct Predicate {
    terms: Vec<Term>,
}

impl Predicate {
    pub fn new(term: Term) -> Self {
        Self { terms: vec![term] }
    }

    pub fn con_join_with(&mut self, pred: &Self) {
        self.terms.extend(pred.terms.clone());
    }

    pub fn is_satisfied(&mut self, scan: &mut dyn Scan) -> Result<bool> {
        for term in self.terms.iter() {
            if !term.is_satisfied(scan)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub fn reduction_factor(&self, plan: &mut impl Plan) -> i32 {
        self.terms
            .iter()
            .map(|term| term.reduction_factor(plan))
            .sum()
    }

    pub fn select_sub_pred(&self, schema: Arc<Schema>) -> Option<Predicate> {
        let terms: Vec<Term> = self
            .terms
            .iter()
            .filter(|term| term.applies_to(schema.clone()))
            .cloned()
            .collect();

        if terms.is_empty() {
            None
        } else {
            Some(Predicate { terms })
        }
    }

    pub fn join_sub_pred(&self, schema1: Arc<Schema>, schema2: Arc<Schema>) -> Result<Predicate> {
        let mut schema = Schema::default();
        schema.add_all(schema1.clone())?;
        schema.add_all(schema2.clone())?;
        let schema = Arc::new(schema);

        let terms: Vec<Term> = self
            .terms
            .iter()
            .filter(|term| {
                !term.applies_to(schema1.clone())
                    && !term.applies_to(schema2.clone())
                    && term.applies_to(schema.clone())
            })
            .cloned()
            .collect();

        Ok(Self { terms })
    }

    pub fn equates_with_constant(&self, field_name: &str) -> Option<Constant> {
        for term in self.terms.iter() {
            if let Some(value) = term.equates_with_constant(field_name) {
                return Some(value);
            }
        }
        None
    }

    pub fn equates_with_field(&self, field_name: &str) -> Option<String> {
        for term in self.terms.iter() {
            if let Some(name) = term.equates_with_field(field_name) {
                return Some(name);
            }
        }
        None
    }
}

impl Display for Predicate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut terms = self.terms.iter();
        if let Some(term) = terms.next() {
            write!(f, "{}", term)?;
            for term in terms {
                write!(f, " AND {}", term)?;
            }
        }
        Ok(())
    }
}
