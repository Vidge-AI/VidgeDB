//! VidgeQL parser — Phase 6 (spec §23, renamed TwinQL → VidgeQL).
//!
//! Phase 6 scope (v0): a practical subset of the spec examples that maps
//! directly onto the Phase 2 graph store:
//!
//! ```text
//! MATCH (m:Motor) -[:MECHANICAL]-> (p:Pump)
//! WHERE m.name = "Motor42" AND p.name CONTAINS "17"
//! RETURN m, p
//! ```
//!
//! Supported:
//! - MATCH pattern: node `(var:Type)` (var optional), zero or more hops
//!   `-[:TOPO]->(next:Type)` (topology filter, direction out);
//! - WHERE: `var.prop OP literal` comparisons combined with AND and OR
//!   (Phase 6.3; spec §23), AND binding tighter than OR;
//!   `prop` ∈ {name, type} for now (spec §25 measurements/specs land with
//!   the Phase 4/5 stores);
//! - RETURN: variable list;
//! - LIMIT n.
//!
//! Grammar (recursive descent):
//!   query       := MATCH pattern (WHERE cond_logic)? RETURN vars LIMIT n?
//!   pattern     := node (edge node)*
//!   node        := '(' [ident ':'] ident ')'
//!   edge        := '-[' ':' ident ']' '->'
//!   cond_logic  := cond ((AND | OR) cond)*      # AND binds tighter than OR
//!   cond        := ident '.' ident ('=' | '!=' | '<' | '>' | '<=' | '>=') value
//!   value       := string | number
//!   vars        := ident (',' ident)*
//!
//! The parser produces a `Query` AST; the executor binds patterns against
//! the graph store's adjacency index (spec §20).

/// One path element: a node with optional variable binding and required type.
#[derive(Debug, Clone, PartialEq)]
pub struct NodePat {
    pub var: Option<String>,
    pub type_name: String,
}

/// One hop: `-[:TOPO]->`.
#[derive(Debug, Clone, PartialEq)]
pub struct EdgePat {
    pub topology: String,
}

/// MATCH pattern: chain of nodes and edges.
#[derive(Debug, Clone, PartialEq)]
pub struct Pattern {
    pub nodes: Vec<NodePat>,
    pub edges: Vec<EdgePat>,
}

/// WHERE comparison: `var.prop OP value`.
#[derive(Debug, Clone, PartialEq)]
pub struct Cond {
    pub var: String,
    pub prop: String,
    pub op: CmpOp,
    pub value: Value,
}

/// Boolean filter tree (Phase 6.3, spec §23): WHERE conditions combined
/// with AND and OR, evaluated with AND binding tighter than OR
/// (`a OR b AND c` == `a OR (b AND c)`).
#[derive(Debug, Clone, PartialEq)]
pub enum BoolExpr {
    /// All children must hold (empty And is vacuously true).
    And(Vec<BoolExpr>),
    /// At least one child must hold (empty Or is false, like SQL).
    Or(Vec<BoolExpr>),
    /// A single comparison.
    Leaf(Cond),
}

impl BoolExpr {
    /// Wrap a single condition.
    pub fn leaf(c: Cond) -> BoolExpr {
        BoolExpr::Leaf(c)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Gt,
    Le,
    Ge,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Str(String),
    Num(f64),
}

/// RETURN items and optional LIMIT.
#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    pub pattern: Pattern,
    /// Flat condition list (Phase 6 v0 shape, kept for backward compat with
    /// the temporal layer): the leaves of `where_tree` in source order.
    pub where_: Vec<Cond>,
    /// Boolean tree combining the conditions with AND/OR (Phase 6.3).
    /// A WHERE-less query carries `BoolExpr::And(vec![])` (vacuously true).
    pub where_tree: BoolExpr,
    /// Temporal snapshot (Phase 6.5, spec §24): unix-seconds instant for
    /// `AT <num>`; `None` = query the "now" adjacency (current behavior).
    /// Hops bind through the adjacency rebuilt at that instant.
    pub at: Option<i64>,
    pub ret_vars: Vec<String>,
    pub limit: Option<usize>,
}

#[derive(Debug)]
pub enum ParseError {
    UnexpectedToken(String),
    UnexpectedEof,
    UnknownProp(String),
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::UnexpectedToken(t) => write!(f, "unexpected token: {}", t),
            ParseError::UnknownProp(p) => write!(f, "unknown property: {}", p),
            ParseError::UnexpectedEof => write!(f, "unexpected end of query"),
        }
    }
}

impl std::error::Error for ParseError {}

// ---------------------------------------------------------------------------
// Tokenizer
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Ident(String),
    Str(String),
    Num(f64),
    LParen,
    RParen,
    Dash,
    Colon,
    Comma,
    Gt,
    Lt,
    Ge,    // >=
    Le,    // <=
    Eq,    // = or ==
    Ne,    // !=
    Arrow, // ->
}

fn tokenize(input: &str) -> Result<Vec<Tok>, ParseError> {
    let mut toks = Vec::new();
    let bytes: Vec<char> = input.chars().collect();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        match c {
            ' ' | '\t' | '\n' | '\r' => i += 1,
            '(' => {
                toks.push(Tok::LParen);
                i += 1;
            }
            ')' => {
                toks.push(Tok::RParen);
                i += 1;
            }
            '-' => {
                if i + 1 < bytes.len() && bytes[i + 1] == '[' {
                    // Start of edge; push dash, loop will see '['.
                    toks.push(Tok::Dash);
                    i += 1;
                } else if i + 1 < bytes.len() && bytes[i + 1] == '>' {
                    toks.push(Tok::Arrow);
                    i += 2;
                } else {
                    toks.push(Tok::Dash);
                    i += 1;
                }
            }
            '[' | ']' => i += 1, // brackets are syntax sugar, not needed in AST
            ':' => {
                toks.push(Tok::Colon);
                i += 1;
            }
            ',' => {
                toks.push(Tok::Comma);
                i += 1;
            }
            '>' => {
                if i + 1 < bytes.len() && bytes[i + 1] == '=' {
                    toks.push(Tok::Ge);
                    i += 2;
                } else {
                    toks.push(Tok::Gt);
                    i += 1;
                }
            }
            '<' => {
                if i + 1 < bytes.len() && bytes[i + 1] == '=' {
                    toks.push(Tok::Le);
                    i += 2;
                } else {
                    toks.push(Tok::Lt);
                    i += 1;
                }
            }
            '=' => {
                if i + 1 < bytes.len() && bytes[i + 1] == '=' {
                    toks.push(Tok::Eq);
                    i += 2;
                } else {
                    toks.push(Tok::Eq);
                    i += 1;
                }
            }
            '!' => {
                if i + 1 < bytes.len() && bytes[i + 1] == '=' {
                    toks.push(Tok::Ne);
                    i += 2;
                } else {
                    return Err(ParseError::UnexpectedToken("!".to_string()));
                }
            }
            '"' | '\'' => {
                let quote = c;
                i += 1;
                let mut s = String::new();
                while i < bytes.len() && bytes[i] != quote {
                    s.push(bytes[i]);
                    i += 1;
                }
                if i >= bytes.len() {
                    return Err(ParseError::UnexpectedEof);
                }
                i += 1;
                toks.push(Tok::Str(s));
            }
            c if c.is_ascii_digit() => {
                let start = i;
                while i < bytes.len() && (bytes[i].is_ascii_digit() || bytes[i] == '.') {
                    i += 1;
                }
                let s: String = bytes[start..i].iter().collect();
                s.parse::<f64>()
                    .map(Tok::Num)
                    .map_err(|_| ParseError::UnexpectedToken(s.clone()))?;
                toks.push(Tok::Num(s.parse::<f64>().unwrap()));
            }
            c if c.is_alphabetic() || c == '_' => {
                let start = i;
                while i < bytes.len()
                    && (bytes[i].is_alphanumeric() || bytes[i] == '_' || bytes[i] == '.')
                {
                    i += 1;
                }
                // Dotted identifiers stay one token; the parser splits.
                let s: String = bytes[start..i].iter().collect();
                let kw = s.to_uppercase();
                match kw.as_str() {
                    "MATCH" | "WHERE" | "RETURN" | "LIMIT" | "AND" | "OR" | "AT" => {
                        toks.push(Tok::Ident(kw))
                    }
                    _ => toks.push(Tok::Ident(s)),
                }
            }
            other => return Err(ParseError::UnexpectedToken(other.to_string())),
        }
    }
    Ok(toks)
}

// ---------------------------------------------------------------------------
// Parser (recursive descent)
// ---------------------------------------------------------------------------

pub struct Parser {
    toks: Vec<Tok>,
    pos: usize,
}

impl Parser {
    pub fn parse(input: &str) -> Result<Query, ParseError> {
        let toks = tokenize(input)?;
        let mut p = Parser { toks, pos: 0 };
        p.parse_query()
    }

    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.pos)
    }

    fn expect_kw(&mut self, kw: &str) -> Result<(), ParseError> {
        match self.peek() {
            Some(Tok::Ident(s)) if s == kw => {
                self.pos += 1;
                Ok(())
            }
            _ => Err(ParseError::UnexpectedToken(format!("expected {}", kw))),
        }
    }

    fn expect(&mut self, t: Tok) -> Result<(), ParseError> {
        if self.peek() == Some(&t) {
            self.pos += 1;
            Ok(())
        } else {
            Err(ParseError::UnexpectedToken(format!("expected {:?}", t)))
        }
    }

    fn ident(&mut self) -> Result<String, ParseError> {
        match self.peek() {
            Some(Tok::Ident(s)) => {
                let s = s.clone();
                self.pos += 1;
                Ok(s)
            }
            other => Err(ParseError::UnexpectedToken(format!(
                "expected ident, got {:?}",
                other
            ))),
        }
    }

    fn parse_query(&mut self) -> Result<Query, ParseError> {
        self.expect_kw("MATCH")?;
        let pattern = self.parse_pattern()?;
        let mut where_ = Vec::new();
        let mut where_tree = BoolExpr::And(Vec::new());
        if self.peek() == Some(&Tok::Ident("WHERE".to_string())) {
            self.pos += 1;
            // cond_logic := cond ((AND | OR) cond)* — AND binds tighter than
            // OR (spec §23 boolean filtering): `a OR b AND c` parses as
            // `a OR (b AND c)`. Consecutive ANDs fold into one flat And
            // group; OR splits the chain into Or-ed groups.
            let first = self.parse_cond()?;
            where_.push(first.clone());
            let mut and_terms = vec![BoolExpr::leaf(first)];
            let mut or_groups: Vec<BoolExpr> = Vec::new();
            loop {
                match self.peek() {
                    Some(Tok::Ident(kw)) if kw == "AND" => {
                        self.pos += 1;
                        let c = self.parse_cond()?;
                        where_.push(c.clone());
                        and_terms.push(BoolExpr::leaf(c));
                    }
                    Some(Tok::Ident(kw)) if kw == "OR" => {
                        self.pos += 1;
                        let c = self.parse_cond()?;
                        where_.push(c.clone());
                        or_groups.push(Self::fold_and(and_terms));
                        and_terms = vec![BoolExpr::leaf(c)];
                    }
                    _ => break,
                }
            }
            where_tree = if or_groups.is_empty() {
                Self::fold_and(and_terms)
            } else {
                or_groups.push(Self::fold_and(and_terms));
                BoolExpr::Or(or_groups)
            };
        }
        // AT <unix-seconds>: temporal snapshot clause (Phase 6.5, spec §24),
        // at the same clause level as WHERE — after MATCH/pattern, before
        // RETURN. `AT 1700000000` reconstructs the topology at that instant.
        let mut at = None;
        if self.peek() == Some(&Tok::Ident("AT".to_string())) {
            self.pos += 1;
            match self.peek() {
                Some(Tok::Num(n)) => {
                    // Timestamps are integral unix seconds.
                    at = Some(*n as i64);
                    self.pos += 1;
                }
                _ => {
                    return Err(ParseError::UnexpectedToken(
                        "expected number after AT".into(),
                    ))
                }
            }
        }
        self.expect_kw("RETURN")?;
        let mut ret_vars = vec![self.ident()?];
        while self.peek() == Some(&Tok::Comma) {
            self.pos += 1;
            ret_vars.push(self.ident()?);
        }
        let mut limit = None;
        if self.peek() == Some(&Tok::Ident("LIMIT".to_string())) {
            self.pos += 1;
            match self.peek() {
                Some(Tok::Num(n)) => {
                    limit = Some(*n as usize);
                    self.pos += 1;
                }
                _ => return Err(ParseError::UnexpectedToken("expected number".into())),
            }
        }
        Ok(Query {
            pattern,
            where_,
            where_tree,
            at,
            ret_vars,
            limit,
        })
    }

    /// Fold AND-combinable terms: 1 term -> the leaf itself, n terms ->
    /// a flat `And`. (Flat shape keeps the Phase-6 single-AND queries
    /// readable and lets the executor short-circuit per term.)
    fn fold_and(terms: Vec<BoolExpr>) -> BoolExpr {
        match terms.len() {
            1 => terms.into_iter().next().unwrap(),
            _ => BoolExpr::And(terms),
        }
    }

    fn parse_pattern(&mut self) -> Result<Pattern, ParseError> {
        let mut nodes = vec![self.parse_node()?];
        let mut edges = Vec::new();
        while self.peek() == Some(&Tok::Dash) {
            self.pos += 1;
            self.expect(Tok::Colon)?; // -[:TOPO]->  ('[' and ']' are skipped)
            let topo = self.ident()?;
            self.expect(Tok::Arrow)?;
            edges.push(EdgePat {
                topology: topo.to_lowercase(),
            });
            nodes.push(self.parse_node()?);
        }
        Ok(Pattern { nodes, edges })
    }

    fn parse_node(&mut self) -> Result<NodePat, ParseError> {
        self.expect(Tok::LParen)?;
        // (var:Type) or (:Type)
        let first = if self.peek() == Some(&Tok::Colon) {
            String::new() // anonymous: no variable
        } else {
            self.ident()?
        };
        if self.peek() == Some(&Tok::Colon) {
            self.pos += 1;
            let type_name = self.ident()?;
            self.expect(Tok::RParen)?;
            let var = if first.is_empty() { None } else { Some(first) };
            Ok(NodePat { var, type_name })
        } else {
            self.expect(Tok::RParen)?;
            Ok(NodePat {
                var: None,
                type_name: first,
            })
        }
    }

    fn parse_cond(&mut self) -> Result<Cond, ParseError> {
        // var.prop OP value  (the tokenizer keeps var.prop as one ident)
        let lhs = self.ident()?;
        let (var, prop) = match lhs.split_once('.') {
            Some((v, p)) => (v.to_string(), p.to_string()),
            None => return Err(ParseError::UnknownProp(lhs)),
        };
        let op = match self.peek() {
            Some(Tok::Eq) => CmpOp::Eq,
            Some(Tok::Ne) => CmpOp::Ne,
            Some(Tok::Lt) => CmpOp::Lt,
            Some(Tok::Gt) => CmpOp::Gt,
            Some(Tok::Le) => CmpOp::Le,
            Some(Tok::Ge) => CmpOp::Ge,
            other => {
                return Err(ParseError::UnexpectedToken(format!(
                    "expected operator, got {:?}",
                    other
                )))
            }
        };
        self.pos += 1;
        let value = match self.peek() {
            Some(Tok::Str(s)) => {
                let v = Value::Str(s.clone());
                self.pos += 1;
                v
            }
            Some(Tok::Num(n)) => {
                let v = Value::Num(*n);
                self.pos += 1;
                v
            }
            other => {
                return Err(ParseError::UnexpectedToken(format!(
                    "expected value, got {:?}",
                    other
                )))
            }
        };
        Ok(Cond {
            var,
            prop,
            op,
            value,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_match() {
        let q = Parser::parse("MATCH (m:Motor) RETURN m").unwrap();
        assert_eq!(q.pattern.nodes.len(), 1);
        assert_eq!(q.pattern.nodes[0].var.as_deref(), Some("m"));
        assert_eq!(q.pattern.nodes[0].type_name, "Motor");
        assert_eq!(q.ret_vars, vec!["m"]);
        assert!(q.where_.is_empty());
        assert_eq!(q.where_tree, BoolExpr::And(Vec::new()));
        assert_eq!(q.limit, None);
    }

    #[test]
    fn traversal_with_topology() {
        let q = Parser::parse("MATCH (m:Motor) -[:MECHANICAL]-> (p:Pump) RETURN m, p").unwrap();
        assert_eq!(q.pattern.nodes.len(), 2);
        assert_eq!(q.pattern.edges.len(), 1);
        assert_eq!(q.pattern.edges[0].topology, "mechanical");
    }

    #[test]
    fn where_clause_and_limit() {
        let q = Parser::parse(
            "MATCH (m:Motor) WHERE m.name = \"Motor42\" AND m.vendor != \"X\" RETURN m LIMIT 10",
        )
        .unwrap();
        assert_eq!(q.where_.len(), 2);
        assert_eq!(q.where_[0].var, "m");
        assert_eq!(q.where_[0].prop, "name");
        assert_eq!(q.where_[0].op, CmpOp::Eq);
        assert_eq!(q.where_[0].value, Value::Str("Motor42".into()));
        assert_eq!(q.where_[1].op, CmpOp::Ne);
        // AND chain folds to a flat And of 2 leaves (Phase 6.3).
        assert_eq!(
            q.where_tree,
            BoolExpr::And(vec![
                BoolExpr::Leaf(q.where_[0].clone()),
                BoolExpr::Leaf(q.where_[1].clone()),
            ])
        );
        assert_eq!(q.limit, Some(10));
    }

    /// Phase 6.3: OR at the same level as AND produces an Or node with
    /// two leaves.
    #[test]
    fn where_clause_or() {
        let q = Parser::parse("MATCH (m:Motor) WHERE m.name = \"A\" OR m.name = \"B\" RETURN m")
            .unwrap();
        assert_eq!(q.where_.len(), 2);
        assert_eq!(
            q.where_tree,
            BoolExpr::Or(vec![
                BoolExpr::Leaf(q.where_[0].clone()),
                BoolExpr::Leaf(q.where_[1].clone()),
            ])
        );
    }

    /// Phase 6.3 precedence: AND binds tighter than OR — `a OR b AND c`
    /// parses as `a OR (b AND c)`.
    #[test]
    fn where_clause_precedence_and_over_or() {
        let q = Parser::parse(
            "MATCH (m:Motor) WHERE m.name = \"a\" OR m.poles < 2 AND m.vendor != \"X\" RETURN m",
        )
        .unwrap();
        // a OR (b AND c)
        assert_eq!(
            q.where_tree,
            BoolExpr::Or(vec![
                BoolExpr::Leaf(q.where_[0].clone()),
                BoolExpr::And(vec![
                    BoolExpr::Leaf(q.where_[1].clone()),
                    BoolExpr::Leaf(q.where_[2].clone()),
                ]),
            ])
        );
        // Reverse order: AND group comes first — `a AND b OR c`.
        let q2 = Parser::parse(
            "MATCH (m:Motor) WHERE m.poles < 2 AND m.vendor != \"X\" OR m.name = \"a\" RETURN m",
        )
        .unwrap();
        assert_eq!(
            q2.where_tree,
            BoolExpr::Or(vec![
                BoolExpr::And(vec![
                    BoolExpr::Leaf(q2.where_[0].clone()),
                    BoolExpr::Leaf(q2.where_[1].clone()),
                ]),
                BoolExpr::Leaf(q2.where_[2].clone()),
            ])
        );
    }

    #[test]
    fn anonymous_node() {
        let q = Parser::parse("MATCH (:Motor) RETURN m").unwrap();
        assert_eq!(q.pattern.nodes[0].var, None);
        assert_eq!(q.pattern.nodes[0].type_name, "Motor");
    }

    #[test]
    fn two_hops() {
        let q = Parser::parse(
            "MATCH (plc:PLC) -[:ELECTRICAL]-> (w:Wire) -[:ELECTRICAL]-> (m:Motor) RETURN plc, w, m",
        )
        .unwrap();
        assert_eq!(q.pattern.nodes.len(), 3);
        assert_eq!(q.pattern.edges.len(), 2);
        assert_eq!(q.pattern.edges[1].topology, "ELECTRICAL".to_lowercase());
    }

    #[test]
    fn numeric_comparison() {
        let q = Parser::parse("MATCH (m:Motor) WHERE m.poles >= 4 RETURN m").unwrap();
        assert_eq!(q.where_[0].value, Value::Num(4.0));
        assert_eq!(q.where_[0].op, CmpOp::Ge);
    }

    #[test]
    fn parse_errors() {
        assert!(Parser::parse("MATCH RETURN m").is_err());
        assert!(Parser::parse("MATCH (m:Motor) RETURN").is_err());
        assert!(Parser::parse("MATCH (m:Motor) WHERE m.name RETURN m").is_err());
    }
}
