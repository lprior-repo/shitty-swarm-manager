#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Serialize, Deserialize)]
pub struct ProtocolEnvelope {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rid: Option<String>,
    pub t: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub d: Option<Box<Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub err: Option<Box<ProtocolError>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fix: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<Box<Value>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProtocolError {
    pub code: String,
    pub msg: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ctx: Option<Box<Value>>,
}

impl ProtocolEnvelope {
    pub fn success(rid: Option<String>, data: Value) -> Self {
        Self {
            ok: true,
            rid,
            t: Utc::now().timestamp_millis(),
            ms: None,
            d: Some(Box::new(data)),
            err: None,
            fix: None,
            next: None,
            state: None,
        }
    }

    pub fn error(rid: Option<String>, code: String, msg: String) -> Self {
        Self {
            ok: false,
            rid,
            t: Utc::now().timestamp_millis(),
            ms: None,
            d: None,
            err: Some(Box::new(ProtocolError {
                code,
                msg,
                ctx: None,
            })),
            fix: None,
            next: None,
            state: None,
        }
    }

    pub fn with_ms(mut self, ms: i64) -> Self {
        self.ms = Some(ms);
        self
    }

    pub fn with_next(mut self, next: String) -> Self {
        self.next = Some(next);
        self
    }

    pub fn with_state(mut self, state: Value) -> Self {
        self.state = Some(Box::new(state));
        self
    }

    pub fn with_fix(mut self, fix: String) -> Self {
        self.fix = Some(fix);
        self
    }

    pub fn with_ctx(mut self, ctx: Value) -> Self {
        if let Some(ref mut err) = self.err {
            err.ctx = Some(Box::new(ctx));
        }
        self
    }
}
