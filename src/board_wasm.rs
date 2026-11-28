#[path = "wasm/board.rs"]
mod board;
#[expect(dead_code, reason = "the board module reaches the host through part of the bridge")]
#[path = "wasm/bridge.rs"]
mod bridge;
#[expect(dead_code, reason = "the board module stores its log through WorkerStore only")]
#[path = "wasm/store.rs"]
mod store;

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Command {
    Board(board::Request),
    #[expect(dead_code, reason = "boards read only the value limit")]
    Configure {
        #[serde(default)]
        blob_threshold: Option<usize>,
        #[serde(default)]
        restarts: Option<u64>,
        #[serde(default)]
        retain_finished: Option<usize>,
        #[serde(default)]
        value_limit: Option<usize>,
    },
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum Reply {
    Error(String),
    Ok(Value),
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn command(id: u32, pointer: *mut u8, length: usize) {
    let bytes = unsafe { bridge::take_bytes(pointer, length) };
    bridge::install();
    bridge::spawn(async move {
        let result = match serde_json::from_slice::<Command>(&bytes) {
            Ok(Command::Board(request)) => board::execute(request).await,
            Ok(Command::Configure { value_limit, .. }) => {
                store::configure_value_limit(value_limit);
                Ok(Value::Null)
            }
            Err(error) => Err(error.to_string()),
        };
        bridge::respond(
            id,
            &match result {
                Ok(value) => Reply::Ok(value),
                Err(error) => Reply::Error(error),
            },
        );
    });
}
