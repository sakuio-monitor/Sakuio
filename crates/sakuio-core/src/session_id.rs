use uuid::Uuid;

/// Generates a new board session id.
/// 生成新的板级会话 id。
pub fn new_session_id() -> String {
    Uuid::new_v4().to_string()
}
