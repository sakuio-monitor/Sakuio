//! Local maintenance commands used by the `sakuio` service menu.
//! These commands open the configured application database directly and do
//! not require the HTTP server to be running.

use anyhow::{anyhow, bail, Result};
use sakuio_db::AdminAuthRepository;
use std::io::{self, Read};

pub async fn run(args: &[String]) -> Result<()> {
    let Some(command) = args.first().map(String::as_str) else {
        bail!("usage: sakuio-server admin <list|set-username|set-password|disable-totp> [--username NAME]");
    };

    let username = option(args, "--username").unwrap_or_else(|| "admin".into());
    let config = crate::config::ServerConfig::load_or_create()?;
    let db = crate::open_app_database(&config).await?;
    let repo = AdminAuthRepository::new(db.pool());

    if command == "list" {
        let users: Vec<(String,)> =
            sqlx::query_as("SELECT username FROM admin_users ORDER BY username")
                .fetch_all(db.pool())
                .await?;
        for (name,) in users {
            println!("{name}");
        }
        return Ok(());
    }

    let Some((user_id, _, _, _)) = repo.find_by_username(&username).await? else {
        bail!("administrator {username:?} not found");
    };

    match command {
        "set-username" => {
            let new_name = args
                .get(1)
                .filter(|value| !value.starts_with("--"))
                .ok_or_else(|| {
                    anyhow!(
                        "usage: sakuio-server admin set-username NEW_NAME [--username OLD_NAME]"
                    )
                })?;
            let new_name = new_name.trim();
            if new_name.is_empty() {
                bail!("new username must not be empty");
            }
            if repo.username_exists(new_name, &user_id).await? {
                bail!("username {new_name:?} is already in use");
            }
            repo.update_username(&user_id, new_name).await?;
            repo.delete_sessions_for_user(&user_id).await?;
            println!("administrator renamed to {new_name}");
        }
        "set-password" => {
            let password = read_password()?;
            if password.len() < 8 {
                bail!("password must contain at least 8 characters");
            }
            let hash = AdminAuthRepository::hash_password(&password)?;
            repo.update_password_hash(&user_id, &hash).await?;
            repo.delete_sessions_for_user(&user_id).await?;
            println!("password updated for {username}");
        }
        "disable-totp" => {
            repo.disable_totp(&user_id).await?;
            repo.delete_sessions_for_user(&user_id).await?;
            println!("TOTP disabled for {username}");
        }
        other => bail!("unknown admin command: {other}"),
    }

    Ok(())
}

fn option(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|arg| arg == name)
        .and_then(|index| args.get(index + 1))
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn read_password() -> Result<String> {
    if let Ok(value) = std::env::var("SAKUIO_ADMIN_PASSWORD") {
        return Ok(value);
    }
    let mut value = String::new();
    io::stdin().read_to_string(&mut value)?;
    let value = value.trim_end_matches(['\r', '\n']).to_owned();
    if value.is_empty() {
        bail!("password must be provided on stdin or in SAKUIO_ADMIN_PASSWORD");
    }
    Ok(value)
}
