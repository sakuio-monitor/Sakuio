//! Persistent IP geolocation cache (database-backed, no Redis).
//! 数据库持久化的 IP 归属地缓存（不使用 Redis）。

use crate::{DbPool, DbResult};
use sqlx::Row;

#[derive(Debug, Clone)]
pub struct GeoCacheRow {
    pub ip: String,
    pub country_code: String,
    pub location: String,
    pub expires_at: i64,
}

pub struct GeoCacheRepository<'a> {
    pool: &'a DbPool,
}

impl<'a> GeoCacheRepository<'a> {
    pub fn new(pool: &'a DbPool) -> Self {
        Self { pool }
    }

    pub async fn get(&self, ip: &str) -> DbResult<Option<GeoCacheRow>> {
        let row = sqlx::query(
            "SELECT ip, country_code, location, expires_at FROM ip_geo_cache WHERE ip = ? LIMIT 1",
        )
        .bind(ip)
        .fetch_optional(self.pool)
        .await?;
        Ok(row.map(|row| GeoCacheRow {
            ip: row.try_get("ip").unwrap_or_default(),
            country_code: row.try_get("country_code").unwrap_or_default(),
            location: row.try_get("location").unwrap_or_default(),
            expires_at: row.try_get("expires_at").unwrap_or_default(),
        }))
    }

    pub async fn upsert(
        &self,
        ip: &str,
        country_code: &str,
        location: &str,
        expires_at: i64,
        updated_at: &str,
    ) -> DbResult<()> {
        let updated = sqlx::query(
            "UPDATE ip_geo_cache SET country_code = ?, location = ?, expires_at = ?, updated_at = ? WHERE ip = ?",
        )
        .bind(country_code)
        .bind(location)
        .bind(expires_at)
        .bind(updated_at)
        .bind(ip)
        .execute(self.pool)
        .await?;
        if updated.rows_affected() == 0 {
            sqlx::query(
                "INSERT INTO ip_geo_cache (ip, country_code, location, expires_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            )
            .bind(ip)
            .bind(country_code)
            .bind(location)
            .bind(expires_at)
            .bind(updated_at)
            .execute(self.pool)
            .await?;
        }
        Ok(())
    }

    /// Deletes up to `limit` expired rows whose IP is not referenced by any
    /// current server. Referenced rows are kept and refreshed by the resolver.
    /// 分批删除已过期且无服务器引用的缓存；仍被引用的条目保留并刷新。
    pub async fn delete_expired_unreferenced(
        &self,
        now: i64,
        referenced: &std::collections::HashSet<String>,
        limit: i64,
    ) -> DbResult<u64> {
        let rows = sqlx::query(
            "SELECT ip FROM ip_geo_cache WHERE expires_at <= ? ORDER BY expires_at ASC LIMIT ?",
        )
        .bind(now)
        .bind(limit.clamp(1, 1000))
        .fetch_all(self.pool)
        .await?;
        let mut deleted = 0u64;
        for row in rows {
            let Ok(ip) = row.try_get::<String, _>("ip") else {
                continue;
            };
            if referenced.contains(&ip) {
                continue;
            }
            let result = sqlx::query("DELETE FROM ip_geo_cache WHERE ip = ?")
                .bind(&ip)
                .execute(self.pool)
                .await?;
            deleted += result.rows_affected();
        }
        Ok(deleted)
    }
}
