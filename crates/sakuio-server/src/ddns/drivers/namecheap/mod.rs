//! Namecheap DNS driver.
//! Namecheap DNS 驱动。

use super::super::{DdnsDriver, DdnsResult, DdnsUpdate, DriverField, DriverRegistration};
use super::common;
use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;

const API: &str = "https://api.namecheap.com/xml.response";

struct NamecheapDriver;

#[derive(Clone)]
struct Host {
    name: String,
    record_type: String,
    address: String,
    ttl: String,
    mx: String,
}

fn attr(tag: &str, name: &str) -> String {
    let needle = format!("{name}=\"");
    tag.find(&needle)
        .and_then(|start| {
            let rest = &tag[start + needle.len()..];
            rest.find('"').map(|end| rest[..end].to_string())
        })
        .unwrap_or_default()
}

fn parse_hosts(xml: &str) -> Vec<Host> {
    xml.split("<host ")
        .skip(1)
        .filter_map(|chunk| {
            let tag = chunk.split('>').next()?;
            Some(Host {
                name: attr(tag, "Name"),
                record_type: attr(tag, "Type"),
                address: attr(tag, "Address"),
                ttl: attr(tag, "TTL"),
                mx: attr(tag, "MXPref"),
            })
        })
        .collect()
}

fn status_ok(xml: &str) -> bool {
    xml.contains("Status=\"OK\"") || xml.contains("Status='OK'")
}

async fn client_ip(config: &Value, client: &reqwest::Client) -> Result<String, String> {
    let configured = common::text(config, "client_ip");
    if !configured.is_empty() {
        return Ok(configured);
    }
    let response = client
        .get("https://api.ipify.org")
        .send()
        .await
        .map_err(|error| format!("获取 Namecheap 客户端 IP 失败：{error}"))?;
    let ip = response.text().await.map_err(|error| error.to_string())?;
    let ip = ip.trim().to_string();
    if ip.is_empty() {
        return Err("无法自动获取 Namecheap 客户端 IP，请手动填写".into());
    }
    Ok(ip)
}

fn zone_parts(zone: &str) -> Result<(String, String), String> {
    let zone = zone.trim().trim_end_matches('.');
    let (sld, tld) = zone
        .split_once('.')
        .ok_or_else(|| "Namecheap 域名格式不正确".to_string())?;
    if sld.is_empty() || tld.is_empty() {
        return Err("Namecheap 域名格式不正确".into());
    }
    Ok((sld.to_string(), tld.to_string()))
}

fn auth_params(config: &Value, ip: &str, command: &str) -> Result<Vec<(String, String)>, String> {
    Ok(vec![
        (
            "ApiUser".into(),
            common::required(config, "api_user", "API User")?,
        ),
        (
            "ApiKey".into(),
            common::required(config, "api_key", "API Key")?,
        ),
        (
            "UserName".into(),
            common::required(config, "api_user", "API User")?,
        ),
        ("ClientIp".into(), ip.into()),
        ("Command".into(), command.into()),
    ])
}

#[async_trait]
impl DdnsDriver for NamecheapDriver {
    fn id(&self) -> &'static str {
        "namecheap"
    }
    fn name(&self) -> &'static str {
        "Namecheap"
    }
    fn description(&self) -> &'static str {
        "Namecheap API User / Key，自动维护 A/AAAA 记录"
    }

    fn fields(&self) -> Vec<DriverField> {
        vec![
            DriverField {
                key: "api_user",
                label: "API User",
                kind: "text",
                required: true,
                placeholder: "Namecheap 用户名",
                options: None,
            },
            DriverField {
                key: "api_key",
                label: "API Key",
                kind: "password",
                required: true,
                placeholder: "Namecheap API Key",
                options: None,
            },
            DriverField {
                key: "client_ip",
                label: "客户端 IP",
                kind: "text",
                required: false,
                placeholder: "留空自动获取（需在 Namecheap 白名单）",
                options: None,
            },
        ]
    }

    async fn validate(&self, config: &Value) -> Result<(), String> {
        common::required(config, "api_user", "API User")?;
        common::required(config, "api_key", "API Key")?;
        Ok(())
    }

    async fn update(&self, config: &Value, request: &DdnsUpdate) -> Result<DdnsResult, String> {
        let client = common::client()?;
        let ip = client_ip(config, &client).await?;
        let zone = request.zone_name.trim().trim_end_matches('.');
        let (sld, tld) = zone_parts(zone)?;
        let base = auth_params(config, &ip, "namecheap.domains.dns.getHosts")?;
        let mut params = base.clone();
        params.push(("SLD".into(), sld.clone()));
        params.push(("TLD".into(), tld.clone()));
        let response = client
            .get(API)
            .query(&params)
            .send()
            .await
            .map_err(|error| format!("连接 Namecheap 失败：{error}"))?;
        let xml = response.text().await.map_err(|error| error.to_string())?;
        if !status_ok(&xml) {
            return Err(format!(
                "Namecheap 获取解析失败：{}",
                xml.chars().take(240).collect::<String>()
            ));
        }
        let mut hosts = parse_hosts(&xml);
        let name = if request.record_name.trim().is_empty() {
            "@"
        } else {
            request.record_name.trim()
        };
        let record_type = request.record_type.to_ascii_uppercase();
        hosts.retain(|host| {
            !(host.name.eq_ignore_ascii_case(name)
                && host.record_type.eq_ignore_ascii_case(&record_type))
        });
        hosts.push(Host {
            name: name.into(),
            record_type,
            address: request.content.clone(),
            ttl: request.ttl.max(60).to_string(),
            mx: "10".into(),
        });
        let mut form = auth_params(config, &ip, "namecheap.domains.dns.setHosts")?;
        form.push(("SLD".into(), sld));
        form.push(("TLD".into(), tld));
        for (index, host) in hosts.iter().enumerate() {
            let n = index + 1;
            form.push((format!("HostName{n}"), host.name.clone()));
            form.push((format!("RecordType{n}"), host.record_type.clone()));
            form.push((format!("Address{n}"), host.address.clone()));
            form.push((
                format!("TTL{n}"),
                if host.ttl.is_empty() {
                    "1800".into()
                } else {
                    host.ttl.clone()
                },
            ));
            form.push((
                format!("MXPref{n}"),
                if host.mx.is_empty() {
                    "10".into()
                } else {
                    host.mx.clone()
                },
            ));
        }
        let response = client
            .post(API)
            .form(&form)
            .send()
            .await
            .map_err(|error| format!("Namecheap 更新失败：{error}"))?;
        let xml = response.text().await.map_err(|error| error.to_string())?;
        if !status_ok(&xml) {
            return Err(format!(
                "Namecheap 更新解析失败：{}",
                xml.chars().take(240).collect::<String>()
            ));
        }
        Ok(DdnsResult {
            zone_id: zone.into(),
            record_id: String::new(),
        })
    }
}

inventory::submit! { DriverRegistration { factory: || Arc::new(NamecheapDriver) } }

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn splits_zone() {
        assert_eq!(
            zone_parts("example.com").unwrap(),
            ("example".into(), "com".into())
        );
        assert_eq!(
            zone_parts("example.co.uk").unwrap(),
            ("example".into(), "co.uk".into())
        );
    }

    #[test]
    fn parses_hosts() {
        let xml = r#"<host Name="@" Type="A" Address="1.2.3.4" TTL="1800" MXPref="10" /><host Name="www" Type="AAAA" Address="::1" TTL="60" MXPref="10" />"#;
        let hosts = parse_hosts(xml);
        assert_eq!(hosts.len(), 2);
        assert_eq!(hosts[1].record_type, "AAAA");
    }

    #[tokio::test]
    async fn requires_credentials() {
        assert!(NamecheapDriver.validate(&json!({})).await.is_err());
    }

    #[test]
    fn driver_metadata() {
        assert_eq!(NamecheapDriver.id(), "namecheap");
    }
}
