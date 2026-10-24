//! AWS Route 53 DNS driver.
//! AWS Route 53 驱动。

use super::super::{DdnsDriver, DdnsResult, DdnsUpdate, DriverField, DriverRegistration};
use super::common;
use async_trait::async_trait;
use hmac::{Hmac, Mac};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::sync::Arc;

const HOST: &str = "route53.amazonaws.com";
const API_VERSION: &str = "2013-04-01";

struct AwsDriver;

fn signature(
    secret: &str,
    date: &str,
    method: &str,
    uri: &str,
    query: &str,
    payload: &str,
) -> String {
    let region = "us-east-1";
    let service = "route53";
    let date_only = &date[..8];
    let payload_hash = hex::encode(Sha256::digest(payload.as_bytes()));
    let canonical_headers =
        format!("host:{HOST}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{date}\n");
    let signed_headers = "host;x-amz-content-sha256;x-amz-date";
    let canonical =
        format!("{method}\n{uri}\n{query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}");
    let scope = format!("{date_only}/{region}/{service}/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{date}\n{scope}\n{}",
        hex::encode(Sha256::digest(canonical.as_bytes()))
    );
    let mut mac = Hmac::<Sha256>::new_from_slice(format!("AWS4{secret}").as_bytes()).expect("hmac");
    mac.update(date_only.as_bytes());
    let key_date = mac.finalize().into_bytes();
    let mut mac = Hmac::<Sha256>::new_from_slice(&key_date).expect("hmac");
    mac.update(region.as_bytes());
    let key_region = mac.finalize().into_bytes();
    let mut mac = Hmac::<Sha256>::new_from_slice(&key_region).expect("hmac");
    mac.update(service.as_bytes());
    let key_service = mac.finalize().into_bytes();
    let mut mac = Hmac::<Sha256>::new_from_slice(&key_service).expect("hmac");
    mac.update(b"aws4_request");
    let key_signing = mac.finalize().into_bytes();
    let mut mac = Hmac::<Sha256>::new_from_slice(&key_signing).expect("hmac");
    mac.update(string_to_sign.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

async fn call(
    config: &Value,
    method: &str,
    uri: &str,
    query: &str,
    payload: String,
) -> Result<reqwest::Response, String> {
    let access_key = common::required(config, "access_key_id", "AccessKey ID")?;
    let secret = common::required(config, "secret_access_key", "SecretAccessKey")?;
    let client = common::client()?;
    let date = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    let date_only = &date[..8];
    let signature = signature(&secret, &date, method, uri, query, &payload);
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={access_key}/{date_only}/us-east-1/route53/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={signature}"
    );
    let payload_hash = hex::encode(Sha256::digest(payload.as_bytes()));
    let url = if query.is_empty() {
        format!("https://{HOST}{uri}")
    } else {
        format!("https://{HOST}{uri}?{query}")
    };
    let request = if method == "GET" {
        client.get(url)
    } else {
        client.post(url)
    };
    request
        .header("Authorization", authorization)
        .header("X-Amz-Date", date)
        .header("X-Amz-Content-Sha256", payload_hash)
        .header("Content-Type", "application/xml")
        .body(payload)
        .send()
        .await
        .map_err(|error| format!("连接 Route 53 失败：{error}"))
}

fn find_zone(xml: &str, zone: &str) -> Option<String> {
    let needle = format!("<Name>{zone}.</Name>");
    xml.split("<HostedZone>").skip(1).find_map(|chunk| {
        if !chunk.contains(&needle) {
            return None;
        }
        let start = chunk.find("<Id>")? + 4;
        let end = chunk[start..].find("</Id>")?;
        Some(
            chunk[start..start + end]
                .trim_start_matches("/hostedzone/")
                .to_string(),
        )
    })
}

#[async_trait]
impl DdnsDriver for AwsDriver {
    fn id(&self) -> &'static str {
        "aws"
    }
    fn name(&self) -> &'static str {
        "AWS Route 53"
    }
    fn description(&self) -> &'static str {
        "AWS AccessKey / SecretKey，自动维护 Route 53 A/AAAA 记录"
    }

    fn fields(&self) -> Vec<DriverField> {
        vec![
            DriverField {
                key: "access_key_id",
                label: "AccessKey ID",
                kind: "text",
                required: true,
                placeholder: "AWS Access Key ID",
                options: None,
            },
            DriverField {
                key: "secret_access_key",
                label: "Secret Access Key",
                kind: "password",
                required: true,
                placeholder: "AWS Secret Access Key",
                options: None,
            },
            DriverField {
                key: "hosted_zone_id",
                label: "Hosted Zone ID",
                kind: "text",
                required: false,
                placeholder: "可选，留空自动查找",
                options: None,
            },
        ]
    }

    async fn validate(&self, config: &Value) -> Result<(), String> {
        common::required(config, "access_key_id", "AccessKey ID")?;
        common::required(config, "secret_access_key", "SecretAccessKey")?;
        Ok(())
    }

    async fn update(&self, config: &Value, request: &DdnsUpdate) -> Result<DdnsResult, String> {
        let zone = request.zone_name.trim().trim_end_matches('.');
        let fqdn = if request.record_name.trim().is_empty() || request.record_name.trim() == "@" {
            format!("{zone}.")
        } else {
            format!("{}.{zone}.", request.record_name.trim())
        };
        let record_type = request.record_type.to_ascii_uppercase();
        let mut hosted = request.zone_id.clone().filter(|value| !value.is_empty());
        if hosted.is_none() {
            let response = call(
                config,
                "GET",
                &format!("/{API_VERSION}/hostedzone"),
                "",
                String::new(),
            )
            .await?;
            let xml = response.text().await.map_err(|error| error.to_string())?;
            hosted = find_zone(&xml, zone);
        }
        let zone_id = hosted.ok_or_else(|| format!("Route 53 中找不到域名 {zone}"))?;
        let payload = format!(
            r#"<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><ChangeBatch><Changes><Change><Action>UPSERT</Action><ResourceRecordSet><Name>{fqdn}</Name><Type>{record_type}</Type><TTL>{ttl}</TTL><ResourceRecords><ResourceRecord><Value>{value}</Value></ResourceRecord></ResourceRecords></ResourceRecordSet></Change></ResourceRecordSet></Change></Changes></ChangeBatch></ChangeResourceRecordSetsRequest>"#,
            ttl = request.ttl.max(60),
            value = request.content
        );
        let response = call(
            config,
            "POST",
            &format!("/{API_VERSION}/hostedzone/{zone_id}/rrset/"),
            "",
            payload,
        )
        .await?;
        if !response.status().is_success() {
            return Err(common::error_message(response).await);
        }
        Ok(DdnsResult {
            zone_id,
            record_id: String::new(),
        })
    }
}

inventory::submit! { DriverRegistration { factory: || Arc::new(AwsDriver) } }

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn parses_zone() {
        let xml = "<ListHostedZonesResponse><HostedZone><Id>/hostedzone/Z123</Id><Name>example.com.</Name></HostedZone></ListHostedZonesResponse>";
        assert_eq!(find_zone(xml, "example.com"), Some("Z123".into()));
    }
    #[tokio::test]
    async fn requires_credentials() {
        assert!(AwsDriver.validate(&json!({})).await.is_err());
    }
    #[test]
    fn driver_metadata() {
        assert_eq!(AwsDriver.id(), "aws");
    }
}
