//! Registrable-domain detection for DDNS input.
//! DDNS 最终域名到可管理根域名的拆分。

/// Common multi-label public suffixes. This intentionally covers the
/// registrar suffixes used by the product without shipping the whole PSL.
const MULTI_SUFFIXES: &[&str] = &[
    "com.cn", "net.cn", "org.cn", "gov.cn", "edu.cn", "ac.cn", "com.hk", "net.hk", "org.hk",
    "edu.hk", "gov.hk", "com.tw", "net.tw", "org.tw", "edu.tw", "gov.tw", "com.mo", "net.mo",
    "org.mo", "edu.mo", "co.uk", "org.uk", "me.uk", "ac.uk", "gov.uk", "ltd.uk", "plc.uk", "co.jp",
    "ne.jp", "or.jp", "ac.jp", "go.jp", "ed.jp", "co.kr", "ne.kr", "or.kr", "re.kr", "go.kr",
    "ac.kr", "com.au", "net.au", "org.au", "edu.au", "gov.au", "id.au", "co.nz", "net.nz",
    "org.nz", "ac.nz", "govt.nz", "com.br", "net.br", "org.br", "edu.br", "gov.br", "com.mx",
    "net.mx", "org.mx", "edu.mx", "gob.mx", "co.in", "net.in", "org.in", "edu.in", "gov.in",
    "ac.in", "com.sg", "net.sg", "org.sg", "edu.sg", "gov.sg", "com.my", "net.my", "org.my",
    "edu.my", "gov.my", "com.ph", "net.ph", "org.ph", "edu.ph", "gov.ph", "com.vn", "net.vn",
    "org.vn", "edu.vn", "gov.vn", "co.id", "or.id", "ac.id", "go.id", "web.id", "co.th", "in.th",
    "or.th", "ac.th", "go.th", "com.tr", "net.tr", "org.tr", "edu.tr", "gov.tr", "com.ru",
    "net.ru", "org.ru", "edu.ru", "gov.ru", "com.ua", "net.ua", "org.ua", "edu.ua", "gov.ua",
    "com.pl", "net.pl", "org.pl", "edu.pl", "gov.pl", "co.za", "net.za", "org.za", "edu.za",
    "gov.za", "com.ar", "net.ar", "org.ar", "edu.ar", "gob.ar", "com.co", "net.co", "org.co",
    "edu.co", "gov.co", "com.pe", "net.pe", "org.pe", "edu.pe", "gob.pe", "com.ec", "com.uy",
    "com.py", "com.bo", "com.do", "com.gt", "com.sv", "com.hn", "com.ni", "com.pa", "co.cr",
    "com.cr", "com.ve", "com.eg", "com.sa", "com.ae", "com.qa", "com.kw", "com.bh", "com.om",
    "com.jo", "com.lb", "co.il", "org.il", "ac.il", "gov.il", "com.ng", "com.gh", "com.ke",
    "co.ke", "or.ke", "com.tz", "co.tz", "com.ug", "co.ug", "com.zm", "co.zm", "com.zw", "co.zw",
    "com.na", "com.bw", "co.bw", "com.mz", "co.mz", "com.ao", "co.ao", "com.mu", "co.mu", "com.sc",
    "com.fj", "com.pg", "com.sb", "com.ws", "com.to", "co.ck", "com.nf", "com.nu", "com.tk",
    "com.vu", "com.nc", "com.pf", "com.gf", "com.gp", "com.mq", "com.re", "com.yt", "com.pm",
    "com.wf", "com.tf", "co.cm", "com.cm", "co.ci", "com.ci", "co.sn", "com.sn", "co.ml", "com.ml",
];

/// Splits a final hostname into `(zone_name, record_name)`.
/// `x.xx.xxx.com` -> `("xxx.com", "x.xx")`; `a.b.com.cn` -> `("b.com.cn", "a")`.
pub fn split_fqdn(input: &str) -> Result<(String, String), String> {
    let raw = input.trim().trim_end_matches('.').to_ascii_lowercase();
    if raw.is_empty() {
        return Err("域名不能为空".into());
    }
    if raw.contains("://") || raw.contains('/') || raw.contains(' ') {
        return Err("请输入最终解析域名，不要包含协议、路径或空格".into());
    }
    let labels: Vec<&str> = raw.split('.').filter(|label| !label.is_empty()).collect();
    if labels.len() < 2 {
        return Err("请输入完整域名，例如 x.example.com".into());
    }
    if labels.iter().any(|label| label.len() > 63) {
        return Err("域名标签过长".into());
    }
    let suffix_len = if labels.len() >= 3
        && MULTI_SUFFIXES.contains(
            &format!("{}.{}", labels[labels.len() - 2], labels[labels.len() - 1]).as_str(),
        ) {
        3
    } else {
        2
    };
    if labels.len() < suffix_len {
        return Err("域名格式不正确".into());
    }
    let zone = labels[labels.len() - suffix_len..].join(".");
    let record = labels[..labels.len() - suffix_len].join(".");
    Ok((
        zone,
        if record.is_empty() {
            "@".into()
        } else {
            record
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::split_fqdn;

    fn split(value: &str) -> (String, String) {
        split_fqdn(value).expect("valid domain")
    }

    #[test]
    fn splits_regular_domain() {
        assert_eq!(split("x.xx.xxx.com"), ("xxx.com".into(), "x.xx".into()));
        assert_eq!(split("example.com"), ("example.com".into(), "@".into()));
    }

    #[test]
    fn keeps_registrable_second_level_domains() {
        assert_eq!(split("en.org"), ("en.org".into(), "@".into()));
        assert_eq!(split("edu.xx"), ("edu.xx".into(), "@".into()));
    }

    #[test]
    fn handles_compound_suffixes() {
        assert_eq!(split("x.xx.com.cn"), ("xx.com.cn".into(), "x".into()));
        assert_eq!(split("a.b.co.uk"), ("b.co.uk".into(), "a".into()));
    }

    #[test]
    fn rejects_invalid_inputs() {
        assert!(split_fqdn("").is_err());
        assert!(split_fqdn("localhost").is_err());
        assert!(split_fqdn("https://example.com/x").is_err());
    }
}
