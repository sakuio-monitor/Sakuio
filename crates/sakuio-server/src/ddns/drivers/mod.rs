//! Self-registered DDNS / authorization drivers.
//! 自注册的 DDNS / 授权驱动。

mod aliyun;
mod aws;
mod azure;
mod baidu;
mod cloudflare;
mod common;
mod godaddy;
mod huaweicloud;
mod namecheap;
mod spaceship;
mod tencentcloud;
mod volcengine;
mod westcn;

#[cfg(test)]
mod tests {
    use crate::ddns::all_drivers;

    #[test]
    fn all_expected_drivers_are_registered() {
        let ids: Vec<&str> = all_drivers().iter().map(|driver| driver.id()).collect();
        for expected in [
            "cloudflare",
            "aliyun",
            "huaweicloud",
            "tencentcloud",
            "baidu",
            "volcengine",
            "godaddy",
            "spaceship",
            "namecheap",
            "westcn",
            "aws",
            "azure",
        ] {
            assert!(ids.contains(&expected), "missing driver: {expected}");
        }
    }
}
