//! Self-registered DDNS / authorization drivers.
//! 自注册的 DDNS / 授权驱动。

mod cloudflare;
mod common;
mod godaddy;
mod spaceship;
mod westcn;
mod namecheap;
mod aliyun;
mod tencentcloud;
mod huaweicloud;
mod baidu;
mod volcengine;
mod aws;
mod azure;

#[cfg(test)]
mod tests {
    use crate::ddns::all_drivers;

    #[test]
    fn all_expected_drivers_are_registered() {
        let ids: Vec<&str> = all_drivers().iter().map(|driver| driver.id()).collect();
        for expected in [
            "cloudflare", "aliyun", "huaweicloud", "tencentcloud", "baidu",
            "volcengine", "godaddy", "spaceship", "namecheap", "westcn", "aws", "azure",
        ] {
            assert!(ids.contains(&expected), "missing driver: {expected}");
        }
    }
}
