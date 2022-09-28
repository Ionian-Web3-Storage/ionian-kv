use serde::{Deserialize, Serialize};
use shared_types::{DataRoot, FileProof, Transaction};

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Status {
    pub connected_peers: usize,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileInfo {
    pub tx: Transaction,
    pub finalized: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Segment(#[serde(with = "base64")] pub Vec<u8>);

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SegmentWithProof {
    /// File merkle root.
    pub root: DataRoot,
    #[serde(with = "base64")]
    /// With fixed data size except the last segment.
    pub data: Vec<u8>,
    /// Segment index.
    pub index: u32,
    /// File merkle proof whose leaf node is segment root.
    pub proof: FileProof,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ValueSegment {
    // key version
    pub version: u64,
    // data
    #[serde(with = "base64")]
    pub data: Vec<u8>,
    // value total size
    pub size: u64,
}

mod base64 {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(v: &Vec<u8>, s: S) -> Result<S::Ok, S::Error> {
        let base64 = base64::encode(v);
        String::serialize(&base64, s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let base64 = String::deserialize(d)?;
        base64::decode(base64.as_bytes()).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::Segment;

    #[test]
    fn test_segment_serde() {
        let seg = Segment("hello, world".as_bytes().to_vec());
        let result = serde_json::to_string(&seg).unwrap();
        assert_eq!(result.as_str(), "\"aGVsbG8sIHdvcmxk\"");

        let seg2: Segment = serde_json::from_str("\"aGVsbG8sIHdvcmxk\"").unwrap();
        assert_eq!(String::from_utf8(seg2.0).unwrap().as_str(), "hello, world");
    }
}
