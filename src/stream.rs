use anyhow::Result;
use ed25519_dalek::{Keypair, PublicKey, Signature, Signer};
use rkyv::ser::serializers::AlignedSerializer;
use rkyv::ser::Serializer;
use rkyv::{AlignedVec, Archive, Deserialize, Serialize};

#[derive(Archive, Deserialize, Serialize, Clone, Copy, Eq, Hash, PartialEq)]
pub struct StreamId {
    pub peer: [u8; 32],
    pub stream: u64,
}

impl std::fmt::Debug for StreamId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        for byte in self.peer.iter() {
            write!(f, "{:02x}", byte)?;
        }
        write!(f, ".{}", self.stream)
    }
}

impl std::fmt::Display for StreamId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl StreamId {
    pub fn to_bytes(&self) -> Result<AlignedVec> {
        let mut ser = AlignedSerializer::new(AlignedVec::new());
        ser.serialize_value(self)?;
        Ok(ser.into_inner())
    }
}

#[derive(Archive, Deserialize, Serialize, Clone, Debug, Eq, PartialEq)]
pub struct Head {
    pub stream: u64,
    pub hash: [u8; 32],
    pub len: u64,
}

impl Head {
    pub(crate) fn new(stream: u64) -> Self {
        Self {
            stream,
            hash: [
                175, 19, 73, 185, 245, 249, 161, 166, 160, 64, 77, 234, 54, 220, 201, 73, 155, 203,
                37, 201, 173, 193, 18, 183, 204, 154, 147, 202, 228, 31, 50, 98,
            ],
            len: 0,
        }
    }

    pub(crate) fn sign(&self, key: &Keypair) -> Result<SignedHead> {
        let mut ser = AlignedSerializer::new(AlignedVec::new());
        ser.serialize_value(self)?;
        let bytes = ser.into_inner();
        let sig = key.sign(&bytes).to_bytes();
        Ok(SignedHead {
            head: bytes.to_vec(),
            sig,
        })
    }
}

#[derive(Archive, Deserialize, Serialize, Clone, Debug, Eq, PartialEq)]
pub struct SignedHead {
    pub head: Vec<u8>,
    pub sig: [u8; 64],
}

impl ArchivedSignedHead {
    pub fn verify(&self, peer: &[u8; 32]) -> Result<()> {
        let public = PublicKey::from_bytes(peer)?;
        let sig = Signature::from(self.sig);
        public.verify_strict(&self.head, &sig)?;
        Ok(())
    }
}

#[derive(Archive, Deserialize, Serialize, Clone, Debug, Eq, PartialEq)]
pub struct Stream {
    pub head: Head,
    pub outboard: Vec<u8>,
}

impl Stream {
    pub(crate) fn new(stream: u64) -> Self {
        Self {
            head: Head::new(stream),
            outboard: vec![0, 0, 0, 0, 0, 0, 0, 0],
        }
    }

    pub(crate) fn to_bytes(&self) -> Result<AlignedVec> {
        let mut ser = AlignedSerializer::new(AlignedVec::new());
        ser.serialize_value(self)?;
        Ok(ser.into_inner())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Slice {
    pub head: SignedHead,
    pub slice: Vec<u8>,
}

impl Slice {}

mod tests {
    use super::*;

    #[test]
    fn test_default_stream() {
        let (outboard, hash) = bao::encode::outboard(&[]);
        let expect = Stream {
            head: Head {
                stream: 42,
                hash: *hash.as_bytes(),
                len: 0,
            },
            outboard,
        };
        let actual = Stream::new(42);
        assert_eq!(actual, expect);
    }
}
