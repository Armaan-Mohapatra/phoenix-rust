//! Portable, bounded append-only frames for finalized research exports.
//! CRC32 detects accidental corruption only; it is NOT cryptographic
//! provenance or a substitute for SHA-256 manifests / source attestation.
use std::io::{Read,Write};
pub const MAX_FRAME_BYTES:usize=1_048_576;
const MAGIC:&[u8;4]=b"PHX1";

#[derive(Debug)]
pub enum Error { Io(std::io::Error), Truncated, TooLarge, Corrupt,
    InvalidUtf8, InvalidSourceTime, InvalidIdentity, InvalidKind }
impl From<std::io::Error> for Error { fn from(e:std::io::Error)->Self{Self::Io(e)} }
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct Frame {
    pub feed:String,pub session:String,pub market:String,
    pub kind:u8,pub sequence:u64,pub exchange_ns:i64,pub received_ns:i64,
    pub payload:Vec<u8>,
}
impl Frame {
    pub fn validate(&self)->Result<(),Error>{
        if self.feed.is_empty()||self.session.is_empty()||self.market.is_empty(){
            return Err(Error::InvalidIdentity);
        }
        if self.exchange_ns>self.received_ns{return Err(Error::InvalidSourceTime);}
        if self.kind==0 {return Err(Error::InvalidKind);}
        if self.payload.len()>MAX_FRAME_BYTES/2 {return Err(Error::TooLarge);}
        Ok(())
    }
}
fn crc32(bytes:&[u8])->u32 {
    let mut crc:u32=!0;
    for &b in bytes {
        crc^=u32::from(b);
        for _ in 0..8 {
            let mask=0u32.wrapping_sub(crc&1);
            crc=(crc>>1)^(0xEDB8_8320&mask);
        }
    }
    !crc
}
fn add_str(buf:&mut Vec<u8>,s:&str)->Result<(),Error>{
    let n=u16::try_from(s.len()).map_err(|_|Error::TooLarge)?;
    buf.extend_from_slice(&n.to_le_bytes());
    buf.extend_from_slice(s.as_bytes());Ok(())
}
pub fn encode<W:Write>(mut sink:W,f:&Frame)->Result<(),Error>{
    f.validate()?;
    let mut body=Vec::with_capacity(f.payload.len()+128);
    body.extend_from_slice(MAGIC);
    body.push(f.kind);
    body.extend_from_slice(&f.sequence.to_le_bytes());
    body.extend_from_slice(&f.exchange_ns.to_le_bytes());
    body.extend_from_slice(&f.received_ns.to_le_bytes());
    add_str(&mut body,&f.feed)?;
    add_str(&mut body,&f.session)?;
    add_str(&mut body,&f.market)?;
    body.extend_from_slice(&(f.payload.len() as u32).to_le_bytes());
    body.extend_from_slice(&f.payload);
    if body.len()>MAX_FRAME_BYTES{return Err(Error::TooLarge);}
    let len=u32::try_from(body.len()).map_err(|_|Error::TooLarge)?;
    sink.write_all(&len.to_le_bytes())?;
    sink.write_all(&crc32(&body).to_le_bytes())?;
    sink.write_all(&body)?;
    Ok(())
}
struct Cursor<'a>{data:&'a[u8],pos:usize}
impl<'a> Cursor<'a>{
    fn take(&mut self,n:usize)->Result<&'a[u8],Error>{
        let end=self.pos.checked_add(n).ok_or(Error::Truncated)?;
        let slice=self.data.get(self.pos..end).ok_or(Error::Truncated)?;
        self.pos=end;Ok(slice)
    }
    fn u16(&mut self)->Result<u16,Error>{
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self)->Result<u32,Error>{
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self)->Result<u64,Error>{
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn i64(&mut self)->Result<i64,Error>{
        Ok(i64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn string(&mut self)->Result<String,Error>{
        let n=usize::from(self.u16()?);
        Ok(std::str::from_utf8(self.take(n)?)
            .map_err(|_|Error::InvalidUtf8)?.to_owned())
    }
}
/// Returns None only for an entirely missing next header, never truncated data.
pub fn decode<R:Read>(mut reader:R)->Result<Option<Frame>,Error>{
    let mut header=[0u8;8];
    let mut got=0;
    while got<header.len(){
        let n=reader.read(&mut header[got..])?;
        if n==0 {if got==0{return Ok(None);}return Err(Error::Truncated);}
        got+=n;
    }
    let len=u32::from_le_bytes(header[0..4].try_into().unwrap()) as usize;
    let expected_crc=u32::from_le_bytes(header[4..8].try_into().unwrap());
    if !(35..=MAX_FRAME_BYTES).contains(&len) {return Err(Error::TooLarge);}
    let mut body=vec![0u8;len];
    if let Err(e)=reader.read_exact(&mut body) {
        return if e.kind()==std::io::ErrorKind::UnexpectedEof{
            Err(Error::Truncated)}else{Err(Error::Io(e))};
    }
    if crc32(&body)!=expected_crc {return Err(Error::Corrupt);}
    let mut cur=Cursor{data:&body,pos:0};
    if cur.take(4)?!=MAGIC {return Err(Error::Corrupt);}
    let kind=cur.take(1)?[0];
    let sequence=cur.u64()?;
    let exchange_ns=cur.i64()?;
    let received_ns=cur.i64()?;
    let feed=cur.string()?;
    let session=cur.string()?;
    let market=cur.string()?;
    let n=cur.u32()? as usize;
    let payload=cur.take(n)?.to_vec();
    if cur.pos!=body.len(){return Err(Error::Corrupt);}
    let frame=Frame{feed,session,market,kind,sequence,exchange_ns,received_ns,payload};
    frame.validate()?;
    Ok(Some(frame))
}
#[cfg(test)]
mod tests{
    use super::*;use std::io::Cursor;
    fn f()->Frame{Frame{feed:"kalshi".into(),session:"s".into(),
        market:"BTC".into(),kind:1,sequence:42,exchange_ns:10,
        received_ns:11,payload:vec![0,1,2,99]}}
    #[test] fn bounded_roundtrip(){
        let mut bytes=Vec::new();encode(&mut bytes,&f()).unwrap();
        assert_eq!(decode(Cursor::new(&bytes)).unwrap(),Some(f()));
        assert!(decode(Cursor::new(&bytes)).unwrap().is_some());
        assert!(decode(Cursor::new(Vec::<u8>::new())).unwrap().is_none());
    }
    #[test] fn corruption_rejected(){
        let mut bytes=Vec::new();encode(&mut bytes,&f()).unwrap();
        let last=bytes.len()-1;bytes[last]^=0xff;
        assert!(matches!(decode(Cursor::new(bytes)),Err(Error::Corrupt)));
    }
    #[test] fn truncation_not_silent_eof(){
        let mut bytes=Vec::new();encode(&mut bytes,&f()).unwrap();
        bytes.pop();
        assert!(matches!(decode(Cursor::new(bytes)),Err(Error::Truncated)));
    }
    #[test] fn rejects_future_source_and_unbounded_frames(){
        let mut invalid=f();invalid.exchange_ns=12;
        assert!(matches!(encode(Vec::new(),&invalid),Err(Error::InvalidSourceTime)));
        let mut huge=f();huge.payload=vec![1;MAX_FRAME_BYTES];
        assert!(matches!(encode(Vec::new(),&huge),Err(Error::TooLarge)));
    }
    #[test] fn crc32_known_reference(){
        assert_eq!(crc32(b"123456789"),0xcbf4_3926);
    }
}