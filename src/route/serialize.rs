use std::io::Write;

use byteorder::{self, WriteBytesExt};
use rmp::{self, Marker};
use rmp::encode::ValueWriteError;
use rmps::encode::Error;
use serde::{Serialize, Serializer};
use serde::ser::{
    SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant,
    SerializeTuple, SerializeTupleStruct, SerializeTupleVariant
};

#[derive(Debug)]
pub struct V4Serializer<W> {
    wr: W,
}

impl<W> V4Serializer<W> {
    fn new(wr: W) -> Self {
        Self { wr }
    }
}

impl<'a, W> Serializer for &'a mut V4Serializer<W>
where
    W: Write
{
    type Ok = ();
    type Error = Error;

    type SerializeSeq = Compound<'a, W>;
    type SerializeTuple = Compound<'a, W>;
    type SerializeTupleStruct = Compound<'a, W>;
    type SerializeTupleVariant = Compound<'a, W>;
    type SerializeMap = Compound<'a, W>;
    type SerializeStruct = Compound<'a, W>;
    type SerializeStructVariant = Compound<'a, W>;

    #[inline]
    fn serialize_bool(self, _v: bool) -> Result<Self::Ok, Self::Error> {
        unreachable!()
    }

    #[inline]
    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        self.serialize_i64(v as i64)
    }

    #[inline]
    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        self.serialize_i64(v as i64)
    }

    #[inline]
    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        self.serialize_i64(v as i64)
    }

    #[inline]
    fn serialize_i64(self, _v: i64) -> Result<Self::Ok, Self::Error> {
        unreachable!()
    }

    #[inline]
    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        self.serialize_u64(v as u64)
    }

    #[inline]
    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        self.serialize_u64(v as u64)
    }

    #[inline]
    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        self.serialize_u64(v as u64)
    }

    #[inline]
    fn serialize_u64(self, _v: u64) -> Result<Self::Ok, Self::Error> {
        unreachable!()
    }

    #[inline]
    fn serialize_f32(self, _v: f32) -> Result<Self::Ok, Self::Error> {
        unreachable!()
    }

    #[inline]
    fn serialize_f64(self, _v: f64) -> Result<Self::Ok, Self::Error> {
        unreachable!()
    }

    #[inline]
    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        let mut buf = [0; 4];
        self.serialize_str(v.encode_utf8(&mut buf))
    }

    #[inline]
    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        // We encode strings as MessagePack v4 raw, skipping 8-byte-length representation, because
        // there wasn"t such.
        write_str_len(&mut self.wr, v.len() as u32)?;
        self.wr.write_all(v.as_bytes()).map_err(ValueWriteError::InvalidDataWrite)?;
        Ok(())
    }

    #[inline]
    fn serialize_bytes(self, _value: &[u8]) -> Result<Self::Ok, Self::Error> {
        unreachable!()
    }

    #[inline]
    fn serialize_none(self) -> Result<(), Self::Error> {
        unreachable!()
    }

    #[inline]
    fn serialize_some<T: ?Sized + Serialize>(self, _v: &T) -> Result<(), Self::Error> {
        unreachable!()
    }

    #[inline]
    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        unreachable!()
    }

    #[inline]
    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        unreachable!()
    }

    #[inline]
    fn serialize_unit_variant(self, _name: &str, _idx: u32, _variant: &str) -> Result<Self::Ok, Self::Error> {
        unreachable!()
    }

    #[inline]
    fn serialize_newtype_struct<T: ?Sized + Serialize>(self, _name: &'static str, _value: &T) -> Result<(), Self::Error> {
        unreachable!()
    }

    #[inline]
    fn serialize_newtype_variant<T: ?Sized + Serialize>(self, _name: &'static str, _variant_index: u32, _variant: &'static str, _value: &T) -> Result<Self::Ok, Self::Error> {
        unreachable!()
    }

    #[inline]
    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Error> {
        match len {
            Some(len) => {
                rmp::encode::write_array_len(&mut self.wr, len as u32)?;
                Ok(Compound { se: self })
            }
            None => Err(Error::UnknownLength),
        }
    }

    #[inline]
    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        self.serialize_seq(Some(len))
    }

    #[inline]
    fn serialize_tuple_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeTupleStruct, Self::Error> {
        unimplemented!()
//        self.serialize_tuple(len)
    }

    #[inline]
    fn serialize_tuple_variant(self, _name: &'static str, _idx: u32, _variant: &'static str, _len: usize) -> Result<Self::SerializeTupleVariant, Error> {
        unreachable!()
    }

    #[inline]
    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Error> {
        unreachable!()
    }

    #[inline]
    fn serialize_struct(self, _name: &'static str, len: usize) -> Result<Self::SerializeStruct, Self::Error> {
        rmp::encode::write_array_len(&mut self.wr, len as u32)?;
        Ok(Compound { se: self })
    }

    #[inline]
    fn serialize_struct_variant(self, _name: &'static str, _id: u32, _variant: &'static str, _len: usize) -> Result<Self::SerializeStructVariant, Error> {
        unreachable!()
    }
}

pub struct Compound<'a, W: 'a> {
    se: &'a mut V4Serializer<W>,
}

impl<'a, W: Write + 'a> SerializeSeq for Compound<'a, W> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        value.serialize(&mut *self.se)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl<'a, W: Write + 'a> SerializeTuple for Compound<'a, W> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        value.serialize(&mut *self.se)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl<'a, W: Write + 'a> SerializeTupleStruct for Compound<'a, W> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        value.serialize(&mut *self.se)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl<'a, W: Write + 'a> SerializeTupleVariant for Compound<'a, W> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        value.serialize(&mut *self.se)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl<'a, W: Write + 'a> SerializeMap for Compound<'a, W> {
    type Ok = ();
    type Error = Error;

    fn serialize_key<T: ?Sized + Serialize>(&mut self, key: &T) -> Result<(), Self::Error> {
        key.serialize(&mut *self.se)
    }

    fn serialize_value<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        value.serialize(&mut *self.se)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl<'a, W: Write + 'a> SerializeStruct for Compound<'a, W> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: ?Sized + Serialize>(&mut self, _key: &'static str, value: &T) -> Result<(), Self::Error> {
        value.serialize(&mut *self.se)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl<'a, W: Write + 'a> SerializeStructVariant for Compound<'a, W> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: ?Sized + Serialize>(&mut self, _key: &'static str, value: &T) -> Result<(), Self::Error> {
        value.serialize(&mut *self.se)
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

/// Hack for MessagePack v4 compatibility.
#[inline]
fn write_str_len<W: Write>(wr: &mut W, len: u32) -> Result<(), ValueWriteError> {
    if len < 32 {
        wr.write_u8(Marker::FixStr(len as u8).to_u8()).map_err(ValueWriteError::InvalidMarkerWrite)?;
    } else if len < 65536 {
        wr.write_u8(Marker::Str16.to_u8()).map_err(ValueWriteError::InvalidMarkerWrite)?;
        wr.write_u16::<byteorder::BigEndian>(len as u16).map_err(ValueWriteError::InvalidDataWrite)?;
    } else {
        wr.write_u8(Marker::Str32.to_u8()).map_err(ValueWriteError::InvalidMarkerWrite)?;
        wr.write_u32::<byteorder::BigEndian>(len).map_err(ValueWriteError::InvalidDataWrite)?;
    }

    Ok(())
}

#[inline]
pub fn to_vec<T>(val: &T) -> Result<Vec<u8>, Error>
where
    T: Serialize + ?Sized
{
    let mut buf = Vec::with_capacity(128);
    val.serialize(&mut V4Serializer::new(&mut buf))?;
    Ok(buf)
}


#[cfg(test)]
mod test {
    use hyper::{HttpVersion, Method};
    use route::app::RequestMeta;

    use super::*;

    #[test]
    fn serialize_request_meta() {
        let request = RequestMeta {
            method: Method::Post,
            uri: "/announce".into(),
            version: HttpVersion::Http11,
            headers: vec![
                ("Content-Length".into(), "82".into()),
                ("Content-Type".into(), "application/x-www-form-urlencoded".into()),
                ("X-Request-Id".into(), "51b123d0603fc55b".into()),
                ("Accept".into(), "*/*".into()),
                ("User-Agent".into(), "curl/7.54.0".into()),
                ("Host".into(), "rbtorrent__v012.apefront.tst12.ape.yandex.net".into()),
                ("X-Real-Ip".into(), "2a02:6b8:0:3712::1:6d".into()),
                ("X-Cocaine-Event".into(), "http".into()),
                ("X-Cocaine-Service".into(), "rbtorrent__v012".into())
            ],
            body: br#"[{"ns":"zen","mds_key":"1904/test_file_tor","type":"file","path":"test_file_tor"}]"#.to_vec(),
        };

        // '\x95\xa4POST\xa9/announce\xa31.1\x99\x92\xaeContent-Length\xa282\x92\xacContent-Type\xda\x00!application/x-www-form-urlencoded\x92\xacX-Request-Id\xb051b123d0603fc55b\x92\xa6Accept\xa3*/*\x92\xaaUser-Agent\xabcurl/7.54.0\x92\xa4Host\xda\x00-rbtorrent__v012.apefront.tst12.ape.yandex.net\x92\xa9X-Real-Ip\xb52a02:6b8:0:3712::1:6d\x92\xafX-Cocaine-Event\xa4http\x92\xb1X-Cocaine-Service\xafrbtorrent__v012\xda\x00R[{"ns":"zen","mds_key":"1904/test_file_tor","type":"file","path":"test_file_tor"}]'
        assert_eq!(
            vec![
                0x95, 0xa4, 0x50, 0x4f, 0x53, 0x54, 0xa9, 0x2f, 0x61, 0x6e,
                0x6e, 0x6f, 0x75, 0x6e, 0x63, 0x65, 0xa3, 0x31, 0x2e, 0x31,
                0x99, 0x92, 0xae, 0x43, 0x6f, 0x6e, 0x74, 0x65, 0x6e, 0x74,
                0x2d, 0x4c, 0x65, 0x6e, 0x67, 0x74, 0x68, 0xa2, 0x38, 0x32,
                0x92, 0xac, 0x43, 0x6f, 0x6e, 0x74, 0x65, 0x6e, 0x74, 0x2d,
                0x54, 0x79, 0x70, 0x65, 0xda, 0x00, 0x21, 0x61, 0x70, 0x70,
                0x6c, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2f, 0x78,
                0x2d, 0x77, 0x77, 0x77, 0x2d, 0x66, 0x6f, 0x72, 0x6d, 0x2d,
                0x75, 0x72, 0x6c, 0x65, 0x6e, 0x63, 0x6f, 0x64, 0x65, 0x64,
                0x92, 0xac, 0x58, 0x2d, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73,
                0x74, 0x2d, 0x49, 0x64, 0xb0, 0x35, 0x31, 0x62, 0x31, 0x32,
                0x33, 0x64, 0x30, 0x36, 0x30, 0x33, 0x66, 0x63, 0x35, 0x35,
                0x62, 0x92, 0xa6, 0x41, 0x63, 0x63, 0x65, 0x70, 0x74, 0xa3,
                0x2a, 0x2f, 0x2a, 0x92, 0xaa, 0x55, 0x73, 0x65, 0x72, 0x2d,
                0x41, 0x67, 0x65, 0x6e, 0x74, 0xab, 0x63, 0x75, 0x72, 0x6c,
                0x2f, 0x37, 0x2e, 0x35, 0x34, 0x2e, 0x30, 0x92, 0xa4, 0x48,
                0x6f, 0x73, 0x74, 0xda, 0x00, 0x2d, 0x72, 0x62, 0x74, 0x6f,
                0x72, 0x72, 0x65, 0x6e, 0x74, 0x5f, 0x5f, 0x76, 0x30, 0x31,
                0x32, 0x2e, 0x61, 0x70, 0x65, 0x66, 0x72, 0x6f, 0x6e, 0x74,
                0x2e, 0x74, 0x73, 0x74, 0x31, 0x32, 0x2e, 0x61, 0x70, 0x65,
                0x2e, 0x79, 0x61, 0x6e, 0x64, 0x65, 0x78, 0x2e, 0x6e, 0x65,
                0x74, 0x92, 0xa9, 0x58, 0x2d, 0x52, 0x65, 0x61, 0x6c, 0x2d,
                0x49, 0x70, 0xb5, 0x32, 0x61, 0x30, 0x32, 0x3a, 0x36, 0x62,
                0x38, 0x3a, 0x30, 0x3a, 0x33, 0x37, 0x31, 0x32, 0x3a, 0x3a,
                0x31, 0x3a, 0x36, 0x64, 0x92, 0xaf, 0x58, 0x2d, 0x43, 0x6f,
                0x63, 0x61, 0x69, 0x6e, 0x65, 0x2d, 0x45, 0x76, 0x65, 0x6e,
                0x74, 0xa4, 0x68, 0x74, 0x74, 0x70, 0x92, 0xb1, 0x58, 0x2d,
                0x43, 0x6f, 0x63, 0x61, 0x69, 0x6e, 0x65, 0x2d, 0x53, 0x65,
                0x72, 0x76, 0x69, 0x63, 0x65, 0xaf, 0x72, 0x62, 0x74, 0x6f,
                0x72, 0x72, 0x65, 0x6e, 0x74, 0x5f, 0x5f, 0x76, 0x30, 0x31,
                0x32, 0xda, 0x00, 0x52, 0x5b, 0x7b, 0x22, 0x6e, 0x73, 0x22,
                0x3a, 0x22, 0x7a, 0x65, 0x6e, 0x22, 0x2c, 0x22, 0x6d, 0x64,
                0x73, 0x5f, 0x6b, 0x65, 0x79, 0x22, 0x3a, 0x22, 0x31, 0x39,
                0x30, 0x34, 0x2f, 0x74, 0x65, 0x73, 0x74, 0x5f, 0x66, 0x69,
                0x6c, 0x65, 0x5f, 0x74, 0x6f, 0x72, 0x22, 0x2c, 0x22, 0x74,
                0x79, 0x70, 0x65, 0x22, 0x3a, 0x22, 0x66, 0x69, 0x6c, 0x65,
                0x22, 0x2c, 0x22, 0x70, 0x61, 0x74, 0x68, 0x22, 0x3a, 0x22,
                0x74, 0x65, 0x73, 0x74, 0x5f, 0x66, 0x69, 0x6c, 0x65, 0x5f,
                0x74, 0x6f, 0x72, 0x22, 0x7d, 0x5d,
            ],
            to_vec(&request).unwrap()
        );
    }
}
