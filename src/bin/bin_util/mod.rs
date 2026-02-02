use std::io::Read;

use flate2::read::ZlibDecoder;

pub fn decompress_zlib(data: &[u8]) -> Result<Vec<u8>, std::io::Error> {
    let mut decoder = ZlibDecoder::new(data);
    let mut out = Vec::new();
    decoder.read_to_end(&mut out)?;
    Ok(out)
}
