use aes::cipher::block_padding::Pkcs7;
use aes::cipher::{BlockDecryptMut, KeyIvInit};
use aes::Aes256;
use base64::Engine;

use crate::error::{AppError, AppResult};

type Aes256CbcDec = cbc::Decryptor<Aes256>;

/// 체결통보 페이로드 복호화 (AES256-CBC + PKCS7, base64 입력)
/// key/iv는 웹소켓 구독 응답으로 내려온 문자열을 그대로 바이트로 사용한다.
pub fn aes_cbc_decrypt(key: &str, iv: &str, b64: &str) -> AppResult<String> {
    let data = base64::engine::general_purpose::STANDARD
        .decode(b64.trim())
        .map_err(|e| AppError::Parse(format!("base64 디코드 실패: {e}")))?;
    let dec = Aes256CbcDec::new_from_slices(key.as_bytes(), iv.as_bytes())
        .map_err(|e| AppError::Parse(format!("AES key/iv 길이 오류: {e}")))?;
    let plain = dec
        .decrypt_padded_vec_mut::<Pkcs7>(&data)
        .map_err(|e| AppError::Parse(format!("AES 복호 실패: {e}")))?;
    String::from_utf8(plain).map_err(|e| AppError::Parse(format!("UTF-8 변환 실패: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use aes::cipher::BlockEncryptMut;

    type Aes256CbcEnc = cbc::Encryptor<Aes256>;

    #[test]
    fn roundtrip() {
        let key = "0123456789abcdef0123456789abcdef"; // 32 bytes
        let iv = "abcdef0123456789"; // 16 bytes
        let plain = "12345678|ACNT|0000012345|매수^0193T0^10^12800";

        let enc = Aes256CbcEnc::new_from_slices(key.as_bytes(), iv.as_bytes()).unwrap();
        let cipher = enc.encrypt_padded_vec_mut::<Pkcs7>(plain.as_bytes());
        let b64 = base64::engine::general_purpose::STANDARD.encode(cipher);

        let out = aes_cbc_decrypt(key, iv, &b64).unwrap();
        assert_eq!(out, plain);
    }

    #[test]
    fn bad_key_length_fails() {
        assert!(aes_cbc_decrypt("short", "abcdef0123456789", "aGVsbG8=").is_err());
    }
}
