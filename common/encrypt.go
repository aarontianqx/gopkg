package common

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"errors"
	"fmt"
)

type Encryptor interface {
	Encrypt(data []byte) ([]byte, error)
	Decrypt(data []byte) ([]byte, error)
	EncryptToBase64(data []byte) (string, error)
	DecryptFromBase64(str string) ([]byte, error)
}

func NewAesEncryptor(key string) Encryptor {
	l := len(key)
	if l != 16 && l != 24 && l != 32 {
		panic(fmt.Errorf("invalid key length: %d", l))
	}
	return &aesEncryptor{key: []byte(key)}
}

type aesEncryptor struct {
	key []byte
}

func (e *aesEncryptor) Encrypt(data []byte) ([]byte, error) {
	block, err := aes.NewCipher(e.key)
	if err != nil {
		return nil, err
	}
	blockSize := block.BlockSize()
	encryptBytes := pkcs7Padding(data, blockSize)
	encrypted := make([]byte, len(encryptBytes))
	blockMode := cipher.NewCBCEncrypter(block, e.key[:blockSize])
	blockMode.CryptBlocks(encrypted, encryptBytes)
	return encrypted, nil
}

func (e *aesEncryptor) Decrypt(data []byte) ([]byte, error) {
	block, err := aes.NewCipher(e.key)
	if err != nil {
		return nil, err
	}
	blockSize := block.BlockSize()
	blockMode := cipher.NewCBCDecrypter(block, e.key[:blockSize])
	decrypted := make([]byte, len(data))
	blockMode.CryptBlocks(decrypted, data)
	decrypted, err = pkcs7UnPadding(decrypted)
	if err != nil {
		return nil, err
	}
	return decrypted, err
}

func (e *aesEncryptor) EncryptToBase64(data []byte) (string, error) {
	bs, err := e.Encrypt(data)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(bs), nil
}

func (e *aesEncryptor) DecryptFromBase64(str string) ([]byte, error) {
	bs, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		return nil, fmt.Errorf("invalid string: %w", err)
	}
	return e.Decrypt(bs)
}

func pkcs7Padding(data []byte, blockSize int) []byte {
	padding := blockSize - len(data)%blockSize
	padText := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(data, padText...)
}

func pkcs7UnPadding(data []byte) ([]byte, error) {
	l := len(data)
	if l == 0 {
		return nil, errors.New("empty data")
	}
	unPadding := int(data[l-1])
	return data[:(l - unPadding)], nil
}
