package cyutil

import (
	"bytes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/google/uuid"
	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/rs/xid"
	"github.com/sony/sonyflake"
	"github.com/spaolacci/murmur3"
	"github.com/tjfoc/gmsm/sm4"
)

func UUID() string {
	newUUID := uuid.New()
	return newUUID.String()
}

func XID() string {
	return xid.New().String()
}

func NanoID(l ...int) string {
	id, _ := gonanoid.New(l...)
	return id
}

func SnowflakeID() string {
	flake := sonyflake.NewSonyflake(sonyflake.Settings{})
	id, err := flake.NextID()
	if err != nil {
		return ""
	}
	return strconv.FormatUint(id, 10)
}

func MD5(params ...interface{}) string {
	h := md5.New()
	for _, p := range params {
		_, _ = io.WriteString(h, fmt.Sprintln(p))
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func HashInt(input string) uint32 {
	// 创建一个 Murmur3 哈希对象
	hasher := murmur3.New32()
	hasher.Write([]byte(input))
	hashValue := hasher.Sum32()
	return hashValue
}

func Base64Encode(v string) string {
	return base64.StdEncoding.EncodeToString([]byte(v))
}

func Base64Decode(v string) string {
	v1, _ := base64.StdEncoding.DecodeString(v)
	return string(v1)
}
func pkcs5UnPadding2(src []byte) []byte {
	l := bytes.IndexByte(src, 0)
	return src[:l]
}

func SM4Decrypt2(key, iv, cipherText []byte) ([]byte, error) {
	block, err := sm4.NewCipher(key)
	if err != nil {
		return nil, err
	}
	blockMode := cipher.NewCBCDecrypter(block, iv)
	origData := make([]byte, len(cipherText))
	blockMode.CryptBlocks(origData, cipherText)
	origData = pkcs5UnPadding2(origData)
	return origData, nil
}

var (
	PUB_VALUE = "Cyclone890123456"
	IV        = "1234567890123456"
)

func SM4Decode2(v string) string {
	v1, _ := base64.StdEncoding.DecodeString(v)
	b, err := SM4Decrypt2([]byte(PUB_VALUE), []byte(IV), v1)
	if err != nil {
		return ""
	}
	return string(b)
}

func RealVal(encodeVal string) string {
	re, err := regexp.Compile(`^ENC\((.*)\)$`)
	if err != nil {
		return ""
	}
	strs := re.FindStringSubmatch(encodeVal)
	if len(strs) == 2 {
		return SM4Decode2(strs[1])
	}
	return encodeVal
}

// ParseQueryToMap converts a query string to a map of parameters
func ParseQueryToMap(queryString string) (map[string]string, error) {
	result := make(map[string]string)

	// Remove leading ? if present
	queryString = strings.TrimPrefix(queryString, "?")

	// Split by & to get key-value pairs
	pairs := strings.Split(queryString, "&")
	for _, pair := range pairs {
		parts := strings.SplitN(pair, "=", 2)
		if len(parts) == 2 {
			key := parts[0]
			value := parts[1]
			result[key] = value
		} else if len(parts) == 1 && parts[0] != "" {
			// Handle keys with no value
			result[parts[0]] = ""
		}
	}

	return result, nil
}

// SortParam creates a normalized string from parameters following RFC3986 rules
func SortParam(method string, params map[string]string) string {
	// Get all keys and sort them
	var keys []string
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Create sorted parameter string with RFC3986 encoding
	s := []string{}
	for _, k := range keys {
		// Skip signature parameter if present
		if k == "signature" {
			continue
		}
		s = append(s, k+"%3D"+(params[k]))
	}

	// Format as method&/&param1=value1&param2=value2...
	u := method + "&%2F&" + strings.Join(s, "%26")
	return u
}

// GenHMAC generates an HMAC-SHA1 signature and encodes it as Base64
func GenHMAC(data string, key string) string {
	// Append & to the key as per the signature algorithm requirements
	key = key + "&"

	// Create HMAC-SHA1 hash
	mac := hmac.New(sha1.New, []byte(key))
	mac.Write([]byte(data))

	// Return Base64 encoded result
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

// GetSignature generates a signature for API requests
// method: HTTP method (GET, POST, etc.)
// key: Secret key for signing
// queryParam: Map of query parameters
func GetSignature(method, key string, queryParam map[string]string) string {
	// Create normalized parameter string
	sortedParam := SortParam(method, queryParam)

	// Generate and return the HMAC signature
	return GenHMAC(sortedParam, key)
}

// GetSignatureFromURL generates a signature from a URL string
// method: HTTP method (GET, POST, etc.)
// key: Secret key for signing
// url: Full URL including query parameters
func GetSignatureFromURL(method, key, url string) (string, error) {
	// Extract query part from URL
	parts := strings.Split(url, "?")
	if len(parts) != 2 {
		return "", fmt.Errorf("invalid URL format: %s", url)
	}

	// Parse query parameters to map
	queryParams, err := ParseQueryToMap(parts[1])
	if err != nil {
		return "", err
	}

	// Generate signature
	return GetSignature(method, key, queryParams), nil
}

// ParseURLParams parses URL parameters into a map
func ParseURLParams(url string) (string, map[string]string, error) {
	parts := strings.Split(url, "?")
	if len(parts) != 2 {
		return "", nil, fmt.Errorf("invalid URL format: %s", url)
	}

	// Parse query parameters
	queryParams, err := ParseQueryToMap(parts[1])
	if err != nil {
		return "", nil, err
	}

	// Add query parameters to the map
	return parts[0], queryParams, nil
}
