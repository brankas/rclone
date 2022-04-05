package hashmap

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"path"
	"strings"
)

func hashNone(a string) string {
	return a
}

func hashMD5(s string) string {
	hash := md5.Sum([]byte(s))
	return hex.EncodeToString(hash[:])
}

func hashSHA1(s string) string {
	hash := sha1.Sum([]byte(s))
	return hex.EncodeToString(hash[:])
}

func hashSHA256(s string) string {
	hash := sha256.Sum256([]byte(s))
	return hex.EncodeToString(hash[:])
}

// toHash converts the provided remote to directory hash and file hash.
// It treats remote as relative to the root of the hashmap.
func (f *Fs) toHash(remote string) (*dirEntry, string, bool) {
	parent, base := path.Split(remote)
	parent = strings.TrimSuffix(parent, "/")
	parent = path.Join(f.root, parent)
	fileHash := f.hasher(base)
	entry, ok := f.dirMap.Path[parent]
	if !ok {
		return nil, fileHash, false
	}
	return entry, fileHash, true
}
