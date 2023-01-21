package uuid

import (
	"bytes"
	"encoding/base64"
	"fmt"

	guuid "github.com/satori/go.uuid"
)

type UIDb64 string

type UIDstring string

type UID []byte
type UIDB []byte

var (
	eod UID = makeUID() // end-of-data
	eob UID = makeUID() // end-of-batch
)

func makeUID() UID {
	u, err := MakeUID()
	if err != nil {
		panic(err)
	}
	return u
}

func (u *UID) EOD() bool {
	if bytes.Equal(*u, eod) {
		return true
	}
	return false
}

func (u *UID) EOB() bool {
	if bytes.Equal(*u, eob) {
		return true
	}
	return false
}

func SendEOD() UID {
	return eod
}

func SendEOB() UID {
	return eob
}

func MakeUID() (UID, error) {
	u := guuid.NewV4()
	uuibin, err := u.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return uuibin, nil
}

// Base64 converts UID binary to base64 string
func (u UID) Base64() UIDb64 {
	return UIDb64(base64.StdEncoding.EncodeToString(u))
}

// EncodeBase64 aka Base64()
func (uid UID) EncodeBase64() UIDb64 {
	return uid.Base64()
}

// DecodeBase64 converts 24 bit base64 encoded string to binary (UID)
func DecodeBase64(ub64 UIDb64) UID {
	dst := make([]byte, base64.StdEncoding.DecodedLen(len(ub64)))
	n, err := base64.StdEncoding.Decode(dst, []byte(ub64))
	if err != nil {
		panic(fmt.Sprintf("UID decode error: %s", err))
	}
	return dst[:n]
}

func (u UIDb64) Decode() UID {
	return DecodeBase64(u)
}

// string - from UID binary to long string ie.format "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
func (u UID) String() string {
	uuid, err := guuid.FromBytes(u)
	if err != nil {
		panic(err)
	}
	return uuid.String()
}

func (u UID) ToUIDString() UIDstring {
	return UIDstring(u.String())
}

func (u UID) ToString() UIDstring {
	return UIDstring(u.String())
}

// FromString converts a UID in long string format to binary
func FromString(u string) UID {

	if len(u) == 24 {
		return DecodeBase64(UIDb64(u))
	}

	uuid, err := guuid.FromString(u)
	if err != nil {
		panic(err)
	}
	uuibin, err := uuid.MarshalBinary()
	if err != nil {
		panic(err)
	}
	return uuibin

}
