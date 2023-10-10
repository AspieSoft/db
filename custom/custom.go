package custom

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/AspieSoft/go-regex-re2/v2"
	"github.com/AspieSoft/goutil/crypt"
	"github.com/AspieSoft/goutil/v7"
	"github.com/cespare/go-smaz"
)

const DebugMode = true

const maxDatabaseSize uint64 = 99999999999999 // 14 (64000 bit - max lines = 1 billion)

type Database struct {
	File *os.File
	Path string
	BitSize uint16
	PrefixList []byte
	MU sync.Mutex
	encKey []byte
}

type Object struct {
	Key []byte
	Val []byte
	Line int64

	OldKey []byte
	OldVal []byte
}


// Open opens an existing database or creates a new one
//
// @bitSize tells the database what bit size to use (this value must always be consistant)
// (default: 1024)
func Open(path string, encKey []byte, bitSize uint16, prefixList []byte) (*Database, error) {
	for _, prefix := range prefixList {
		if goutil.Contains([]byte("%=,@-!\n"), prefix) {
			return &Database{}, errors.New("'"+string(prefix)+"' is reserved for the core database structure")
		}
	}

	path, err := filepath.Abs(path)
	if err != nil {
		return &Database{}, err
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0755)
	if err != nil {
		return &Database{}, err
	}

	if bitSize == 0 {
		bitSize = 1024
	}else if DebugMode && bitSize < 16 {
		bitSize = 16
	}else if !DebugMode && bitSize < 64 {
		bitSize = 64
	}else if bitSize > 64000 {
		bitSize = 64000
	}

	return &Database{
		File: file,
		Path: path,
		BitSize: bitSize,
		PrefixList: prefixList,
		encKey: encKey,
	}, nil
}

// Close closes the database file
func (db *Database) Close() error {
	db.MU.Lock()
	defer db.MU.Unlock()

	err1 := db.File.Sync()
	err2 := db.File.Close()

	if err2 == nil {
		return err1
	}
	return err2
}


// AddDataObj adds a new key value pair to the database, given a prefix
//
// note: this method also runs `File.Seek(0, io.SeekStart)`
func AddDataObj(db *Database, prefix byte, key []byte, val []byte) (Object, error) {
	pos, _ := db.File.Seek(0, io.SeekStart)

	if off := pos % int64(db.BitSize); off != 0 {
		db.File.Write(bytes.Repeat([]byte{'-'}, int(off)))
		pos += off
	}

	buf := make([]byte, 1)
	_, err := db.File.Read(buf)
	for err == nil && buf[0] != '!' {
		pos, _ = db.File.Seek(int64(db.BitSize)-1, io.SeekCurrent)
		_, err = db.File.Read(buf)
	}

	addNew := false
	if err == io.EOF {
		addNew = true
		pos, _ = db.File.Seek(0, io.SeekEnd)
	}else{
		pos, _ = db.File.Seek(-1, io.SeekCurrent)
	}

	obj := Object{
		Key: key,
		Val: val,
		Line: pos / int64(db.BitSize),
	}

	val = regex.JoinBytes(key, '=', val)

	val, err = encData(db, val)
	if err != nil {
		return Object{}, err
	}

	posLine := pos / int64(db.BitSize)

	// add data
	db.File.Write([]byte{prefix})

	off := 1
	if DebugMode {
		off++
	}

	for len(val) + off > int(db.BitSize) {
		var posStr []byte
		var useNewPos int64 = -1

		if !addNew {
			curPos, _ := db.File.Seek(0, io.SeekCurrent)
			db.File.Seek(int64(db.BitSize)-1, io.SeekCurrent)

			_, err = db.File.Read(buf)
			for err == nil && buf[0] != '!' {
				db.File.Seek(int64(db.BitSize)-1, io.SeekCurrent)
				_, err = db.File.Read(buf)
			}

			if err == io.EOF {
				addNew = true
				newPos, _ := db.File.Seek(0, io.SeekEnd)
				useNewPos = newPos
				newPos /= int64(db.BitSize)
				posStr = []byte(strconv.FormatInt(newPos, 36))
				posLine = newPos
			}else{
				newPos, _ := db.File.Seek(-1, io.SeekCurrent)
				useNewPos = newPos
				newPos /= int64(db.BitSize)
				posStr = []byte(strconv.FormatInt(newPos, 36))
			}

			db.File.Seek(curPos, io.SeekStart)
		}else if addNew {
			posLine++
			posStr = []byte(strconv.FormatInt(posLine, 36))
		}

		posStr = append([]byte{'@'}, posStr...)
		offset := int(db.BitSize) - len(posStr) - 1

		if DebugMode {
			offset--
		}

		db.File.Write(val[:offset])
		db.File.Write(posStr)
		val = val[offset:]

		if DebugMode {
			db.File.Write([]byte{'\n'})
		}

		if useNewPos != -1 {
			db.File.Seek(useNewPos, io.SeekStart)
		}

		db.File.Write([]byte{'&'})
	}

	db.File.Write(val)
	if len(val) < int(db.BitSize) {
		if DebugMode {
			db.File.Write(bytes.Repeat([]byte{'-'}, int(db.BitSize) - len(val) - 2))
			db.File.Write([]byte{'\n'})
		}else{
			db.File.Write(bytes.Repeat([]byte{'-'}, int(db.BitSize) - len(val) - 1))
		}
	}

	return obj, nil
}

// GetDataObj finds a key value pair in the database, given a prefix
//
// set the first byte of the key/val param to 0 to authorize the use of regex
func GetDataObj(db *Database, prefix byte, key []byte, val []byte, stopAfterFirstRow ...bool) (Object, error) {
	regTypeKey := uint8(0)
	regTypeVal := uint8(0)
	var reKey *regex.Regexp
	var reVal *regex.Regexp
	var err error

	var encErr error

	if len(key) != 0 && key[0] == 0 {
		regTypeKey = 1
		key = key[1:]

		if len(key) == 1 && key[0] == '*' {
			regTypeKey = 2
		}else if len(key) != 0 {
			regTypeKey = 3
			reKey, err = regex.CompTry(string(regex.Comp(`(\\*)([\\\%])`).RepFunc(key, func(data func(int) []byte) []byte {
				if l := len(data(1)); (l == 0 || l % 2 == 0) && data(2)[0] != '\\' {
					return regex.JoinBytes(data(1), '\\', data(2))
				}
				return data(0)
			})))
			if err != nil {
				return Object{}, err
			}
		}
	}

	if len(val) != 0 && val[0] == 0 {
		regTypeVal = 1
		val = val[1:]

		if len(val) == 1 && val[0] == '*' {
			regTypeVal = 2
		}else if len(val) != 0 {
			regTypeVal = 3
			reVal, err = regex.CompTry(string(regex.Comp(`(\\*)([\\\%])`).RepFunc(val, func(data func(int) []byte) []byte {
				if l := len(data(1)); (l == 0 || l % 2 == 0) && data(2)[0] != '\\' {
					return regex.JoinBytes(data(1), '\\', data(2))
				}
				return data(0)
			})))
			if err != nil {
				return Object{}, err
			}
		}
	}

	stopFirstRow := false
	if len(stopAfterFirstRow) != 0 && stopAfterFirstRow[0] == true {
		stopFirstRow = true
	}
	
	pos, _ := db.File.Seek(0, io.SeekCurrent)

	if off := pos % int64(db.BitSize); off != 0 {
		db.File.Write(bytes.Repeat([]byte{'-'}, int(off)))
		pos += off
	}

	buf := make([]byte, 1)
	_, err = db.File.Read(buf)

	for err == nil /* && buf[0] != prefix */ {
		if buf[0] == prefix {
			buf = make([]byte, int64(db.BitSize)-1)
			_, err = db.File.Read(buf)

			buf = bytes.TrimRight(buf, "-\n")
			reInd := regex.Comp(`[\-\n]*@([a-z0-9]+)$`)
			for reInd.Match(buf) {
				buf = reInd.RepFunc(buf, func(data func(int) []byte) []byte {
					if getPos, err := strconv.ParseInt(string(data(1)), 36, 64); err == nil {
						db.File.Seek(getPos*int64(db.BitSize), io.SeekStart)
						b := make([]byte, int64(db.BitSize))
						_, err = db.File.Read(b)
						if err == nil && b[0] == '&' {
							return bytes.TrimRight(b[1:], "-\n")
						}
					}
					return []byte{}
				})
			}

			buf, encErr = decData(db, buf)
			if encErr != nil {
				db.File.Seek(pos + int64(db.BitSize), io.SeekStart)
				buf = make([]byte, 1)
				_, err = db.File.Read(buf)

				if stopFirstRow {
					return Object{}, encErr
				}

				continue
			}

			data := bytes.SplitN(buf, []byte{'='}, 2)
			if len(data) == 0 {
				db.File.Seek(pos + int64(db.BitSize), io.SeekStart)
				buf = make([]byte, 1)
				_, err = db.File.Read(buf)

				if stopFirstRow {
					if encErr != nil {
						return Object{}, encErr
					}
					return Object{}, io.EOF
				}

				continue
			}

			for len(data) < 2 {
				data = append(data, []byte{})
			}

			if (regTypeKey == 0 && bytes.Equal(key, data[0])) || regTypeKey == 1 || regTypeKey == 2 ||
			(regTypeKey == 3 && reKey.Match(data[0])) {
				if (regTypeVal == 0 && bytes.Equal(val, data[1])) || regTypeVal == 1 || regTypeVal == 2 ||
				(regTypeVal == 3 && reVal.Match(data[1])) {
					db.File.Seek(pos + int64(db.BitSize), io.SeekStart)
					
					obj := Object{
						Key: data[0],
						Val: data[1],
						Line: pos / int64(db.BitSize),
					}

					if encErr != nil {
						return obj, encErr
					}

					return obj, nil
				}
			}

			if stopFirstRow {
				if encErr != nil {
					return Object{}, encErr
				}
				return Object{}, io.EOF
			}

			db.File.Seek(pos + int64(db.BitSize), io.SeekStart)
			buf = make([]byte, 1)
			_, err = db.File.Read(buf)
			continue
		}

		pos, _ = db.File.Seek(int64(db.BitSize)-1, io.SeekCurrent)
		_, err = db.File.Read(buf)
	}

	if encErr != nil {
		return Object{}, encErr
	}else if err != nil {
		return Object{}, err
	}

	return Object{}, io.EOF
}

// DelDataObj removes a key value pair from the database, given a prefix
func DelDataObj(db *Database, prefix byte) (Object, error) {
	pos, _ := db.File.Seek(0, io.SeekCurrent)

	if off := pos % int64(db.BitSize); off != 0 {
		db.File.Write(bytes.Repeat([]byte{'-'}, int(off)))
		pos += off
	}

	buf := make([]byte, 1)
	_, err := db.File.Read(buf)

	for err == nil && buf[0] != prefix {
		pos, _ = db.File.Seek(int64(db.BitSize)-1, io.SeekCurrent)
		_, err = db.File.Read(buf)
	}

	if err != nil {
		return Object{}, nil
	}

	buf = make([]byte, int64(db.BitSize)-1)
	_, err = db.File.Read(buf)

	db.File.Seek(int64(db.BitSize) * -1, io.SeekCurrent)
	db.File.Write([]byte{'!'})
	if DebugMode {
		db.File.Write(bytes.Repeat([]byte{'-'}, int(db.BitSize)-2))
		db.File.Write([]byte{'\n'})
	}else{
		db.File.Write(bytes.Repeat([]byte{'-'}, int(db.BitSize)-1))
	}

	buf = bytes.TrimRight(buf, "-\n")
	reInd := regex.Comp(`[\-\n]*@([a-z0-9]+)$`)
	for reInd.Match(buf) {
		buf = reInd.RepFunc(buf, func(data func(int) []byte) []byte {
			if getPos, err := strconv.ParseInt(string(data(1)), 36, 64); err == nil {
				db.File.Seek(getPos*int64(db.BitSize), io.SeekStart)
				b := make([]byte, int64(db.BitSize))
				_, err = db.File.Read(b)
				if err == nil && b[0] == '&' {
					db.File.Seek(int64(db.BitSize) * -1, io.SeekCurrent)
					db.File.Write([]byte{'!'})
					if DebugMode {
						db.File.Write(bytes.Repeat([]byte{'-'}, int(db.BitSize)-2))
						db.File.Write([]byte{'\n'})
					}else{
						db.File.Write(bytes.Repeat([]byte{'-'}, int(db.BitSize)-1))
					}
					
					return bytes.TrimRight(b[1:], "-\n")
				}
			}
			return []byte{}
		})
	}

	data := bytes.SplitN(buf, []byte{'='}, 2)
	for len(data) < 2 {
		data = append(data, []byte{})
	}

	db.File.Seek(pos + int64(db.BitSize), io.SeekStart)

	return Object{
		Key: data[0],
		Val: data[1],
		Line: pos / int64(db.BitSize),
	}, nil
}

// SetDataObj replaces an old key value pair with a new one
//
// note: it is your job to run the `File.Seek` method, and start at the correct position
func SetDataObj(db *Database, prefix byte, key []byte, val []byte) (Object, error) {
	pos, _ := db.File.Seek(0, io.SeekCurrent)

	if off := pos % int64(db.BitSize); off != 0 {
		db.File.Write(bytes.Repeat([]byte{'-'}, int(off)))
		pos += off
	}

	buf := make([]byte, 1)
	_, err := db.File.Read(buf)

	for err == nil && buf[0] != prefix {
		pos, _ = db.File.Seek(int64(db.BitSize)-1, io.SeekCurrent)
		_, err = db.File.Read(buf)
	}

	if err != nil {
		return Object{}, nil
	}


	// set data on object
	obj := Object{
		Key: key,
		Val: val,
		Line: pos / int64(db.BitSize),
	}

	val = regex.JoinBytes(key, '=', val)

	val, err = encData(db, val)
	if err != nil {
		return Object{}, err
	}

	// set data
	off := 1
	if DebugMode {
		off++
	}

	buf = make([]byte, int64(db.BitSize)-1)
	_, err = db.File.Read(buf)
	oldPos, _ := db.File.Seek(int64(db.BitSize) * -1, io.SeekCurrent)

	buf = bytes.TrimRight(buf, "-\n")
	reInd := regex.Comp(`[\-\n]*@([a-z0-9]+)$`)
	for err == nil && reInd.Match(buf) {
		buf = reInd.RepFunc(buf, func(data func(int) []byte) []byte {
			if getPos, err := strconv.ParseInt(string(data(1)), 36, 64); err == nil {
				newPos, err := db.File.Seek(getPos*int64(db.BitSize), io.SeekStart)
				b := make([]byte, int64(db.BitSize))
				_, err = db.File.Read(b)
				if err == nil && b[0] == '&' {
					if len(val) == 0 {
						db.File.Seek(oldPos, io.SeekStart)
						db.File.Write([]byte{'!'})
						if DebugMode {
							db.File.Write(bytes.Repeat([]byte{'-'}, int(db.BitSize)-2))
							db.File.Write([]byte{'\n'})
						}else{
							db.File.Write(bytes.Repeat([]byte{'-'}, int(db.BitSize)-1))
						}
					}else{
						db.File.Seek(oldPos+1, io.SeekStart)

						if len(val) + off > int(db.BitSize) {
							posStr := []byte(strconv.FormatInt(newPos / int64(db.BitSize), 36))

							posStr = append([]byte{'@'}, posStr...)
							offset := int(db.BitSize) - len(posStr) - 1

							if DebugMode {
								offset--
							}

							db.File.Write(val[:offset])
							db.File.Write(posStr)
							val = val[offset:]

							if DebugMode {
								db.File.Write([]byte{'\n'})
							}
						}else{
							db.File.Write(val)
							if len(val) < int(db.BitSize) {
								if DebugMode {
									db.File.Write(bytes.Repeat([]byte{'-'}, int(db.BitSize) - len(val) - 2))
									db.File.Write([]byte{'\n'})
								}else{
									db.File.Write(bytes.Repeat([]byte{'-'}, int(db.BitSize) - len(val) - 1))
								}
							}
							val = []byte{}
						}
					}

					oldPos, _ = db.File.Seek(newPos, io.SeekStart)

					return bytes.TrimRight(b[1:], "-\n")
				}
			}
			return []byte{}
		})
	}


	// add buf to old data
	oldData := bytes.SplitN(buf, []byte{'='}, 2)
	for len(oldData) < 2 {
		oldData = append(oldData, []byte{})
	}
	obj.OldKey = oldData[0]
	obj.OldVal = oldData[1]


	// finish adding new value
	buf = make([]byte, int64(db.BitSize))
	_, err = db.File.Read(buf)
	if err == nil /* && buf[0] == '&' */ {
		if len(val) == 0 {
			db.File.Seek(oldPos, io.SeekStart)
			db.File.Write([]byte{'!'})
			if DebugMode {
				db.File.Write(bytes.Repeat([]byte{'-'}, int(db.BitSize)-2))
				db.File.Write([]byte{'\n'})
			}else{
				db.File.Write(bytes.Repeat([]byte{'-'}, int(db.BitSize)-1))
			}
		}else{
			db.File.Seek(oldPos+1, io.SeekStart)
			posLine := oldPos / int64(db.BitSize)

			if len(val) + off > int(db.BitSize) {
				var posStr []byte
				var useNewPos int64 = -1

				curPos, _ := db.File.Seek(0, io.SeekCurrent)
				db.File.Seek(0, io.SeekStart)

				buf = make([]byte, 1)
				_, err = db.File.Read(buf)
				for err == nil && buf[0] != '!' {
					db.File.Seek(int64(db.BitSize)-1, io.SeekCurrent)
					_, err = db.File.Read(buf)
				}

				addNew := false

				if err == io.EOF {
					addNew = true
					newPos, _ := db.File.Seek(0, io.SeekEnd)
					useNewPos = newPos
					newPos /= int64(db.BitSize)
					posStr = []byte(strconv.FormatInt(newPos, 36))
					posLine = newPos
				}else{
					newPos, _ := db.File.Seek(-1, io.SeekCurrent)
					useNewPos = newPos
					newPos /= int64(db.BitSize)
					posStr = []byte(strconv.FormatInt(newPos, 36))
				}
	
				db.File.Seek(curPos, io.SeekStart)

				posStr = append([]byte{'@'}, posStr...)
				offset := int(db.BitSize) - len(posStr) - 1

				if DebugMode {
					offset--
				}

				db.File.Write(val[:offset])
				db.File.Write(posStr)
				val = val[offset:]

				if DebugMode {
					db.File.Write([]byte{'\n'})
				}

				if useNewPos != -1 {
					db.File.Seek(useNewPos, io.SeekStart)
				}

				db.File.Write([]byte{'&'})

				for len(val) + off > int(db.BitSize) {
					var posStr []byte
					var useNewPos int64 = -1

					if !addNew {
						curPos, _ := db.File.Seek(0, io.SeekCurrent)
						db.File.Seek(int64(db.BitSize)-1, io.SeekCurrent)
			
						_, err = db.File.Read(buf)
						for err == nil && buf[0] != '!' {
							db.File.Seek(int64(db.BitSize)-1, io.SeekCurrent)
							_, err = db.File.Read(buf)
						}
			
						if err == io.EOF {
							addNew = true
							newPos, _ := db.File.Seek(0, io.SeekEnd)
							useNewPos = newPos
							newPos /= int64(db.BitSize)
							posStr = []byte(strconv.FormatInt(newPos, 36))
							posLine = newPos
						}else{
							newPos, _ := db.File.Seek(-1, io.SeekCurrent)
							useNewPos = newPos
							newPos /= int64(db.BitSize)
							posStr = []byte(strconv.FormatInt(newPos, 36))
						}
			
						db.File.Seek(curPos, io.SeekStart)
					}else if addNew {
						posLine++
						posStr = []byte(strconv.FormatInt(posLine, 36))
					}
			
					posStr = append([]byte{'@'}, posStr...)
					offset := int(db.BitSize) - len(posStr) - 1
			
					if DebugMode {
						offset--
					}
			
					db.File.Write(val[:offset])
					db.File.Write(posStr)
					val = val[offset:]
			
					if DebugMode {
						db.File.Write([]byte{'\n'})
					}
			
					if useNewPos != -1 {
						db.File.Seek(useNewPos, io.SeekStart)
					}
			
					db.File.Write([]byte{'&'})
				}

				db.File.Write(val)
				if len(val) < int(db.BitSize) {
					if DebugMode {
						db.File.Write(bytes.Repeat([]byte{'-'}, int(db.BitSize) - len(val) - 2))
						db.File.Write([]byte{'\n'})
					}else{
						db.File.Write(bytes.Repeat([]byte{'-'}, int(db.BitSize) - len(val) - 1))
					}
				}
			}else{
				db.File.Write(val)
				if len(val) < int(db.BitSize) {
					if DebugMode {
						db.File.Write(bytes.Repeat([]byte{'-'}, int(db.BitSize) - len(val) - 2))
						db.File.Write([]byte{'\n'})
					}else{
						db.File.Write(bytes.Repeat([]byte{'-'}, int(db.BitSize) - len(val) - 1))
					}
				}
			}
		}
	}

	return obj, nil
}


func encData(db *Database, buf []byte) ([]byte, error) {
	var err error
	
	if db.encKey != nil {
		buf, err = crypt.CFB.Encrypt(buf, db.encKey)
		if err != nil {
			return nil, err
		}
	}else if !DebugMode {
		buf = smaz.Compress(buf)
	}

	re, err := regex.CompTry(`[%=,@\-!\n%1]`, string(db.PrefixList))
	if err != nil {
		return nil, err
	}

	charList := append([]byte("%=,@-!\n"), db.PrefixList...)
	buf = re.RepFunc(buf, func(data func(int) []byte) []byte {
		if i := bytes.IndexRune(charList, rune(data(0)[0])); i != -1 {
			return []byte{'%', strconv.Itoa(i)[0], '%'}
		}
		return []byte{}
	})

	return buf, nil
}

func decData(db *Database, buf []byte) ([]byte, error) {
	var err error
	if db.encKey != nil {
		buf, err = crypt.CFB.Decrypt(buf, db.encKey)
		if err != nil {
			return nil, err
		}
	}else if !DebugMode {
		buf, err = smaz.Decompress(buf)
		if err != nil {
			return nil, err
		}
	}

	charList := append([]byte("%=,@-!\n"), db.PrefixList...)
	buf = regex.Comp(`%([0-9]+)%`).RepFunc(buf, func(data func(int) []byte) []byte {
		if i, err := strconv.Atoi(string(data(1))); err == nil {
			return []byte{charList[i]}
		}
		return []byte{}
	})

	return buf, nil
}
