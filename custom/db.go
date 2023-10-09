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
	"github.com/AspieSoft/goutil/v7"
)

const DebugMode = true

const maxDatabaseSize uint64 = 99999999999999 // 14 (64000 bit - max lines = 1 billion)

type Database struct {
	file *os.File
	path string
	bitSize uint16
	prefixList []byte
	mu sync.Mutex
}

type CustomDB struct {
	prefixList []byte
}

type dbObj struct {
	key []byte
	val []byte
	line int64

	oldKey []byte
	oldVal []byte
}


func New(prefixList []byte, bitSize uint16) (*CustomDB, error) {
	for _, prefix := range prefixList {
		if goutil.Contains([]byte("%=,@-!"), prefix) {
			return &CustomDB{}, errors.New("'"+string(prefix)+"' is a core prefix")
		}
	}

	return &CustomDB{

	}, nil
}

func Open(path string, bitSize uint16, prefixList []byte) (*Database, error) {
	for _, prefix := range prefixList {
		if goutil.Contains([]byte("%=,@-!"), prefix) {
			return &Database{}, errors.New("'"+string(prefix)+"' is a core prefix")
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
		file: file,
		path: path,
		bitSize: bitSize,
		prefixList: []byte("$:"),
	}, nil
}

// Close closes the database file
func (db *Database) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	err1 := db.file.Sync()
	err2 := db.file.Close()

	if err2 == nil {
		return err1
	}
	return err2
}



//todo: consider adding an automated method to handle possible long term errors down the road
// example: if the (@n) index gets too large, the bit size may need to increase (unless the data being added is too large to be normal)
// example: if the pos in the file starts to get close to the integer limit, a new file (i.e. test.1.db) may need to extend the existing file to hold more data
//  this senerio may also require the first file to be moved to an index (i.e. test.0.db), and placed inside a folder with the original name (i.e. test.db - folder)
// example: handle potential power outage recovery. keep a queue of database operations inside a file (using the core database functions for simplicity, but in a different format)

//todo: add an optional `AdvancedDatabase` struct that allows users to utilize the core methods and build their own database structure
// users should also be able to choose the default bit size, and what data object prefixes to use (which will need to be escaped)
// the core prefixes should also be escaped, and should warn users that they exist, incase they try to add one of them into their prefix list
// core prefixes: [%=,@-!], and debug char [\n]
// default database prefixes: [$:] (note: do not allow users to use whitespace characters as prefixes)


//todo: add compression and (optional) encryption to core database methods
// also ensure values do not include special chars from database syntax [%$:=,@-!]

func addDataObj(db *Database, prefix byte, key []byte, val []byte) (dbObj, error) {
	pos, _ := db.file.Seek(0, io.SeekStart)

	if off := pos % int64(db.bitSize); off != 0 {
		db.file.Write(bytes.Repeat([]byte{'-'}, int(off)))
		pos += off
	}

	buf := make([]byte, 1)
	_, err := db.file.Read(buf)
	for err == nil && buf[0] != '!' {
		pos, _ = db.file.Seek(int64(db.bitSize)-1, io.SeekCurrent)
		_, err = db.file.Read(buf)
	}

	addNew := false
	if err == io.EOF {
		addNew = true
		pos, _ = db.file.Seek(0, io.SeekEnd)
	}else{
		pos, _ = db.file.Seek(-1, io.SeekCurrent)
	}

	obj := dbObj{
		key: key,
		val: val,
		line: pos / int64(db.bitSize),
	}

	val = regex.JoinBytes(key, '=', val)

	posLine := pos / int64(db.bitSize)

	// add data
	db.file.Write([]byte{prefix})

	off := 1
	if DebugMode {
		off++
	}

	for len(val) + off > int(db.bitSize) {
		var posStr []byte
		var useNewPos int64 = -1

		if !addNew {
			curPos, _ := db.file.Seek(0, io.SeekCurrent)
			db.file.Seek(int64(db.bitSize)-1, io.SeekCurrent)

			_, err = db.file.Read(buf)
			for err == nil && buf[0] != '!' {
				db.file.Seek(int64(db.bitSize)-1, io.SeekCurrent)
				_, err = db.file.Read(buf)
			}

			if err == io.EOF {
				addNew = true
				newPos, _ := db.file.Seek(0, io.SeekEnd)
				useNewPos = newPos
				newPos /= int64(db.bitSize)
				posStr = []byte(strconv.FormatInt(newPos, 36))
				posLine = newPos
			}else{
				newPos, _ := db.file.Seek(-1, io.SeekCurrent)
				useNewPos = newPos
				newPos /= int64(db.bitSize)
				posStr = []byte(strconv.FormatInt(newPos, 36))
			}

			db.file.Seek(curPos, io.SeekStart)
		}else if addNew {
			posLine++
			posStr = []byte(strconv.FormatInt(posLine, 36))
		}

		posStr = append([]byte{'@'}, posStr...)
		offset := int(db.bitSize) - len(posStr) - 1

		if DebugMode {
			offset--
		}

		db.file.Write(val[:offset])
		db.file.Write(posStr)
		val = val[offset:]

		if DebugMode {
			db.file.Write([]byte{'\n'})
		}

		if useNewPos != -1 {
			db.file.Seek(useNewPos, io.SeekStart)
		}

		db.file.Write([]byte{'&'})
	}

	db.file.Write(val)
	if len(val) < int(db.bitSize) {
		if DebugMode {
			db.file.Write(bytes.Repeat([]byte{'-'}, int(db.bitSize) - len(val) - 2))
			db.file.Write([]byte{'\n'})
		}else{
			db.file.Write(bytes.Repeat([]byte{'-'}, int(db.bitSize) - len(val) - 1))
		}
	}

	return obj, nil
}

func getDataObj(db *Database, prefix byte, key []byte, val []byte, stopAfterFirstRow ...bool) (dbObj, error) {
	regTypeKey := uint8(0)
	regTypeVal := uint8(0)
	var reKey *regex.Regexp
	var reVal *regex.Regexp
	var err error

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
				return dbObj{}, err
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
				return dbObj{}, err
			}
		}
	}

	stopFirstRow := false
	if len(stopAfterFirstRow) != 0 && stopAfterFirstRow[0] == true {
		stopFirstRow = true
	}
	
	pos, _ := db.file.Seek(0, io.SeekCurrent)

	if off := pos % int64(db.bitSize); off != 0 {
		db.file.Write(bytes.Repeat([]byte{'-'}, int(off)))
		pos += off
	}

	buf := make([]byte, 1)
	_, err = db.file.Read(buf)

	for err == nil /* && buf[0] != prefix */ {
		if buf[0] == prefix {
			buf = make([]byte, int64(db.bitSize)-1)
			_, err = db.file.Read(buf)

			buf = bytes.TrimRight(buf, "-\n")
			reInd := regex.Comp(`[\-\n]*@([a-z0-9]+)$`)
			for reInd.Match(buf) {
				buf = reInd.RepFunc(buf, func(data func(int) []byte) []byte {
					if getPos, err := strconv.ParseInt(string(data(1)), 36, 64); err == nil {
						db.file.Seek(getPos*int64(db.bitSize), io.SeekStart)
						b := make([]byte, int64(db.bitSize))
						_, err = db.file.Read(b)
						if err == nil && b[0] == '&' {
							return bytes.TrimRight(b[1:], "-\n")
						}
					}
					return []byte{}
				})
			}

			data := bytes.SplitN(buf, []byte{'='}, 2)
			if len(data) == 0 {
				db.file.Seek(pos + int64(db.bitSize), io.SeekStart)
				buf = make([]byte, 1)
				_, err = db.file.Read(buf)

				if stopFirstRow {
					return dbObj{}, io.EOF
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
					db.file.Seek(pos + int64(db.bitSize), io.SeekStart)

					return dbObj{
						key: data[0],
						val: data[1],
						line: pos / int64(db.bitSize),
					}, nil
				}
			}

			if stopFirstRow {
				return dbObj{}, io.EOF
			}

			db.file.Seek(pos + int64(db.bitSize), io.SeekStart)
			buf = make([]byte, 1)
			_, err = db.file.Read(buf)
			continue
		}

		pos, _ = db.file.Seek(int64(db.bitSize)-1, io.SeekCurrent)
		_, err = db.file.Read(buf)
	}

	if err != nil {
		return dbObj{}, err
	}

	return dbObj{}, io.EOF
}

func delDataObj(db *Database, prefix byte) (dbObj, error) {
	pos, _ := db.file.Seek(0, io.SeekCurrent)

	if off := pos % int64(db.bitSize); off != 0 {
		db.file.Write(bytes.Repeat([]byte{'-'}, int(off)))
		pos += off
	}

	buf := make([]byte, 1)
	_, err := db.file.Read(buf)

	for err == nil && buf[0] != prefix {
		pos, _ = db.file.Seek(int64(db.bitSize)-1, io.SeekCurrent)
		_, err = db.file.Read(buf)
	}

	if err != nil {
		return dbObj{}, nil
	}

	buf = make([]byte, int64(db.bitSize)-1)
	_, err = db.file.Read(buf)

	db.file.Seek(int64(db.bitSize) * -1, io.SeekCurrent)
	db.file.Write([]byte{'!'})
	if DebugMode {
		db.file.Write(bytes.Repeat([]byte{'-'}, int(db.bitSize)-2))
		db.file.Write([]byte{'\n'})
	}else{
		db.file.Write(bytes.Repeat([]byte{'-'}, int(db.bitSize)-1))
	}

	buf = bytes.TrimRight(buf, "-\n")
	reInd := regex.Comp(`[\-\n]*@([a-z0-9]+)$`)
	for reInd.Match(buf) {
		buf = reInd.RepFunc(buf, func(data func(int) []byte) []byte {
			if getPos, err := strconv.ParseInt(string(data(1)), 36, 64); err == nil {
				db.file.Seek(getPos*int64(db.bitSize), io.SeekStart)
				b := make([]byte, int64(db.bitSize))
				_, err = db.file.Read(b)
				if err == nil && b[0] == '&' {
					db.file.Seek(int64(db.bitSize) * -1, io.SeekCurrent)
					db.file.Write([]byte{'!'})
					if DebugMode {
						db.file.Write(bytes.Repeat([]byte{'-'}, int(db.bitSize)-2))
						db.file.Write([]byte{'\n'})
					}else{
						db.file.Write(bytes.Repeat([]byte{'-'}, int(db.bitSize)-1))
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

	db.file.Seek(pos + int64(db.bitSize), io.SeekStart)

	return dbObj{
		key: data[0],
		val: data[1],
		line: pos / int64(db.bitSize),
	}, nil
}

func setDataObj(db *Database, prefix byte, key []byte, val []byte) (dbObj, error) {
	pos, _ := db.file.Seek(0, io.SeekCurrent)

	if off := pos % int64(db.bitSize); off != 0 {
		db.file.Write(bytes.Repeat([]byte{'-'}, int(off)))
		pos += off
	}

	buf := make([]byte, 1)
	_, err := db.file.Read(buf)

	for err == nil && buf[0] != prefix {
		pos, _ = db.file.Seek(int64(db.bitSize)-1, io.SeekCurrent)
		_, err = db.file.Read(buf)
	}

	if err != nil {
		return dbObj{}, nil
	}


	// set data on object
	obj := dbObj{
		key: key,
		val: val,
		line: pos / int64(db.bitSize),
	}

	val = regex.JoinBytes(key, '=', val)

	// set data
	off := 1
	if DebugMode {
		off++
	}

	buf = make([]byte, int64(db.bitSize)-1)
	_, err = db.file.Read(buf)
	oldPos, _ := db.file.Seek(int64(db.bitSize) * -1, io.SeekCurrent)

	buf = bytes.TrimRight(buf, "-\n")
	reInd := regex.Comp(`[\-\n]*@([a-z0-9]+)$`)
	for err == nil && reInd.Match(buf) {
		buf = reInd.RepFunc(buf, func(data func(int) []byte) []byte {
			if getPos, err := strconv.ParseInt(string(data(1)), 36, 64); err == nil {
				newPos, err := db.file.Seek(getPos*int64(db.bitSize), io.SeekStart)
				b := make([]byte, int64(db.bitSize))
				_, err = db.file.Read(b)
				if err == nil && b[0] == '&' {
					if len(val) == 0 {
						db.file.Seek(oldPos, io.SeekStart)
						db.file.Write([]byte{'!'})
						if DebugMode {
							db.file.Write(bytes.Repeat([]byte{'-'}, int(db.bitSize)-2))
							db.file.Write([]byte{'\n'})
						}else{
							db.file.Write(bytes.Repeat([]byte{'-'}, int(db.bitSize)-1))
						}
					}else{
						db.file.Seek(oldPos+1, io.SeekStart)

						if len(val) + off > int(db.bitSize) {
							posStr := []byte(strconv.FormatInt(newPos / int64(db.bitSize), 36))

							posStr = append([]byte{'@'}, posStr...)
							offset := int(db.bitSize) - len(posStr) - 1

							if DebugMode {
								offset--
							}

							db.file.Write(val[:offset])
							db.file.Write(posStr)
							val = val[offset:]

							if DebugMode {
								db.file.Write([]byte{'\n'})
							}
						}else{
							db.file.Write(val)
							if len(val) < int(db.bitSize) {
								if DebugMode {
									db.file.Write(bytes.Repeat([]byte{'-'}, int(db.bitSize) - len(val) - 2))
									db.file.Write([]byte{'\n'})
								}else{
									db.file.Write(bytes.Repeat([]byte{'-'}, int(db.bitSize) - len(val) - 1))
								}
							}
							val = []byte{}
						}
					}

					oldPos, _ = db.file.Seek(newPos, io.SeekStart)

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
	obj.oldKey = oldData[0]
	obj.oldVal = oldData[1]


	// finish adding new value
	buf = make([]byte, int64(db.bitSize))
	_, err = db.file.Read(buf)
	if err == nil /* && buf[0] == '&' */ {
		if len(val) == 0 {
			db.file.Seek(oldPos, io.SeekStart)
			db.file.Write([]byte{'!'})
			if DebugMode {
				db.file.Write(bytes.Repeat([]byte{'-'}, int(db.bitSize)-2))
				db.file.Write([]byte{'\n'})
			}else{
				db.file.Write(bytes.Repeat([]byte{'-'}, int(db.bitSize)-1))
			}
		}else{
			db.file.Seek(oldPos+1, io.SeekStart)
			posLine := oldPos / int64(db.bitSize)

			if len(val) + off > int(db.bitSize) {
				var posStr []byte
				var useNewPos int64 = -1

				curPos, _ := db.file.Seek(0, io.SeekCurrent)
				db.file.Seek(0, io.SeekStart)

				buf = make([]byte, 1)
				_, err = db.file.Read(buf)
				for err == nil && buf[0] != '!' {
					db.file.Seek(int64(db.bitSize)-1, io.SeekCurrent)
					_, err = db.file.Read(buf)
				}

				addNew := false

				if err == io.EOF {
					addNew = true
					newPos, _ := db.file.Seek(0, io.SeekEnd)
					useNewPos = newPos
					newPos /= int64(db.bitSize)
					posStr = []byte(strconv.FormatInt(newPos, 36))
					posLine = newPos
				}else{
					newPos, _ := db.file.Seek(-1, io.SeekCurrent)
					useNewPos = newPos
					newPos /= int64(db.bitSize)
					posStr = []byte(strconv.FormatInt(newPos, 36))
				}
	
				db.file.Seek(curPos, io.SeekStart)

				posStr = append([]byte{'@'}, posStr...)
				offset := int(db.bitSize) - len(posStr) - 1

				if DebugMode {
					offset--
				}

				db.file.Write(val[:offset])
				db.file.Write(posStr)
				val = val[offset:]

				if DebugMode {
					db.file.Write([]byte{'\n'})
				}

				if useNewPos != -1 {
					db.file.Seek(useNewPos, io.SeekStart)
				}

				db.file.Write([]byte{'&'})

				for len(val) + off > int(db.bitSize) {
					var posStr []byte
					var useNewPos int64 = -1

					if !addNew {
						curPos, _ := db.file.Seek(0, io.SeekCurrent)
						db.file.Seek(int64(db.bitSize)-1, io.SeekCurrent)
			
						_, err = db.file.Read(buf)
						for err == nil && buf[0] != '!' {
							db.file.Seek(int64(db.bitSize)-1, io.SeekCurrent)
							_, err = db.file.Read(buf)
						}
			
						if err == io.EOF {
							addNew = true
							newPos, _ := db.file.Seek(0, io.SeekEnd)
							useNewPos = newPos
							newPos /= int64(db.bitSize)
							posStr = []byte(strconv.FormatInt(newPos, 36))
							posLine = newPos
						}else{
							newPos, _ := db.file.Seek(-1, io.SeekCurrent)
							useNewPos = newPos
							newPos /= int64(db.bitSize)
							posStr = []byte(strconv.FormatInt(newPos, 36))
						}
			
						db.file.Seek(curPos, io.SeekStart)
					}else if addNew {
						posLine++
						posStr = []byte(strconv.FormatInt(posLine, 36))
					}
			
					posStr = append([]byte{'@'}, posStr...)
					offset := int(db.bitSize) - len(posStr) - 1
			
					if DebugMode {
						offset--
					}
			
					db.file.Write(val[:offset])
					db.file.Write(posStr)
					val = val[offset:]
			
					if DebugMode {
						db.file.Write([]byte{'\n'})
					}
			
					if useNewPos != -1 {
						db.file.Seek(useNewPos, io.SeekStart)
					}
			
					db.file.Write([]byte{'&'})
				}

				db.file.Write(val)
				if len(val) < int(db.bitSize) {
					if DebugMode {
						db.file.Write(bytes.Repeat([]byte{'-'}, int(db.bitSize) - len(val) - 2))
						db.file.Write([]byte{'\n'})
					}else{
						db.file.Write(bytes.Repeat([]byte{'-'}, int(db.bitSize) - len(val) - 1))
					}
				}
			}else{
				db.file.Write(val)
				if len(val) < int(db.bitSize) {
					if DebugMode {
						db.file.Write(bytes.Repeat([]byte{'-'}, int(db.bitSize) - len(val) - 2))
						db.file.Write([]byte{'\n'})
					}else{
						db.file.Write(bytes.Repeat([]byte{'-'}, int(db.bitSize) - len(val) - 1))
					}
				}
			}
		}
	}

	return obj, nil
}

