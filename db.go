package db

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/AspieSoft/go-regex-re2/v2"
	"github.com/AspieSoft/goutil/crypt"
	"github.com/alphadose/haxmap"
	"github.com/cespare/go-smaz"
)

var DebugMode = false

const coreChars = "%=,@#!-\n"
const maxDatabaseSize uint64 = 99999999999999 // 14 (64000 bit - max lines = 1 billion)

type Database struct {
	file *os.File
	path string
	bitSize uint16
	prefixList []byte
	cache *haxmap.Map[string, *Table]
	mu sync.Mutex
	encKey []byte
}

type dbObj struct {
	key []byte
	val []byte
	line int64

	oldKey []byte
	oldVal []byte
}


// Open opens an existing database or creates a new one
//
// @bitSize tells the database what bit size to use (this value must always be consistent)
//  - (default: 128)
//  - (0 = default 128)
//  - (min = 64)
//  - (max = 64000)
// note: in debug mode, (min = 16)
func Open(path string, encKey []byte, bitSize ...uint16) (*Database, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return &Database{}, err
	}

	path = string(regex.Comp(`[\\/]+$`).RepStr([]byte(path), []byte{}))
	if !strings.HasSuffix(path, ".db") {
		path += ".db"
	}

	os.MkdirAll(string(regex.Comp(`[\\/][^\\/]+$`).RepStr([]byte(path), []byte{})), 0755)

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0755)
	if err != nil {
		return &Database{}, err
	}

	newFile := false
	if _, err = file.ReadAt(make([]byte, 1), 0); err == io.EOF {
		newFile = true
	}

	bSize := uint16(0)
	if len(bitSize) != 0 {
		bSize = bitSize[0]
	}

	if bSize == 0 {
		bSize = 128
	}else if DebugMode && bSize < 16 {
		bSize = 16
	}else if !DebugMode && bSize < 64 {
		bSize = 64
	}else if bSize > 64000 {
		bSize = 64000
	}

	db := &Database{
		file: file,
		path: path,
		bitSize: 10,
		prefixList: []byte("$:~"),
		cache: haxmap.New[string, *Table](),
		encKey: encKey,
	}

	if newFile {
		db.bitSize = bSize
		s := strconv.FormatUint(uint64(bSize), 36)
		sp := int(bSize - 5) - len(s)
		if DebugMode {
			sp--
		}
		if sp < 0 || len(s) > 5 {
			return &Database{}, errors.New("bit size too large") // user specified bit for a new database
		}

		file.WriteAt([]byte("#bit="+s+strings.Repeat("-", sp)), 0)
		if DebugMode {
			file.WriteAt([]byte{'\n'}, int64(bSize-1))
		}

		addDataObj(db, '#', []byte("enc"), []byte("enc"))
	}else{
		buf := make([]byte, 10)
		_, err = file.ReadAt(buf, 0)
		if err != nil || !bytes.HasPrefix(buf, []byte("#bit=")) {
			return &Database{}, errors.New("defined bit size too large") // current bit size defined by the database file
		}

		if i, err := strconv.ParseUint(string(bytes.TrimRight(buf[5:], "-")), 36, 16); err == nil && i != 0 {
			bSize = uint16(i)
		}else{
			return &Database{}, errors.New("defined bit size is NaN:36 (not a base36 number)") // current bit size defined by the database file
		}
		db.bitSize = bSize

		file.Seek(int64(bSize), io.SeekStart)
		if encData, err := getDataObj(db, '#', []byte("enc"), []byte{0}); err != nil || !bytes.Equal(encData.val, []byte("enc")) {
			return &Database{}, errors.New("failed to decrypt database")
		}
	}

	return db, nil
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

	val, err = encData(db, val)
	if err != nil {
		return dbObj{}, err
	}

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

			buf, encErr = decData(db, buf)
			if encErr != nil {
				db.file.Seek(pos + int64(db.bitSize), io.SeekStart)
				buf = make([]byte, 1)
				_, err = db.file.Read(buf)

				if stopFirstRow {
					return dbObj{}, encErr
				}

				//todo: fix loop continuing if encryption fails
				continue
			}

			data := bytes.SplitN(buf, []byte{'='}, 2)
			if len(data) == 0 {
				db.file.Seek(pos + int64(db.bitSize), io.SeekStart)
				buf = make([]byte, 1)
				_, err = db.file.Read(buf)

				if stopFirstRow {
					if encErr != nil {
						return dbObj{}, encErr
					}
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

					obj := dbObj{
						key: data[0],
						val: data[1],
						line: pos / int64(db.bitSize),
					}

					if encErr != nil {
						return obj, encErr
					}

					return obj, nil
				}
			}

			if stopFirstRow {
				if encErr != nil {
					return dbObj{}, encErr
				}
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

	if encErr != nil {
		return dbObj{}, encErr
	}else if err != nil {
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

	val, err = encData(db, val)
	if err != nil {
		return dbObj{}, err
	}

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


func encData(db *Database, buf []byte) ([]byte, error) {
	var err error

	if db.encKey != nil {
		buf, err = crypt.CFB.Encrypt(append(buf, []byte("#enc")...), db.encKey)
		if err != nil {
			return nil, err
		}
	}else if !DebugMode {
		buf = smaz.Compress(buf)
	}

	// for some reason, using regex lead to inconsistent results and caused issues with decoding
	res := []byte{}
	charList := append([]byte(coreChars), db.prefixList...)
	for i := 0; i < len(buf); i++ {
		if ind := bytes.IndexRune(charList, rune(buf[i])); ind != -1 {
			res = append(res, buf[:i]...)
			res = append(res, '%')
			res = append(res, []byte(strconv.Itoa(ind))...)
			res = append(res, '%')
			buf = buf[i+1:]
			i = -1
		}
	}
	buf = append(res, buf...)
	res = nil

	return buf, nil
}

func decData(db *Database, buf []byte) ([]byte, error) {
	// for some reason, using regex lead to inconsistent results and caused issues with decoding
	res := []byte{}
	charList := append([]byte(coreChars), db.prefixList...)
	var b []byte
	for i := 0; i < len(buf); i++ {
		if buf[i] == '%' {
			if b == nil {
				res = append(res, buf[:i]...)
				b = []byte{}
			}else{
				buf = buf[i+1:]
				i = -1

				if n, err := strconv.Atoi(string(b)); err == nil && n < len(charList) {
					res = append(res, charList[n])
				}
				b = nil
			}
		}else if b != nil {
			b = append(b, buf[i])
		}
	}
	buf = append(res, buf...)
	res = nil

	var err error
	if db.encKey != nil {
		buf, err = crypt.CFB.Decrypt(buf, db.encKey)
		if err != nil {
			return nil, err
		}else if !bytes.HasSuffix(buf, []byte("#enc")) {
			return nil, errors.New("failed to decrypt")
		}
		buf = buf[:len(buf)-4]
	}else if !DebugMode {
		buf, err = smaz.Decompress(buf)
		if err != nil {
			return nil, err
		}
	}

	return buf, nil
}
