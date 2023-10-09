package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/AspieSoft/go-regex-re2/v2"
	"github.com/AspieSoft/goutil/v7"
	"github.com/alphadose/haxmap"
)

const DebugMode = true

type Database struct {
	file *os.File
	path string
	bitSize uint16
	prefixList []byte
	cache *haxmap.Map[string, *Table]
	mu sync.Mutex
}

type Table struct {
	db *Database
	Name string
	key []byte
	val []byte
	line int64
}

type Row struct {
	table *Table
	Key string
	Value string
	line int64
}

type dbObj struct {
	key []byte
	val []byte
	line int64

	oldKey []byte
	oldVal []byte
}


const maxDatabaseSize uint64 = 99999999999999 // 14 (64000 bit - max lines = 1 billion)

func main(){
	db, err := New("test.db", 16)
	if err != nil {
		panic(err)
	}

	if DebugMode {
		_ = fmt.Print
		db.file.Truncate(0)
	}

	db.AddTable("MyTable")
	table2, err := db.AddTable("MyTable2")
	table, err := db.GetTable("MyTable")
	table.AddRow("Row1", "val1")
	table.AddRow("Row2", "val2")
	table.GetRow("Row1")
	table2.Del()
	// db.DelTable("MyTable2")

	//todo: use this to test the setDataObj method
	/* db.addDataObj('$', []byte("MyTable_MoreTextToMakeThisLonger"), []byte("test"))

	db.file.Seek(0, io.SeekStart)
	db.getDataObj('$', []byte("MyTable_MoreTextToMakeThisLonger"), []byte{0})

	db.file.Seek(0, io.SeekStart)
	db.setDataObj('$', []byte("MyTable"), []byte("MyVal"))
	// db.setDataObj('$', []byte("MyTable"), []byte("MyVal_MoreTextToMakeThisLonger"))
	// db.setDataObj('$', []byte("MyTable"), []byte("MyVal_MoreTextToMakeThisLonger_MoreTextToMakeThisLonger")) */

	db.Optimize()
}

func New(path string, bitSize uint16) (*Database, error) {
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
		cache: haxmap.New[string, *Table](),
	}, nil
}

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

func (db *Database) addDataObj(prefix byte, key []byte, val []byte) (dbObj, error) {
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

func (db *Database) getDataObj(prefix byte, key []byte, val []byte, stopAfterFirstRow ...bool) (dbObj, error) {
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

func (db *Database) rmDataObj(prefix byte) (dbObj, error) {
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

func (db *Database) setDataObj(prefix byte, key []byte, val []byte) (dbObj, error) {
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


// Optimize will optimize a database file by cloning the tables and their rows to a new file
//
// this method will remove any orphaned data (rows without a table, etc),
// and will move existing tables to the top of the database file for quicker access
//
// row indexes are referenced from the tables, so having tables at the top is best for performance
func (db *Database) Optimize() (*Database, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	
	db.file.Sync()
	
	file, err := os.OpenFile(db.path+".opt", os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0755)
	if err != nil {
		return db, err
	}
	defer file.Close()

	newDB := &Database{
		file: file,
		path: db.path+".opt",
		bitSize: db.bitSize,
	}

	tableList, err := db.FindTables([]byte{0})
	if err != nil {
		return db, nil
	}

	newTables := make([]*Table, len(tableList))
	for i, table := range tableList {
		if tb, err := newDB.AddTable(table.Name); err == nil {
			newTables[i] = tb
		}
	}

	for i, table := range tableList {
		if tb := newTables[i]; tb != nil {
			if rowList, err := table.FindRows([]byte{0}, []byte{0}); err == nil {
				for _, row := range rowList {
					tb.AddRow(row.Key, row.Value)
				}
			}
		}
	}

	file.Sync()
	file.Close()
	db.file.Close()
	os.Remove(db.path)
	os.Rename(db.path+".opt", db.path)

	file, err = os.OpenFile(db.path, os.O_CREATE|os.O_RDWR, 0755)
	db.file = file
	if err != nil {
		return db, err
	}

	return db, nil
}

// AddTable adds a new table to the database
// this method returns the new table
func (db *Database) AddTable(name string) (*Table, error) {
	keyB := goutil.Clean.Bytes([]byte(name))
	keyB = bytes.TrimLeftFunc(keyB, func(r rune) bool {
		return r == 0
	})

	db.mu.Lock()
	defer db.mu.Unlock()

	// ensure table does not already exist
	db.file.Seek(0, io.SeekStart)
	if table, err := db.getDataObj('$', keyB, []byte{0}); err == nil {
		return &Table{
			db: db,
			Name: string(table.key),
			key: table.key,
			val: table.val,
			line: table.line,
		}, errors.New("table already exists")
	}

	table, err := db.addDataObj('$', keyB, []byte{})
	if err != nil {
		return &Table{db: db}, err
	}

	newTable := &Table{
		db: db,
		Name: string(table.key),
		key: table.key,
		val: table.val,
		line: table.line,
	}

	//todo: add table to cache

	return newTable, nil
}

// GetTable retrieves an existing table from the database
func (db *Database) GetTable(name string) (*Table, error) {
	keyB := goutil.Clean.Bytes([]byte(name))
	keyB = bytes.TrimLeftFunc(keyB, func(r rune) bool {
		return r == 0
	})

	db.mu.Lock()
	defer db.mu.Unlock()

	//todo: get table from cache

	db.file.Seek(0, io.SeekStart)
	table, err := db.getDataObj('$', keyB, []byte{0})
	if err != nil {
		return &Table{db: db}, err
	}

	newTable := &Table{
		db: db,
		Name: string(table.key),
		key: table.key,
		val: table.val,
		line: table.line,
	}

	//todo: add table to cache

	return newTable, nil
}

// FindTables allows you to do a more complex search for a list of tables
//
// for security, you can authorize a regex search by setting the first byte of the name to 0
//
// if you include nothing else, or a '*' as the second byte, the name will match all tables
//
// anything else with a 0 as first byte will run an RE2 regex match
//
// if you are dealing with user input, it is recommended to sanitize it and remove the first byte of 0,
// to ensure the input cannot run regex, and will be treated as a literal string
func (db *Database) FindTables(name []byte) ([]*Table, error) {
	resTables := []*Table{}

	db.mu.Lock()
	defer db.mu.Unlock()

	db.file.Seek(0, io.SeekStart)
	for {
		table, err := db.getDataObj('$', name, []byte{0})
		if err != nil {
			break
		}

		newTable := &Table{
			db: db,
			Name: string(table.key),
			key: table.key,
			val: table.val,
			line: table.line,
		}
	
		//todo: add table to cache

		resTables = append(resTables, newTable)
	}

	if len(resTables) == 0 {
		return resTables, io.EOF
	}

	return resTables, nil
}

// Del removes the table from the database
func (table *Table) Del() error {
	table.db.mu.Lock()
	defer table.db.mu.Unlock()
	
	table.db.file.Seek(table.line * int64(table.db.bitSize), io.SeekStart)
	_, err := table.db.rmDataObj('$')

	rowList := bytes.Split(table.val, []byte{','})
	for _, rowLine := range rowList {
		if line, err := strconv.ParseInt(string(rowLine), 36, 64); err == nil {
			table.db.file.Seek(line * int64(table.db.bitSize), io.SeekStart)
			table.db.rmDataObj(':')
		}
	}

	table.line = -1

	return err
}

// Rename changes the name of the table
func (table *Table) Rename(name string) error {
	keyB := goutil.Clean.Bytes([]byte(name))
	keyB = bytes.TrimLeftFunc(keyB, func(r rune) bool {
		return r == 0
	})

	table.db.mu.Lock()
	defer table.db.mu.Unlock()

	table.db.file.Seek(table.line * int64(table.db.bitSize), io.SeekStart)
	tb, err := table.db.setDataObj('$', keyB, table.val)
	if err != nil {
		return err
	}

	table.Name = string(tb.key)
	table.key = tb.key
	table.val = tb.val

	//todo: add table to cache

	return nil
}

// AddRow adds a new key value pair to the table
//
// this method returns the new row
func (table *Table) AddRow(key string, value string) (*Row, error) {
	keyB := goutil.Clean.Bytes([]byte(key))
	keyB = bytes.TrimLeftFunc(keyB, func(r rune) bool {
		return r == 0
	})

	valB := goutil.Clean.Bytes([]byte(value))
	valB = bytes.TrimLeftFunc(valB, func(r rune) bool {
		return r == 0
	})

	table.db.mu.Lock()
	defer table.db.mu.Unlock()

	// ensure row does not already exist
	rowList := bytes.Split(table.val, []byte{','})
	for _, rowLine := range rowList {
		if line, err := strconv.ParseInt(string(rowLine), 36, 64); err == nil {
			table.db.file.Seek(line * int64(table.db.bitSize), io.SeekStart)
			if row, err := table.db.getDataObj(':', keyB, []byte{0}, true); err == nil {
				newRow := &Row{
					table: table,
					Key: string(row.key),
					Value: string(row.val),
					line: row.line,
				}

				//todo: add row to table cache

				return newRow, errors.New("row already exists")
			}
		}
	}

	row, err := table.db.addDataObj(':', keyB, valB)
	if err != nil {
		return &Row{table: table}, err
	}

	table.db.file.Seek(table.line * int64(table.db.bitSize), io.SeekStart)
	if len(table.val) == 0 {
		table.val = []byte(strconv.FormatInt(row.line, 36))
	}else{
		table.val = append(table.val, append([]byte{','}, strconv.FormatInt(row.line, 36)...)...)
	}
	table.db.setDataObj('$', table.key, table.val)

	newRow := &Row{
		table: table,
		Key: string(row.key),
		Value: string(row.val),
		line: row.line,
	}

	//todo: add row to cache

	return newRow, nil
}

// GetRow retrieves an existing row from the table
func (table *Table) GetRow(key string) (*Row, error) {
	keyB := goutil.Clean.Bytes([]byte(key))
	keyB = bytes.TrimLeftFunc(keyB, func(r rune) bool {
		return r == 0
	})

	table.db.mu.Lock()
	defer table.db.mu.Unlock()

	//todo: get row from table cache

	rowList := bytes.Split(table.val, []byte{','})
	for _, rowLine := range rowList {
		if line, err := strconv.ParseInt(string(rowLine), 36, 64); err == nil {
			table.db.file.Seek(line * int64(table.db.bitSize), io.SeekStart)
			if row, err := table.db.getDataObj(':', keyB, []byte{0}, true); err == nil {
				newRow := &Row{
					table: table,
					Key: string(row.key),
					Value: string(row.val),
					line: row.line,
				}

				//todo: add row to table cache

				return newRow, nil
			}
		}
	}

	return &Row{table: table}, io.EOF
}

// FindRows allows you to do a more complex search for a list of rows
//
// for security, you can authorize a regex search by setting the first byte to 0 (for both the key and the value)
//
// if you include nothing else, or a '*' as the second byte, the key/value will match all rows
//
// anything else with a 0 as first byte will run an RE2 regex match
//
// if you are dealing with user input, it is recommended to sanitize it and remove the first byte of 0,
// to ensure the input cannot run regex, and will be treated as a literal string
func (table *Table) FindRows(key []byte, value []byte) ([]*Row, error) {
	resRow := []*Row{}

	table.db.mu.Lock()
	defer table.db.mu.Unlock()

	rowList := bytes.Split(table.val, []byte{','})
	for _, rowLine := range rowList {
		if line, err := strconv.ParseInt(string(rowLine), 36, 64); err == nil {
			table.db.file.Seek(line * int64(table.db.bitSize), io.SeekStart)
			if row, err := table.db.getDataObj(':', key, value, true); err == nil {
				newRow := &Row{
					table: table,
					Key: string(row.key),
					Value: string(row.val),
					line: row.line,
				}

				//todo: add row to table cache

				resRow = append(resRow, newRow)
			}
		}
	}

	if len(resRow) == 0 {
		return resRow, io.EOF
	}

	return resRow, nil
}

// DelRow removes the key value pair from the table
func (row *Row) Del() error {
	row.table.db.mu.Lock()
	defer row.table.db.mu.Unlock()

	row.table.db.file.Seek(row.line * int64(row.table.db.bitSize), io.SeekStart)
	_, err := row.table.db.rmDataObj(':')
	
	row.line = -1

	return err
}

// Rename changes the key of the row
func (row *Row) Rename(key string) error {
	keyB := goutil.Clean.Bytes([]byte(key))
	keyB = bytes.TrimLeftFunc(keyB, func(r rune) bool {
		return r == 0
	})

	row.table.db.mu.Lock()
	defer row.table.db.mu.Unlock()

	valB := goutil.Clean.Bytes([]byte(row.Value))
	valB = bytes.TrimLeftFunc(valB, func(r rune) bool {
		return r == 0
	})

	row.table.db.file.Seek(row.line * int64(row.table.db.bitSize), io.SeekStart)
	rw, err := row.table.db.setDataObj('$', keyB, valB)
	if err != nil {
		return err
	}

	row.Key = string(rw.key)
	row.Value = string(rw.val)

	//todo: add row to table cache

	return nil
}

// SetValue changes the value of the row
func (row *Row) SetValue(value string) error {
	valB := goutil.Clean.Bytes([]byte(value))
	valB = bytes.TrimLeftFunc(valB, func(r rune) bool {
		return r == 0
	})

	row.table.db.mu.Lock()
	defer row.table.db.mu.Unlock()

	keyB := goutil.Clean.Bytes([]byte(row.Key))
	keyB = bytes.TrimLeftFunc(keyB, func(r rune) bool {
		return r == 0
	})

	row.table.db.file.Seek(row.line * int64(row.table.db.bitSize), io.SeekStart)
	rw, err := row.table.db.setDataObj('$', keyB, valB)
	if err != nil {
		return err
	}

	row.Key = string(rw.key)
	row.Value = string(rw.val)

	//todo: add row to table cache

	return nil
}
