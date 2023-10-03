package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/AspieSoft/go-regex-re2/v2"
	"github.com/AspieSoft/goutil/v7"
)

const debugMode = true

type Database struct {
	file *os.File
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

const dataObjSize uint16 = 16

func main(){
	file, err := os.OpenFile("test.db", os.O_CREATE|os.O_RDWR, 0755)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	db := Database{
		file: file,
	}

	if debugMode {
		_ = fmt.Print
		db.file.Truncate(0)
	}

	/* db.AddTable("MyTable")
	table, err := db.GetTable("MyTable")
	table.AddRow("Row1", "val1")
	table.AddRow("Row2", "val2")
	table.GetRow("Row1") */
	// db.RemoveTable("MyTable")

	//todo: use this to test the setDataObj method
	db.addDataObj('$', []byte("MyTable_MoreTextToMakeThisLonger"), []byte("test"))

	db.file.Seek(0, io.SeekStart)
	db.getDataObj('$', []byte("MyTable_MoreTextToMakeThisLonger"), []byte{0})

	db.file.Seek(0, io.SeekStart)
	// db.setDataObj('$', []byte("MyTable"), []byte("MyVal"))
	// db.setDataObj('$', []byte("MyTable"), []byte("MyVal_MoreTextToMakeThisLonger"))
	db.setDataObj('$', []byte("MyTable"), []byte("MyVal_MoreTextToMakeThisLonger_MoreTextToMakeThisLonger"))
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
// also ensure valyes do not include special chars from database syntax [%$:=,@-!]

func (db *Database) addDataObj(prefix byte, key []byte, val []byte) (dbObj, error) {
	pos, _ := db.file.Seek(0, io.SeekStart)

	if off := pos % int64(dataObjSize); off != 0 {
		db.file.Write(bytes.Repeat([]byte{'-'}, int(off)))
		pos += off
	}

	buf := make([]byte, 1)
	_, err := db.file.Read(buf)
	for err == nil && buf[0] != '!' {
		pos, _ = db.file.Seek(int64(dataObjSize)-1, io.SeekCurrent)
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
		line: pos / int64(dataObjSize),
	}

	val = regex.JoinBytes(key, '=', val)

	posLine := pos / int64(dataObjSize)

	// add data
	db.file.Write([]byte{prefix})

	off := 1
	if debugMode {
		off++
	}

	for len(val) + off > int(dataObjSize) {
		var posStr []byte
		var useNewPos int64 = -1

		if !addNew {
			curPos, _ := db.file.Seek(0, io.SeekCurrent)
			db.file.Seek(int64(dataObjSize)-1, io.SeekCurrent)

			_, err = db.file.Read(buf)
			for err == nil && buf[0] != '!' {
				db.file.Seek(int64(dataObjSize)-1, io.SeekCurrent)
				_, err = db.file.Read(buf)
			}

			if err == io.EOF {
				addNew = true
				newPos, _ := db.file.Seek(0, io.SeekEnd)
				useNewPos = newPos
				newPos /= int64(dataObjSize)
				posStr = []byte(strconv.FormatInt(newPos, 36))
				posLine = newPos
			}else{
				newPos, _ := db.file.Seek(-1, io.SeekCurrent)
				useNewPos = newPos
				newPos /= int64(dataObjSize)
				posStr = []byte(strconv.FormatInt(newPos, 36))
			}

			db.file.Seek(curPos, io.SeekStart)
		}else if addNew {
			posLine++
			posStr = []byte(strconv.FormatInt(posLine, 36))
		}

		posStr = append([]byte{'@'}, posStr...)
		offset := int(dataObjSize) - len(posStr) - 1

		if debugMode {
			offset--
		}

		db.file.Write(val[:offset])
		db.file.Write(posStr)
		val = val[offset:]

		if debugMode {
			db.file.Write([]byte{'\n'})
		}

		if useNewPos != -1 {
			db.file.Seek(useNewPos, io.SeekStart)
		}

		db.file.Write([]byte{'&'})
	}

	db.file.Write(val)
	if len(val) < int(dataObjSize) {
		if debugMode {
			db.file.Write(bytes.Repeat([]byte{'-'}, int(dataObjSize) - len(val) - 2))
			db.file.Write([]byte{'\n'})
		}else{
			db.file.Write(bytes.Repeat([]byte{'-'}, int(dataObjSize) - len(val) - 1))
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

	if off := pos % int64(dataObjSize); off != 0 {
		db.file.Write(bytes.Repeat([]byte{'-'}, int(off)))
		pos += off
	}

	buf := make([]byte, 1)
	_, err = db.file.Read(buf)

	for err == nil /* && buf[0] != prefix */ {
		if buf[0] == prefix {
			buf = make([]byte, int64(dataObjSize)-1)
			_, err = db.file.Read(buf)

			buf = bytes.TrimRight(buf, "-\n")
			reInd := regex.Comp(`[\-\n]*@([a-z0-9]+)$`)
			for reInd.Match(buf) {
				buf = reInd.RepFunc(buf, func(data func(int) []byte) []byte {
					if getPos, err := strconv.ParseInt(string(data(1)), 36, 64); err == nil {
						db.file.Seek(getPos*int64(dataObjSize), io.SeekStart)
						b := make([]byte, int64(dataObjSize))
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
				db.file.Seek(pos + int64(dataObjSize), io.SeekStart)
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
					db.file.Seek(pos + int64(dataObjSize), io.SeekStart)

					return dbObj{
						key: data[0],
						val: data[1],
						line: pos / int64(dataObjSize),
					}, nil
				}
			}

			if stopFirstRow {
				return dbObj{}, io.EOF
			}

			db.file.Seek(pos + int64(dataObjSize), io.SeekStart)
			buf = make([]byte, 1)
			_, err = db.file.Read(buf)
			continue
		}

		pos, _ = db.file.Seek(int64(dataObjSize)-1, io.SeekCurrent)
		_, err = db.file.Read(buf)
	}

	if err != nil {
		return dbObj{}, err
	}

	return dbObj{}, io.EOF
}

func (db *Database) rmDataObj(prefix byte) (dbObj, error) {
	pos, _ := db.file.Seek(0, io.SeekCurrent)

	if off := pos % int64(dataObjSize); off != 0 {
		db.file.Write(bytes.Repeat([]byte{'-'}, int(off)))
		pos += off
	}

	buf := make([]byte, 1)
	_, err := db.file.Read(buf)

	for err == nil && buf[0] != prefix {
		pos, _ = db.file.Seek(int64(dataObjSize)-1, io.SeekCurrent)
		_, err = db.file.Read(buf)
	}

	if err != nil {
		return dbObj{}, nil
	}

	buf = make([]byte, int64(dataObjSize)-1)
	_, err = db.file.Read(buf)

	db.file.Seek(int64(dataObjSize) * -1, io.SeekCurrent)
	db.file.Write([]byte{'!'})
	if debugMode {
		db.file.Write(bytes.Repeat([]byte{'-'}, int(dataObjSize)-2))
		db.file.Write([]byte{'\n'})
	}else{
		db.file.Write(bytes.Repeat([]byte{'-'}, int(dataObjSize)-1))
	}

	buf = bytes.TrimRight(buf, "-\n")
	reInd := regex.Comp(`[\-\n]*@([a-z0-9]+)$`)
	for reInd.Match(buf) {
		buf = reInd.RepFunc(buf, func(data func(int) []byte) []byte {
			if getPos, err := strconv.ParseInt(string(data(1)), 36, 64); err == nil {
				db.file.Seek(getPos*int64(dataObjSize), io.SeekStart)
				b := make([]byte, int64(dataObjSize))
				_, err = db.file.Read(b)
				if err == nil && b[0] == '&' {
					db.file.Seek(int64(dataObjSize) * -1, io.SeekCurrent)
					db.file.Write([]byte{'!'})
					if debugMode {
						db.file.Write(bytes.Repeat([]byte{'-'}, int(dataObjSize)-2))
						db.file.Write([]byte{'\n'})
					}else{
						db.file.Write(bytes.Repeat([]byte{'-'}, int(dataObjSize)-1))
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

	db.file.Seek(pos + int64(dataObjSize), io.SeekStart)

	return dbObj{
		key: data[0],
		val: data[1],
		line: pos / int64(dataObjSize),
	}, nil
}

//todo: add method to set data over existing object
func (db *Database) setDataObj(prefix byte, key []byte, val []byte) (dbObj, error) {
	pos, _ := db.file.Seek(0, io.SeekCurrent)

	if off := pos % int64(dataObjSize); off != 0 {
		db.file.Write(bytes.Repeat([]byte{'-'}, int(off)))
		pos += off
	}

	buf := make([]byte, 1)
	_, err := db.file.Read(buf)

	for err == nil && buf[0] != prefix {
		pos, _ = db.file.Seek(int64(dataObjSize)-1, io.SeekCurrent)
		_, err = db.file.Read(buf)
	}

	if err != nil {
		return dbObj{}, nil
	}


	// set data on object
	addNew := false
	_ = addNew

	obj := dbObj{
		key: key,
		val: val,
		line: pos / int64(dataObjSize),
	}

	val = regex.JoinBytes(key, '=', val)

	// set data
	off := 1
	if debugMode {
		off++
	}

	buf = make([]byte, int64(dataObjSize)-1)
	_, err = db.file.Read(buf)
	oldPos, _ := db.file.Seek(int64(dataObjSize) * -1, io.SeekCurrent)

	buf = bytes.TrimRight(buf, "-\n")
	reInd := regex.Comp(`[\-\n]*@([a-z0-9]+)$`)
	for err == nil && reInd.Match(buf) {
		buf = reInd.RepFunc(buf, func(data func(int) []byte) []byte {
			if getPos, err := strconv.ParseInt(string(data(1)), 36, 64); err == nil {
				newPos, err := db.file.Seek(getPos*int64(dataObjSize), io.SeekStart)
				b := make([]byte, int64(dataObjSize))
				_, err = db.file.Read(b)
				if err == nil && b[0] == '&' {
					if len(val) == 0 {
						db.file.Seek(oldPos, io.SeekStart)
						db.file.Write([]byte{'!'})
						if debugMode {
							db.file.Write(bytes.Repeat([]byte{'-'}, int(dataObjSize)-2))
							db.file.Write([]byte{'\n'})
						}else{
							db.file.Write(bytes.Repeat([]byte{'-'}, int(dataObjSize)-1))
						}
					}else{
						db.file.Seek(oldPos+1, io.SeekStart)

						if len(val) + off > int(dataObjSize) {
							posStr := []byte(strconv.FormatInt(newPos / int64(dataObjSize), 36))

							posStr = append([]byte{'@'}, posStr...)
							offset := int(dataObjSize) - len(posStr) - 1

							if debugMode {
								offset--
							}

							db.file.Write(val[:offset])
							db.file.Write(posStr)
							val = val[offset:]

							if debugMode {
								db.file.Write([]byte{'\n'})
							}
						}else{
							db.file.Write(val)
							if len(val) < int(dataObjSize) {
								if debugMode {
									db.file.Write(bytes.Repeat([]byte{'-'}, int(dataObjSize) - len(val) - 2))
									db.file.Write([]byte{'\n'})
								}else{
									db.file.Write(bytes.Repeat([]byte{'-'}, int(dataObjSize) - len(val) - 1))
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
	buf = make([]byte, int64(dataObjSize))
	_, err = db.file.Read(buf)
	if err == nil && buf[0] == '&' {
		if len(val) == 0 {
			db.file.Seek(oldPos, io.SeekStart)
			db.file.Write([]byte{'!'})
			if debugMode {
				db.file.Write(bytes.Repeat([]byte{'-'}, int(dataObjSize)-2))
				db.file.Write([]byte{'\n'})
			}else{
				db.file.Write(bytes.Repeat([]byte{'-'}, int(dataObjSize)-1))
			}
		}else{
			db.file.Seek(oldPos+1, io.SeekStart)
			posLine := oldPos / int64(dataObjSize)
			_ = posLine

			if len(val) + off > int(dataObjSize) {
				var posStr []byte
				var useNewPos int64 = -1

				curPos, _ := db.file.Seek(0, io.SeekCurrent)
				// db.file.Seek(int64(dataObjSize)-1, io.SeekCurrent)
				db.file.Seek(0, io.SeekStart)
				_ = curPos

				buf = make([]byte, 1)
				_, err = db.file.Read(buf)
				for err == nil && buf[0] != '!' {
					db.file.Seek(int64(dataObjSize)-1, io.SeekCurrent)
					_, err = db.file.Read(buf)
				}

				if err == io.EOF {
					newPos, _ := db.file.Seek(0, io.SeekEnd)
					useNewPos = newPos
					newPos /= int64(dataObjSize)
					posStr = []byte(strconv.FormatInt(newPos, 36))
					posLine = newPos
				}else{
					newPos, _ := db.file.Seek(-1, io.SeekCurrent)
					useNewPos = newPos
					newPos /= int64(dataObjSize)
					posStr = []byte(strconv.FormatInt(newPos, 36))
				}
	
				db.file.Seek(curPos, io.SeekStart)

				posStr = append([]byte{'@'}, posStr...)
				offset := int(dataObjSize) - len(posStr) - 1

				if debugMode {
					offset--
				}

				db.file.Write(val[:offset])
				db.file.Write(posStr)
				val = val[offset:]

				if debugMode {
					db.file.Write([]byte{'\n'})
				}

				if useNewPos != -1 {
					db.file.Seek(useNewPos, io.SeekStart)
				}

				db.file.Write([]byte{'&'})

				for len(val) + off > int(dataObjSize) {
					//todo: finish adding value
					// use the addDataObj method as a reference
					// this method will mainly just run the bulk of that method at this step in the process

					break
				}
			}else{
				db.file.Write(val)
				if len(val) < int(dataObjSize) {
					if debugMode {
						db.file.Write(bytes.Repeat([]byte{'-'}, int(dataObjSize) - len(val) - 2))
						db.file.Write([]byte{'\n'})
					}else{
						db.file.Write(bytes.Repeat([]byte{'-'}, int(dataObjSize) - len(val) - 1))
					}
				}
			}
		}
	}


	//todo: replace this less preformant method with a better one
	// note: this method needs to use the same starting point for the data object being changed,
	// to avoid the need to update table values, and to better optimize the database performance
	// db.rmDataObj(prefix)
	// db.addDataObj(prefix, key, val)

	return obj, nil
}


//todo: include a sync.Mutex for public database methods to prevent them from running at the same time

// AddTable adds a new table to the database
// this method returns the new table
func (db *Database) AddTable(name string) (*Table, error) {
	keyB := goutil.Clean.Bytes([]byte(name))
	keyB = bytes.TrimLeftFunc(keyB, func(r rune) bool {
		return r == 0
	})

	//todo: ensure table does not already exist

	table, err := db.addDataObj('$', keyB, []byte{})
	if err != nil {
		return &Table{db: db}, err
	}

	newTable := &Table{
		db: db,
		Name: string(table.key),
		key: table.key,
		val: []byte{},
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
		val: []byte{},
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
func (db *Database) FineTables(name []byte) ([]*Table, error) {
	resTables := []*Table{}

	db.file.Seek(0, io.SeekStart)
	for {
		table, err := db.getDataObj('$', name, []byte{0})
		if err == nil {
			break
		}

		newTable := &Table{
			db: db,
			Name: string(table.key),
			key: table.key,
			val: []byte{},
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

// DelTable removes a table from the database
//
// this method returns the old table
func (db *Database) DelTable(name string) (*Table, error) {
	keyB := goutil.Clean.Bytes([]byte(name))
	keyB = bytes.TrimLeftFunc(keyB, func(r rune) bool {
		return r == 0
	})

	//todo: get table from cache

	db.file.Seek(0, io.SeekStart)
	table, err := db.getDataObj('$', keyB, []byte{0})
	if err != nil {
		return &Table{db: db}, err
	}

	db.file.Seek(table.line * int64(dataObjSize), io.SeekStart)
	db.rmDataObj('$')

	rowList := bytes.Split(table.val, []byte{','})
	for _, rowLine := range rowList {
		if line, err := strconv.ParseInt(string(rowLine), 36, 64); err == nil {
			db.file.Seek(line * int64(dataObjSize), io.SeekStart)
			db.rmDataObj(':')
		}
	}

	//todo: remove table from cache

	return &Table{
		db: db,
		Name: string(table.key),
		line: -1,
	}, nil
}

// Rename changes the name of the table
//
// this method returns the updated table
func (table *Table) Rename(name string) (*Table, error) {
	keyB := goutil.Clean.Bytes([]byte(name))
	keyB = bytes.TrimLeftFunc(keyB, func(r rune) bool {
		return r == 0
	})

	table.db.file.Seek(table.line * int64(dataObjSize), io.SeekStart)
	tb, err := table.db.setDataObj('$', keyB, table.val)
	if err != nil {
		return table, err
	}

	table.Name = string(tb.key)
	table.key = tb.key
	table.val = tb.val

	//todo: add table to cache

	return table, nil
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

	//todo: ensure row does not already exist
	
	row, err := table.db.addDataObj(':', keyB, valB)
	if err != nil {
		return &Row{table: table}, err
	}

	table.db.file.Seek(table.line * int64(dataObjSize), io.SeekStart)
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

	//todo: get row from table cache

	rowList := bytes.Split(table.val, []byte{','})
	for _, rowLine := range rowList {
		if line, err := strconv.ParseInt(string(rowLine), 36, 64); err == nil {
			table.db.file.Seek(line * int64(dataObjSize), io.SeekStart)
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

	rowList := bytes.Split(table.val, []byte{','})
	for _, rowLine := range rowList {
		if line, err := strconv.ParseInt(string(rowLine), 36, 64); err == nil {
			table.db.file.Seek(line * int64(dataObjSize), io.SeekStart)
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

// DelRow removes a key value pair from the table
//
// this method returns the old row
func (table *Table) DelRow(key string) (*Row, error) {
	keyB := goutil.Clean.Bytes([]byte(key))
	keyB = bytes.TrimLeftFunc(keyB, func(r rune) bool {
		return r == 0
	})

	//todo: get row from cache

	rowList := bytes.Split(table.val, []byte{','})
	for _, rowLine := range rowList {
		if line, err := strconv.ParseInt(string(rowLine), 36, 64); err == nil {
			table.db.file.Seek(line * int64(dataObjSize), io.SeekStart)
			if row, err := table.db.getDataObj(':', keyB, []byte{0}, true); err == nil {
				table.db.file.Seek(row.line * int64(dataObjSize), io.SeekStart)
				table.db.rmDataObj(':')

				//todo: remove row from table cache

				return &Row{
					table: table,
					Key: string(row.key),
					Value: string(row.val),
					line: -1,
				}, nil
			}
		}
	}

	return &Row{table: table}, io.EOF
}

// Rename changes the key of the row
//
// this method returns the updated row
func (row *Row) Rename(key string) (*Row, error) {
	keyB := goutil.Clean.Bytes([]byte(key))
	keyB = bytes.TrimLeftFunc(keyB, func(r rune) bool {
		return r == 0
	})

	valB := goutil.Clean.Bytes([]byte(row.Value))
	valB = bytes.TrimLeftFunc(valB, func(r rune) bool {
		return r == 0
	})

	row.table.db.file.Seek(row.line * int64(dataObjSize), io.SeekStart)
	rw, err := row.table.db.setDataObj('$', keyB, valB)
	if err != nil {
		return row, err
	}

	row.Key = string(rw.key)
	row.Value = string(rw.val)

	//todo: add row to table cache

	return row, nil
}

// SetValue changes the value of the row
//
// this method returns the updated row
func (row *Row) SetValue(value string) (*Row, error) {
	keyB := goutil.Clean.Bytes([]byte(row.Key))
	keyB = bytes.TrimLeftFunc(keyB, func(r rune) bool {
		return r == 0
	})

	valB := goutil.Clean.Bytes([]byte(value))
	valB = bytes.TrimLeftFunc(valB, func(r rune) bool {
		return r == 0
	})

	row.table.db.file.Seek(row.line * int64(dataObjSize), io.SeekStart)
	rw, err := row.table.db.setDataObj('$', keyB, valB)
	if err != nil {
		return row, err
	}

	row.Key = string(rw.key)
	row.Value = string(rw.val)

	//todo: add row to table cache

	return row, nil
}
