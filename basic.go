package db

import (
	"bytes"
	"errors"
	"io"
	"os"
	"strconv"

	"github.com/AspieSoft/goutil/v7"
)

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

type Data struct {
	db *Database
	Key string
	Value string
	line int64
}


// Optimize will optimize a database file by cloning the tables and their rows to a new file
//
// this method will remove any orphaned data (rows without a table, etc),
// and will move existing tables to the top of the database file for quicker access
//
// row indexes are referenced from the tables, so having tables at the top is best for performance
func (db *Database) Optimize() error {
	return errors.New("Database optimization is temporary unavailable! This method is not yet compatible with the current algorithm!")

	db.mu.Lock()
	defer db.mu.Unlock()

	db.file.Sync()

	file, err := os.OpenFile(db.path+".opt", os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0755)
	if err != nil {
		return err
	}
	defer file.Close()

	newDB := &Database{
		file: file,
		path: db.path+".opt",
		bitSize: db.bitSize,
		prefixList: db.prefixList,
		encKey: db.encKey,
		cache: db.cache,
	}

	//todo: fix database to include default #data (#bit, #enc, etc.)

	if dataList, err := db.FindData([]byte{0}, []byte{0}, true); err == nil {
		for _, data := range dataList {
			newDB.AddData(data.Key, data.Value)
		}
	}

	if tableList, err := db.FindTables([]byte{0}, true); err == nil {
		newTables := make([]*Table, len(tableList))
		for i, table := range tableList {
			if tb, err := newDB.AddTable(table.Name); err == nil {
				newTables[i] = tb
			}
		}

		for i, table := range tableList {
			if tb := newTables[i]; tb != nil {
				if rowList, err := table.FindRows([]byte{0}, []byte{0}, true); err == nil {
					for _, row := range rowList {
						tb.AddRow(row.Key, row.Value)
					}
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
		return err
	}

	return nil
}


// AddData adds a new key value pair to the database
//
// this method returns the new data
func (db *Database) AddData(key string, value string, noLock ...bool) (*Data, error) {
	keyB := goutil.Clean.Bytes([]byte(key))
	keyB = bytes.TrimLeftFunc(keyB, func(r rune) bool {
		return r == 0
	})

	valB := goutil.Clean.Bytes([]byte(value))
	valB = bytes.TrimLeftFunc(valB, func(r rune) bool {
		return r == 0
	})

	if len(noLock) == 0 || noLock[0] == false {
		db.mu.Lock()
		defer db.mu.Unlock()
	}

	// ensure data does not already exist
	db.file.Seek(0, io.SeekStart)
	if data, err := getDataObj(db, '~', keyB, []byte{0}); err == nil {
		return &Data{
			db: db,
			Key: string(data.key),
			Value: string(data.val),
			line: data.line,
		}, errors.New("data key already exists")
	}

	data, err := addDataObj(db, '~', keyB, []byte{})
	if err != nil {
		return &Data{db: db}, err
	}

	newData := &Data{
		db: db,
		Key: string(data.key),
		Value: string(data.val),
		line: data.line,
	}

	//todo: add data to cache

	return newData, nil
}

// GetData retrieves an existing key value pair from the database
func (db *Database) GetData(key string, noLock ...bool) (*Data, error) {
	keyB := goutil.Clean.Bytes([]byte(key))
	keyB = bytes.TrimLeftFunc(keyB, func(r rune) bool {
		return r == 0
	})

	if len(noLock) == 0 || noLock[0] == false {
		db.mu.Lock()
		defer db.mu.Unlock()
	}

	//todo: get table from cache

	db.file.Seek(0, io.SeekStart)
	data, err := getDataObj(db, '~', keyB, []byte{0})
	if err != nil {
		return &Data{db: db}, err
	}

	newData := &Data{
		db: db,
		Key: string(data.key),
		Value: string(data.val),
		line: data.line,
	}

	//todo: add table to cache

	return newData, nil
}

// FindData allows you to do a more complex search for a list of key value pairs
//
// for security, you can authorize a regex search by setting the first byte of the name to 0
//
// if you include nothing else, or a '*' as the second byte, the name will match all tables
//
// anything else with a 0 as first byte will run an RE2 regex match
//
// if you are dealing with user input, it is recommended to sanitize it and remove the first byte of 0,
// to ensure the input cannot run regex, and will be treated as a literal string
func (db *Database) FindData(key []byte, value []byte, noLock ...bool) ([]*Data, error) {
	resData := []*Data{}

	if len(noLock) == 0 || noLock[0] == false {
		db.mu.Lock()
		defer db.mu.Unlock()
	}

	db.file.Seek(0, io.SeekStart)
	for {
		data, err := getDataObj(db, '~', key, value)
		if err != nil {
			break
		}

		newData := &Data{
			db: db,
			Key: string(data.key),
			Value: string(data.val),
			line: data.line,
		}
	
		//todo: add table to cache

		resData = append(resData, newData)
	}

	if len(resData) == 0 {
		return resData, io.EOF
	}

	return resData, nil
}

// Del removes the key value pair from the database
func (data *Data) Del(noLock ...bool) error {
	if len(noLock) == 0 || noLock[0] == false {
		data.db.mu.Lock()
		defer data.db.mu.Unlock()
	}
	
	data.db.file.Seek(data.line * int64(data.db.bitSize), io.SeekStart)
	_, err := delDataObj(data.db, '~')

	data.line = -1

	return err
}

// SetValue changes the value of the row
func (data *Data) SetValue(value string, noLock ...bool) error {
	valB := goutil.Clean.Bytes([]byte(value))
	valB = bytes.TrimLeftFunc(valB, func(r rune) bool {
		return r == 0
	})

	if len(noLock) == 0 || noLock[0] == false {
		data.db.mu.Lock()
		defer data.db.mu.Unlock()
	}

	keyB := goutil.Clean.Bytes([]byte(data.Key))
	keyB = bytes.TrimLeftFunc(keyB, func(r rune) bool {
		return r == 0
	})

	data.db.file.Seek(data.line * int64(data.db.bitSize), io.SeekStart)
	dt, err := setDataObj(data.db, '~', keyB, valB)
	if err != nil {
		return err
	}

	data.Key = string(dt.key)
	data.Value = string(dt.val)

	//todo: add row to table cache

	return nil
}


// AddTable adds a new table to the database
//
// this method returns the new table
func (db *Database) AddTable(name string, noLock ...bool) (*Table, error) {
	keyB := goutil.Clean.Bytes([]byte(name))
	keyB = bytes.TrimLeftFunc(keyB, func(r rune) bool {
		return r == 0
	})

	if len(noLock) == 0 || noLock[0] == false {
		db.mu.Lock()
		defer db.mu.Unlock()
	}

	// ensure table does not already exist
	db.file.Seek(0, io.SeekStart)
	if table, err := getDataObj(db, '$', keyB, []byte{0}); err == nil {
		return &Table{
			db: db,
			Name: string(table.key),
			key: table.key,
			val: table.val,
			line: table.line,
		}, errors.New("table already exists")
	}

	table, err := addDataObj(db, '$', keyB, []byte{})
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
func (db *Database) GetTable(name string, noLock ...bool) (*Table, error) {
	keyB := goutil.Clean.Bytes([]byte(name))
	keyB = bytes.TrimLeftFunc(keyB, func(r rune) bool {
		return r == 0
	})

	if len(noLock) == 0 || noLock[0] == false {
		db.mu.Lock()
		defer db.mu.Unlock()
	}

	//todo: get table from cache

	db.file.Seek(0, io.SeekStart)
	table, err := getDataObj(db, '$', keyB, []byte{0})
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
func (db *Database) FindTables(name []byte, noLock ...bool) ([]*Table, error) {
	resTables := []*Table{}

	if len(noLock) == 0 || noLock[0] == false {
		db.mu.Lock()
		defer db.mu.Unlock()
	}

	db.file.Seek(0, io.SeekStart)
	for {
		table, err := getDataObj(db, '$', name, []byte{0})
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
func (table *Table) Del(noLock ...bool) error {
	if len(noLock) == 0 || noLock[0] == false {
		table.db.mu.Lock()
		defer table.db.mu.Unlock()
	}
	
	table.db.file.Seek(table.line * int64(table.db.bitSize), io.SeekStart)
	_, err := delDataObj(table.db, '$')

	rowList := bytes.Split(table.val, []byte{','})
	for _, rowLine := range rowList {
		if line, err := strconv.ParseInt(string(rowLine), 36, 64); err == nil {
			table.db.file.Seek(line * int64(table.db.bitSize), io.SeekStart)
			delDataObj(table.db, ':')
		}
	}

	table.line = -1

	return err
}

// Rename changes the name of the table
func (table *Table) Rename(name string, noLock ...bool) error {
	keyB := goutil.Clean.Bytes([]byte(name))
	keyB = bytes.TrimLeftFunc(keyB, func(r rune) bool {
		return r == 0
	})

	if len(noLock) == 0 || noLock[0] == false {
		table.db.mu.Lock()
		defer table.db.mu.Unlock()
	}

	table.db.file.Seek(table.line * int64(table.db.bitSize), io.SeekStart)
	tb, err := setDataObj(table.db, '$', keyB, table.val)
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
func (table *Table) AddRow(key string, value string, noLock ...bool) (*Row, error) {
	keyB := goutil.Clean.Bytes([]byte(key))
	keyB = bytes.TrimLeftFunc(keyB, func(r rune) bool {
		return r == 0
	})

	valB := goutil.Clean.Bytes([]byte(value))
	valB = bytes.TrimLeftFunc(valB, func(r rune) bool {
		return r == 0
	})

	if len(noLock) == 0 || noLock[0] == false {
		table.db.mu.Lock()
		defer table.db.mu.Unlock()
	}

	// ensure row does not already exist
	rowList := bytes.Split(table.val, []byte{','})
	for _, rowLine := range rowList {
		if line, err := strconv.ParseInt(string(rowLine), 36, 64); err == nil {
			table.db.file.Seek(line * int64(table.db.bitSize), io.SeekStart)
			if row, err := getDataObj(table.db, ':', keyB, []byte{0}, true); err == nil {
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

	row, err := addDataObj(table.db, ':', keyB, valB)
	if err != nil {
		return &Row{table: table}, err
	}

	table.db.file.Seek(table.line * int64(table.db.bitSize), io.SeekStart)
	if len(table.val) == 0 {
		table.val = []byte(strconv.FormatInt(row.line, 36))
	}else{
		table.val = append(table.val, append([]byte{','}, strconv.FormatInt(row.line, 36)...)...)
	}
	setDataObj(table.db, '$', table.key, table.val)

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
func (table *Table) GetRow(key string, noLock ...bool) (*Row, error) {
	keyB := goutil.Clean.Bytes([]byte(key))
	keyB = bytes.TrimLeftFunc(keyB, func(r rune) bool {
		return r == 0
	})

	if len(noLock) == 0 || noLock[0] == false {
		table.db.mu.Lock()
		defer table.db.mu.Unlock()
	}

	//todo: get row from table cache

	rowList := bytes.Split(table.val, []byte{','})
	for _, rowLine := range rowList {
		if line, err := strconv.ParseInt(string(rowLine), 36, 64); err == nil {
			table.db.file.Seek(line * int64(table.db.bitSize), io.SeekStart)
			if row, err := getDataObj(table.db, ':', keyB, []byte{0}, true); err == nil {
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
func (table *Table) FindRows(key []byte, value []byte, noLock ...bool) ([]*Row, error) {
	resRow := []*Row{}

	if len(noLock) == 0 || noLock[0] == false {
		table.db.mu.Lock()
		defer table.db.mu.Unlock()
	}

	rowList := bytes.Split(table.val, []byte{','})
	for _, rowLine := range rowList {
		if line, err := strconv.ParseInt(string(rowLine), 36, 64); err == nil {
			table.db.file.Seek(line * int64(table.db.bitSize), io.SeekStart)
			if row, err := getDataObj(table.db, ':', key, value, true); err == nil {
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
func (row *Row) Del(noLock ...bool) error {
	if len(noLock) == 0 || noLock[0] == false {
		row.table.db.mu.Lock()
		defer row.table.db.mu.Unlock()
	}

	row.table.db.file.Seek(row.line * int64(row.table.db.bitSize), io.SeekStart)
	_, err := delDataObj(row.table.db, ':')
	
	row.line = -1

	return err
}

// Rename changes the key of the row
func (row *Row) Rename(key string, noLock ...bool) error {
	keyB := goutil.Clean.Bytes([]byte(key))
	keyB = bytes.TrimLeftFunc(keyB, func(r rune) bool {
		return r == 0
	})

	if len(noLock) == 0 || noLock[0] == false {
		row.table.db.mu.Lock()
		defer row.table.db.mu.Unlock()
	}

	valB := goutil.Clean.Bytes([]byte(row.Value))
	valB = bytes.TrimLeftFunc(valB, func(r rune) bool {
		return r == 0
	})

	row.table.db.file.Seek(row.line * int64(row.table.db.bitSize), io.SeekStart)
	rw, err := setDataObj(row.table.db, ':', keyB, valB)
	if err != nil {
		return err
	}

	row.Key = string(rw.key)
	row.Value = string(rw.val)

	//todo: add row to table cache

	return nil
}

// SetValue changes the value of the row
func (row *Row) SetValue(value string, noLock ...bool) error {
	valB := goutil.Clean.Bytes([]byte(value))
	valB = bytes.TrimLeftFunc(valB, func(r rune) bool {
		return r == 0
	})

	if len(noLock) == 0 || noLock[0] == false {
		row.table.db.mu.Lock()
		defer row.table.db.mu.Unlock()
	}

	keyB := goutil.Clean.Bytes([]byte(row.Key))
	keyB = bytes.TrimLeftFunc(keyB, func(r rune) bool {
		return r == 0
	})

	row.table.db.file.Seek(row.line * int64(row.table.db.bitSize), io.SeekStart)
	rw, err := setDataObj(row.table.db, ':', keyB, valB)
	if err != nil {
		return err
	}

	row.Key = string(rw.key)
	row.Value = string(rw.val)

	//todo: add row to table cache

	return nil
}
