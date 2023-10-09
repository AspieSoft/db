package db

import (
	"fmt"
	"io"
	"testing"
)

func Test(t *testing.T){
	DebugMode = true

	db, err := Open("test/test.db", 16)
	if err != nil {
		panic(err)
	}

	_ = fmt.Print
	db.file.Truncate(0)


	db.AddTable("MyTable")
	table2, err := db.AddTable("MyTable2")
	table, err := db.GetTable("MyTable")
	table.AddRow("Row1", "val1")
	table.AddRow("Row2", "val2")
	table.GetRow("Row1")
	table2.Del()

	// db.DelTable("MyTable2")

	db.Optimize()
}

func TestCore(t *testing.T){
	DebugMode = true

	db, err := Open("test/core.db", 16)
	if err != nil {
		panic(err)
	}

	_ = fmt.Print
	db.file.Truncate(0)


	addDataObj(db, '$', []byte("MyTable_MoreTextToMakeThisLonger"), []byte("test"))

	db.file.Seek(0, io.SeekStart)
	getDataObj(db, '$', []byte("MyTable_MoreTextToMakeThisLonger"), []byte{0})

	db.file.Seek(0, io.SeekStart)
	setDataObj(db, '$', []byte("MyTable"), []byte("MyVal"))
	// setDataObj(db, '$', []byte("MyTable"), []byte("MyVal_MoreTextToMakeThisLonger"))
	// setDataObj(db, '$', []byte("MyTable"), []byte("MyVal_MoreTextToMakeThisLonger_MoreTextToMakeThisLonger"))
}
