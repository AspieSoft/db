package db

import (
	"fmt"
	"io"
	"testing"
)

func Test(t *testing.T){
	DebugMode = true

	db, err := Open("test/test.db", []byte("key123"), 16)
	if err != nil {
		t.Error(err)
	}

	_ = fmt.Print
	db.file.Truncate(0)


	_, err = db.AddTable("MyTable")
	if err != nil {
		t.Error(err)
	}

	table2, err := db.AddTable("MyTable2")
	if err != nil {
		t.Error(err)
	}

	table, err := db.GetTable("MyTable")
	if err != nil {
		t.Error(err)
	}

	_, err = table.AddRow("Row1", "val1")
	if err != nil {
		t.Error(err)
	}

	_, err = table.AddRow("Row2", "val2")
	if err != nil {
		t.Error(err)
	}

	_, err = table.GetRow("Row1")
	if err != nil {
		t.Error(err)
	}

	err = table2.Del()
	if err != nil {
		t.Error(err)
	}

	err = db.Optimize()
	if err != nil {
		t.Error(err)
	}
}

func TestCore(t *testing.T){
	DebugMode = true

	db, err := Open("test/core.db", nil, 16)
	if err != nil {
		t.Error(err)
	}

	_ = fmt.Print
	db.file.Truncate(0)


	_, err = addDataObj(db, '$', []byte("MyTable_MoreTextToMakeThisLonger"), []byte("test"))
	if err != nil {
		t.Error(err)
	}

	db.file.Seek(0, io.SeekStart)
	_, err = getDataObj(db, '$', []byte("MyTable_MoreTextToMakeThisLonger"), []byte{0})
	if err != nil {
		t.Error(err)
	}

	db.file.Seek(0, io.SeekStart)
	_, err = setDataObj(db, '$', []byte("MyTable"), []byte("MyVal"))
	if err != nil {
		t.Error(err)
	}

	// setDataObj(db, '$', []byte("MyTable"), []byte("MyVal_MoreTextToMakeThisLonger"))
	// setDataObj(db, '$', []byte("MyTable"), []byte("MyVal_MoreTextToMakeThisLonger_MoreTextToMakeThisLonger"))
}
