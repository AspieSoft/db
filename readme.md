# DataBase

A simple database that anyone can build off of.

## Notice: This Module Is Currently In Beta

> Note: If you have any name suggestions, please share.

## Installation

```shell script
go get https://github.com/AspieSoft/db
```

## Usage

```go

import "github.com/AspieSoft/db"

func main(){
  myDB, err := db.Open("path/to/file.db")
  defer myDB.Close()

  myDB.AddTable("MyTable")
  myTable, err := myDB.GetTable("MyTable")
  myTable.AddRow("Row1", "val1")
  myTable.AddRow("Row2", "val2")
  row1, err := myTable.GetRow("Row1")

  fmt.Println("key": row1.Key)
  fmt.Println("value": row1.Value)

  myDB.Optimize()
}

```

## Custom Database

```go

import db "github.com/AspieSoft/db/custom"

func main(){
  myDB, err := db.Open("path/to/file.db", 1024, []byte{'$', ':'})
  defer myDB.Close()

  // a custom database is similar, but it does not come with any data structure, such as tables and rows
  // instead, you can build these yourself using some of the core methods for reading and writing to the database

  db.AddDataObj(myDB, '$', []byte("MyTable"), []byte{})

  db.File.Seek(0, io.SeekStart)
  myTable, err := db.GetDataObj(myDB, '$', []byte("MyTable"), []byte{0}) // a prefix of 0 allows a regex match, or blank to match all

  db.File.Seek(myTable.Line * int64(myDB.BitSize), io.SeekStart)
  db.SetDataObj(myDB, '$', []byte("MyTable"), []byte("MyRowList"))

  db.File.Seek(myTable.Line * int64(myDB.BitSize), io.SeekStart)
  db.DelDataObj(myDB, '$')

  //note: to have tabes map to rows, you will need to handle this manually
  //recommended: create your own module as a wrapper for your custom database using this module
}

```
