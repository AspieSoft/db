# DataBase

A simple database that anyone can build off of.

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
