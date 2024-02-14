package main

import "fmt"

func main() {
	// initialize db
	dal, err := NewDal("db.db")

	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}

	// create a new page
	p := dal.AllocateEmptyPage()
	// fmt.Println(dal)
	p.num = dal.getNextPage()
	copy(p.data[:], "data")

	// commit it
	_ = dal.WritePage(p)
}
