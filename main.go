package main

import (
	"bufio"
	"fmt"
	"os"
)

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Println("1)Unos\n2)Traženje\n3)Brisanje")
		fmt.Print("Izaberite radnju: ")
		scanner.Scan()
		if scanner.Text() == "1" {
			fmt.Println("Unos podatka.")
			fmt.Println("Unesite ključ: ")
			scanner.Scan()
			k := scanner.Text()
			fmt.Println(k)
			fmt.Println("Unesite vrednost:")
			scanner.Scan()
			v := scanner.Text()
			fmt.Println(v)
		} else if scanner.Text() == "2" {
			fmt.Println("Traženje podatka.")
			fmt.Println("Unesite ključ: ")
			scanner.Scan()
			k := scanner.Text()
			fmt.Println(k)
			fmt.Println("Unesite vrednost:")
			scanner.Scan()
			v := scanner.Text()
			fmt.Println(v)

		} else if scanner.Text() == "3" {
			fmt.Println("Brisanje podatka.")
			fmt.Println("Unesite ključ: ")
			scanner.Scan()
			k := scanner.Text()
			fmt.Println(k)
			fmt.Println("Unesite vrednost:")
			scanner.Scan()
			v := scanner.Text()
			fmt.Println(v)

		} else {
			fmt.Println("Unesite validnu radnju.")
		}

		if scanner.Err() != nil {
			fmt.Println("Error: ", scanner.Err())
		}
	}
}
