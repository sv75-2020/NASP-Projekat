package Config

import (
	"bufio"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	maxTokens    int   //maksimalan broj tokena u Token Bucketu
	maxTime      int64 //vreme za obradjivanje zahteva u Token Bucketu
	sizeSegments int   //velicina segmenta u WAL-u
	sizeMem      int   //velicina memtable
	numLevel     int   //LSM- dopušten broj nivoa
	numElems     int   //Cache - broj elemenata strukture

}

func (c *Config) GetMaxTokens() int {
	return c.maxTokens

}
func (c *Config) GetMaxTime() int64 {
	return c.maxTime

}
func (c *Config) GetSizeSegments() int {
	return c.sizeSegments

}
func (c *Config) GetSizeMem() int {
	return c.sizeMem

}
func (c *Config) GetNumLevel() int {
	return c.numLevel

}
func (c *Config) GetNumElems() int {
	return c.numElems

}

func (c *Config) ReadConfigFile() {
	file, _ := os.OpenFile("Config/configFile.txt", os.O_RDWR, 0777)
	scanner := bufio.NewScanner(file)

	scanner.Scan()
	s := strings.Split(scanner.Text(), ":")
	if s[1] != "0" {
		c.maxTokens, _ = strconv.Atoi(s[1])
	} else {
		c.maxTokens = 5
	}

	scanner.Scan()
	s1 := strings.Split(scanner.Text(), ":")
	if s1[1] != "0" {
		c.maxTime, _ = strconv.ParseInt(s1[1], 10, 64)
	} else {
		c.maxTime = 60
	}

	scanner.Scan()
	s2 := strings.Split(scanner.Text(), ":")
	if s2[1] != "0" {
		c.sizeSegments, _ = strconv.Atoi(s2[1])
	} else {
		c.sizeSegments = 300
	}

	scanner.Scan()
	s3 := strings.Split(scanner.Text(), ":")
	if s3[1] != "0" {
		c.sizeMem, _ = strconv.Atoi(s3[1])
	} else {
		c.sizeMem = 170
	}

	scanner.Scan()
	s4 := strings.Split(scanner.Text(), ":")
	if s4[1] != "0" {
		c.numLevel, _ = strconv.Atoi(s4[1])
	} else {
		c.numLevel = 3
	}

	scanner.Scan()
	s5 := strings.Split(scanner.Text(), ":")
	if s5[1] != "0" {
		c.numElems, _ = strconv.Atoi(s5[1])
	} else {
		c.numElems = 5
	}

}
