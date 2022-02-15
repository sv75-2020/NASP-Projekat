package main

import (
	"bufio"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	_ "github.com/spaolacci/murmur3"
	"log"
	"os"
)

var output = []string{}

type MerkleRoot struct {
	root  *Node
	nodes []*Node
}

func (mr *MerkleRoot) String() string {
	return mr.root.String()
}

type Node struct {
	data  [20]byte //hash vrednost koju svaki element u stablu cuva
	left  *Node
	right *Node
}

func (n *Node) String() string {
	return hex.EncodeToString(n.data[:])
}

func Hash(data []byte) [20]byte {
	return sha1.Sum(data)
}

func recursion(elements []Node) []Node {
	if len(elements)%2 != 0 {
		elements = append(elements, Node{left: nil, right: nil, data: [20]byte{}})
	}
	var nodes []Node
	k := 2
	for i := 0; k-1 <= len(elements); i += 2 {
		pair := elements[i:k]
		left := pair[0]
		right := pair[1]
		data := Hash(append(left.data[:], right.data[:]...))
		nodes = append(nodes, Node{
			left:  &left,
			right: &right,
			data:  data,
		})
		k += 2

	}
	if len(nodes) == 1 {
		return nodes
	} else {
		return recursion(nodes)
	}
}

func MerkleTree(elements []Node) *MerkleRoot {
	root := recursion(elements)
	return &MerkleRoot{root: &root[0]}

}

func inorder(n *Node) {
	if n == nil {
		return
	}

	inorder(n.left)
	output = append(output, n.String())
	fmt.Println(n.String())
	inorder(n.right)

}

func WriteToFile(n *Node, fileName string) {
	inorder(n)
	file, err := os.OpenFile(fileName, os.O_WRONLY, 0666)
	if err != nil {
		log.Fatal(err)
	}
	datawriter := bufio.NewWriter(file)
	for _, data := range output {
		_, _ = datawriter.WriteString(data + "\n")
	}

	datawriter.Flush()
	file.Close()
}

func main() {
	nodes := []Node{
		Node{left: nil, right: nil, data: Hash([]byte("ponedeljak"))},
		Node{left: nil, right: nil, data: Hash([]byte("utorak"))},
		Node{left: nil, right: nil, data: Hash([]byte("sreda"))},
		Node{left: nil, right: nil, data: Hash([]byte("cetvrtak"))},
	}

	root := MerkleTree(nodes)
	WriteToFile(root.root, "Metadata.txt")
	fmt.Println("Root: ", root)

}
