package SSTable

import (
	"bufio"
	"crypto/sha1"
	"encoding/hex"
	_ "github.com/spaolacci/murmur3"
	"log"
	"os"
)

var output = []string{}

type MerkleRoot struct {
	Root  *Node
	nodes []*Node
}

func (mr *MerkleRoot) String() string {
	return mr.Root.String()
}

type Node struct {
	Data  [20]byte //hash vrednost koju svaki element u stablu cuva
	Left  *Node
	Right *Node
}

func (n *Node) String() string {
	return hex.EncodeToString(n.Data[:])
}

func Hash(data []byte) [20]byte {
	return sha1.Sum(data)
}

func recursion(elements []Node) []Node {
	if len(elements)%2 != 0 {
		elements = append(elements, Node{Left: nil, Right: nil, Data: [20]byte{}})
	}
	k := 2
	var nodess []Node
	for i := 0; k-1 <= len(elements); i += 2 {
		pair := elements[i:k]
		left := pair[0]
		right := pair[1]
		data := Hash(append(left.Data[:], right.Data[:]...))
		nodess = append(nodess, Node{
			Left:  &left,
			Right: &right,
			Data:  data,
		})
		k += 2

	}
	if len(nodess) == 1 {
		return nodess
	} else {
		return recursion(nodess)
	}
}

func MerkleTree(elements []Node) *MerkleRoot {
	if len(elements) == 0 {
		return nil
	}
	root := recursion(elements)
	return &MerkleRoot{Root: &root[0]}

}

func inorder(n *Node) {
	if n == nil {
		return
	}

	inorder(n.Left)
	output = append(output, n.String())
	inorder(n.Right)

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
	output = nil
	datawriter.Flush()
	file.Close()
}

/*func main() {
	nodes := []Node{
		Node{left: nil, right: nil, data: Hash([]byte("ponedeljak"))},
		Node{left: nil, right: nil, data: Hash([]byte("utorak"))},
		Node{left: nil, right: nil, data: Hash([]byte("sreda"))},
		Node{left: nil, right: nil, data: Hash([]byte("cetvrtak"))},
	}

	root := MerkleTree(nodes)
	WriteToFile(root.root, "Metadata.txt")
	fmt.Println("Root: ", root)

}*/
