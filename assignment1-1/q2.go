package cos418_hw1_1

import (
	"bufio"
	"io"
	"strconv"
	"os"
)

// Sum numbers from channel `nums` and output sum to `out`.
// You should only output to `out` once.
// Do NOT modify function signature.
func sumWorker(nums chan int, out chan int) {
	// HINT: use for loop over `nums`
	total := 0
	for num := range nums {
		total += num
	}

	out <- total
}

// Read integers from the file `fileName` and return sum of all values.
// This function must launch `num` go routines running
// `sumWorker` to find the sum of the values concurrently.
// You should use `checkError` to handle potential errors.
// Do NOT modify function signature.
func sum(num int, fileName string) int {
	// HINT: use `readInts` and `sumWorkers`
	// HINT: used buffered channels for splitting numbers between workers
	file, err := os.Open(fileName)
	checkError(err)

	ints, err := readInts(file)
	checkError(err)
	size := len(ints)
	span := size / num

	pos := 0
	out := make(chan int)
	nums := make(chan int)
	for count := 0; count < num; count++ {
		go sumWorker(nums, out)
		for idx := pos; idx < pos+span; idx++ {
			nums <- ints[idx]
		}
		pos += span
	}
	close(nums)

	sum := 0
	iter := 0
	for value := range out {
		sum += value
		iter++
		if iter == num {
			break
		}
	}

	return sum
}

// Read a list of integers separated by whitespace from `r`.
// Return the integers successfully read with no error, or
// an empty slice of integers and the error that occurred.
// Do NOT modify this function.
func readInts(r io.Reader) ([]int, error) {
	scanner := bufio.NewScanner(r)
	scanner.Split(bufio.ScanWords)
	var elems []int
	for scanner.Scan() {
		val, err := strconv.Atoi(scanner.Text())
		if err != nil {
			return elems, err
		}
		elems = append(elems, val)
	}
	return elems, nil
}
