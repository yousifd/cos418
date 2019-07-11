package mapreduce

import (
	"os"
	"encoding/json"
	"sort"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// Read Output from map function file
	mapFileName := reduceName(jobName, nMap, reduceTaskNumber)
	mapFile, err := os.Open(mapFileName)
	checkError(err)

	kvs := make(map[string][]string)
	var keys []string
	decoder := json.NewDecoder(mapFile)
	for {
		var kv KeyValue
		err := decoder.Decode(&kv)

		kvs[kv.Key] = append(kvs[kv.Key], kv.Value)
		_, inMap := kvs[kv.Key]
		if !inMap {
			keys = append(keys, kv.Key)
		}

		if (err != nil) {
			break
		}
	}

	// debug("kvs: %v\n", kvs)

	// Sort KV pairs by Key
	sort.Strings(keys)

	// Do Reduce
	// Write output to file
	mergeFileName := mergeName(jobName, reduceTaskNumber)
	mergeFile, err := os.Open(mergeFileName)
	checkError(err)
	defer mergeFile.Close()

	enc := json.NewEncoder(mergeFile)
	for _, key := range keys {
		enc.Encode(KeyValue{key, reduceF(key, kvs[key])})
	}

	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Use checkError to handle errors.
}
