package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		// call for a job
		reply := callForJob()
		if reply.File == "" {
			return
		}

		// do map or reduce job
		switch reply.JobType {
		case Map:
			mapWorker(&reply, mapf)
		case Reduce:
			reduceWorker(&reply, reducef)
		}
		time.Sleep(10 * time.Second)
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	fmt.Println(err)
	return false
}

func callForJob() RequestReply {
	// call for a job
	args := RequestArgs{
		ReqType: RequestForJob,
	}
	reply := RequestReply{}

	connected := call("Coordinator.RequestJob", &args, &reply)

	// terminate the worker if failing to connect the coordinator
	if !connected {
		return RequestReply{File: ""}
	}
	return reply
}

func callForReport(jobtp JobType, jobid int, filename string) (int, bool) {
	args := RequestArgs{
		ReqType: ReportFinish,
		Info: ReportInfo{
			JobTp:   jobtp,
			JobId:   jobid,
			JobName: filename,
		},
	}
	reply := 0
	// terminate the worker if failing to connect the coordinator
	connected := call("Coordinator.ReportFinish", &args, &reply)
	return reply, connected
}

func createTempFiles(jobid int, nReduce int) map[string]*os.File {
	mp := make(map[string]*os.File)
	// buckets := []string{}
	for i := 0; i < nReduce; i++ {
		intermediateName := "mr-" + strconv.Itoa(jobid) + "-" + strconv.Itoa(i)
		// buckets = append(buckets, intermediateName)
		tempFile, err := ioutil.TempFile("./", "intermediate-")
		if err != nil {
			log.Fatalf("Failed to create new temp file")
		}
		mp[intermediateName] = tempFile
	}
	return mp
}

func writeToJsonAndRename(jobid int, nReduce int, kva []KeyValue, mp map[string]*os.File) {
	for _, kv := range kva {
		intermediateName := "mr-" + strconv.Itoa(jobid) + "-" + strconv.Itoa(ihash(kv.Key)%nReduce)
		f, ok := mp[intermediateName]
		if !ok {
			log.Fatalf("Invalid file name %v\n", intermediateName)
		}
		enc := json.NewEncoder(f)
		// save the map results to json file
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatalf("Cannot encode key value pair %v - %v into a json file\n", kv.Key, kv.Value)
		}
	}

	// rename after successful writing to files
	for key, val := range mp {
		val.Close()
		err := os.Rename(val.Name(), key)
		if err != nil {
			log.Fatalf("Fail to rename the temp file %s to be %s\n", val.Name(), key)
		}
	}
}

func mapWorker(reply *RequestReply, mapf func(string, string) []KeyValue) {

	jobfile, jobId, nReduce := reply.File, reply.JobId, reply.NReduce

	// read file for content
	file, err := os.Open(jobfile)
	if err != nil {
		log.Fatalf("cannot open %v", jobfile)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", jobfile)
	}
	file.Close()

	// map work
	kva := mapf(jobfile, string(content))

	// create temp files to write into
	mp := createTempFiles(jobId, nReduce)

	// write map results into temp json files
	writeToJsonAndRename(jobId, nReduce, kva, mp)

	// report results to coordinator
	callForReport(Map, jobId, jobfile)
}

func reduceWorker(reply *RequestReply, reducef func(string, []string) string) {

	jobId, jobName, intermediateIds := reply.JobId, reply.File, reply.MapRes

	reduceId, _ := strconv.Atoi(jobName)

	ofName := "mr-out-" + strconv.Itoa(reduceId)
	ofile, _ := os.Create(ofName)

	kva := []KeyValue{}
	for _, id := range intermediateIds {
		matchfile := "mr-" + strconv.Itoa(id) + "-" + strconv.Itoa(reduceId)
		// read back from json file
		f, err := os.Open(matchfile)
		if err != nil {
			log.Fatalf("Cannot open intermediate file %s\n", matchfile)
		}
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	// sort the kva in order to group distinct keys
	sort.Sort(ByKey(kva))

	// do the reduce job
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	ofile.Close()

	callForReport(Reduce, jobId, jobName)
}
