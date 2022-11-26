package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

var mapChan chan string
var reduceChan chan int

type JobStatus int

const (
	unstarted JobStatus = 0
	running   JobStatus = 1
	finished  JobStatus = 2
)

type Coordinator struct {
	// Your definitions here.
	mu               *sync.RWMutex
	files            []string
	nReduce          int
	mapTaskStatus    map[string]JobStatus
	reduceTaskStatus map[int]JobStatus
	mapRes           []int
	mapFinished      bool
	reduceFinished   bool
	n                int
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) RequestJob(args *RequestArgs, reply *RequestReply) error {
	select {
	case filename := <-mapChan:
		reply.File = filename
		reply.JobType = Map

		c.mu.Lock()
		reply.NReduce = c.nReduce
		reply.JobId = c.n
		c.mapTaskStatus[filename] = running
		c.n += 1
		c.mu.Unlock()
		go monitorTimedTask(c, Map, filename)

	case reduceId := <-reduceChan:
		reply.File = strconv.Itoa(reduceId)
		reply.JobType = Reduce

		c.mu.Lock()
		reply.MapRes = c.mapRes
		reply.NReduce = c.nReduce
		reply.JobId = c.n
		// only valid intermediate files will be passed to do the reduce task
		c.reduceTaskStatus[reduceId] = running
		c.n += 1
		c.mu.Unlock()
		go monitorTimedTask(c, Reduce, strconv.Itoa(reduceId))

	}
	return nil
}

func monitorTimedTask(c *Coordinator, jobType JobType, jobIdentity string) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if jobType == Map {
				c.mu.Lock()
				c.mapTaskStatus[jobIdentity] = unstarted
				c.mu.Unlock()
				mapChan <- jobIdentity
			} else {
				reduceId, _ := strconv.Atoi(jobIdentity)
				c.mu.Lock()
				c.reduceTaskStatus[reduceId] = unstarted
				c.mu.Unlock()
				reduceChan <- reduceId
			}
			return
		default:
			if jobType == Map {
				c.mu.Lock()
				if c.mapTaskStatus[jobIdentity] == finished {
					c.mu.Unlock()
					return
				} else {
					c.mu.Unlock()
				}
			} else {
				reduceId, _ := strconv.Atoi(jobIdentity)
				c.mu.Lock()
				if c.reduceTaskStatus[reduceId] == finished {
					c.mu.Unlock()
					return
				} else {
					c.mu.Unlock()
				}
			}
		}
	}
}

func (c *Coordinator) ReportFinish(args *RequestArgs, reply *int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch args.Info.JobTp {
	case Map:
		c.mapTaskStatus[args.Info.JobName] = finished
		c.mapRes = append(c.mapRes, args.Info.JobId)
	case Reduce:
		reduceId, _ := strconv.Atoi(args.Info.JobName)
		c.reduceTaskStatus[reduceId] = finished
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	mapChan = make(chan string)
	reduceChan = make(chan int)
	go c.monitorTask(mapChan, reduceChan)

	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) monitorTask(mapChan chan string, reduceChan chan int) {
	// put map tasks into a channel, blocked when channel is full.
	for _, k := range c.files {
		mapChan <- k
	}

	ok := false
	for !ok {
		ok = checkMapStatus(c)
	}
	c.mu.Lock()
	c.mapFinished = true
	c.mu.Unlock()

	// put reduce tasks into a channel, blocked when channel is full.
	for i := 0; i < c.nReduce; i++ {
		reduceChan <- i
	}

	ok = false
	for !ok {
		ok = checkReduceStatus(c)
	}
	c.mu.Lock()
	c.reduceFinished = true
	c.mu.Unlock()
}

func checkMapStatus(c *Coordinator) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, v := range c.mapTaskStatus {
		if v != finished {
			return false
		}
	}
	return true
}

func checkReduceStatus(c *Coordinator) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, v := range c.reduceTaskStatus {
		if v != finished {
			return false
		}
	}
	return true
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.reduceFinished
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	c.files = files
	c.nReduce = nReduce
	c.mapTaskStatus = map[string]JobStatus{}
	c.reduceTaskStatus = map[int]JobStatus{}
	c.mapRes = make([]int, 0)
	c.mapFinished = false
	c.reduceFinished = false
	c.mu = new(sync.RWMutex)
	c.n = 1
	for _, file := range files {
		c.mapTaskStatus[file] = unstarted
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTaskStatus[i] = unstarted
	}

	c.server()
	return &c
}
