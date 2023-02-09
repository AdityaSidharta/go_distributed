package mapreduce

import (
	"container/list"
	"log"
	"strconv"
	"time"
)
import "fmt"

const (
	Idle       = "Idle"
	InProgress = "InProgress"
	Dead       = "Dead"
)

type StatusType string

type WorkerInfo struct {
	address string
	status  StatusType
}

type JobInfo struct {
	JobNumber int
	Success   bool
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) GetIdleWorkerId() (string, bool) {
	mr.mu.Lock()
	defer mr.mu.Unlock()

	for workerId, workerInfo := range mr.Workers {
		if workerInfo.status == Idle {
			DPrintln("GetIdleWorkerId : workerID %s is chosen", workerId)
			mr.Workers[workerId].status = InProgress
			return workerId, true
		}
	}
	//DPrintln("GetIdleWorkerId : None of the Worker is Idle right now. Unlocking...")
	return "nil", false
}

func (mr *MapReduce) SetWorkerStatus(workerId string, workerStatus StatusType) {
	mr.mu.Lock()
	defer mr.mu.Unlock()

	prevWorkerStatus := mr.Workers[workerId].status
	if prevWorkerStatus == workerStatus {
		DPrintln("workerID %s status does not change: %s", workerId, workerStatus)
	} else {
		mr.Workers[workerId].status = workerStatus
		DPrintln("workerID %s status changed from %s to %s", workerId, prevWorkerStatus, workerStatus)
	}
}

func (mr *MapReduce) AssignWork(JobNumber int, JobDone chan JobInfo, Operation JobType) {
	var numOtherPhase int
	var reply DoJobReply
	for {
		workerId, ok := mr.GetIdleWorkerId()
		if ok {
			DPrintln("GetIdleWorkerId : %s", workerId)
			workerInfo := mr.Workers[workerId]
			if Operation == Map {
				numOtherPhase = mr.nReduce
			} else if Operation == Reduce {
				numOtherPhase = mr.nMap
			} else {
				log.Fatalln("AssignWork : Invalid Operation Type : ", Operation)
			}
			workerArgs := DoJobArgs{
				mr.file,
				Operation,
				JobNumber,
				numOtherPhase,
			}
			DPrintln("Performing Operation %s, Job Number %d, Worker ID %s, Worker Address %s", Operation, JobNumber, workerInfo.address, workerId)

			ok := call(
				workerInfo.address,
				"Worker.DoJob",
				workerArgs,
				&reply,
			)

			if ok {
				DPrintln("Completed Operation %s, Job Number %d, Worker ID %s, Worker Address %s", Operation, JobNumber, workerInfo.address, workerId)
				mr.SetWorkerStatus(workerId, Idle)
			} else {
				DPrintln("Failed Operation %s, Job Number %d, Worker ID %s, Worker Address %s", Operation, JobNumber, workerInfo.address, workerId)
				mr.SetWorkerStatus(workerId, Dead)
			}
			jobInfo := JobInfo{
				JobNumber,
				ok,
			}
			JobDone <- jobInfo
			return
		}
	}
}

func (mr *MapReduce) AddWorkers(quit chan bool) {
	workerNumber := 0
	for {
		select {
		case workerAddress := <-mr.registerChannel:
			mr.mu.Lock()
			DPrintln("Previous Number of Workers : %d", mr.nWorkers)
			DPrintln("Adding Worker : %s", workerAddress)
			workerInfo := WorkerInfo{
				address: workerAddress,
				status:  Idle,
			}
			mr.Workers[strconv.Itoa(workerNumber)] = &workerInfo
			mr.nWorkers = len(mr.Workers)
			DPrintln("New Number of Workers : %d", mr.nWorkers)
			workerNumber = workerNumber + 1
			time.Sleep(1 * time.Millisecond)
			mr.mu.Unlock()
		case <-quit:
			DPrintln("AddWorkers has been completed")
			return
		}
	}
}

func (mr *MapReduce) RunMaster() *list.List {
	mr.Workers = make(map[string]*WorkerInfo)
	quit := make(chan bool)
	MapDone := make(chan JobInfo)
	ReduceDone := make(chan JobInfo)
	MapQueue := list.New()
	ReduceQueue := list.New()

	DPrintln("nMap : %d", mr.nMap)
	DPrintln("nReduce : %d", mr.nReduce)
	go mr.AddWorkers(quit)

	for mapJobNumber := 0; mapJobNumber < mr.nMap; mapJobNumber++ {
		MapQueue.PushBack(mapJobNumber)
	}
	for MapQueue.Len() > 0 {
		nQueue := MapQueue.Len()
		for i := 0; i < nQueue; i++ {
			mapItem := MapQueue.Front()
			mapJobNumber := mapItem.Value.(int)
			go mr.AssignWork(mapJobNumber, MapDone, Map)
			MapQueue.Remove(mapItem)
		}
		for i := 0; i < nQueue; i++ {
			jobInfo := <-MapDone
			if !jobInfo.Success {
				DPrintln("Map %d has failed", jobInfo.JobNumber)
				MapQueue.PushBack(jobInfo.JobNumber)
			} else {
				DPrintln("Map %d has been completed", jobInfo.JobNumber)
			}
		}
	}

	for reduceJobNumber := 0; reduceJobNumber < mr.nReduce; reduceJobNumber++ {
		ReduceQueue.PushBack(reduceJobNumber)
	}
	for ReduceQueue.Len() > 0 {
		nQueue := ReduceQueue.Len()
		for i := 0; i < nQueue; i++ {
			reduceItem := ReduceQueue.Front()
			reduceJobNumber := reduceItem.Value.(int)
			go mr.AssignWork(reduceJobNumber, ReduceDone, Reduce)
			ReduceQueue.Remove(reduceItem)
		}
		for i := 0; i < nQueue; i++ {
			jobInfo := <-ReduceDone
			if !jobInfo.Success {
				DPrintln("Reduce %d has failed", jobInfo.JobNumber)
				ReduceQueue.PushBack(jobInfo.JobNumber)
			} else {
				DPrintln("Reduce %d has been completed", jobInfo.JobNumber)
			}
		}
	}

	quit <- true
	return mr.KillWorkers()
}
