package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	nMap       int
	nReduce    int
	doneMap    int
	doneReduce int
	tasks      []task

	heartbeatCh chan controlMessage[*HeartbeatResponse]
	finishCh    chan controlMessage[*FinishRequest]
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) getNextPendingTask() (*task, bool) {
	for i := 0; i < len(c.tasks); i++ {
		task := &c.tasks[i]
		if task.taskStatus == taskStatusPending {
			return task, true
		}
	}
	return nil, false
}

// controllerLoop listen the heartbeatCh and finishCh
func (c *Coordinator) controllerLoop() {
	updateTask := func(t *task) {
		t.taskStatus = taskStatusRunning
		t.startTime = time.Now()
	}
	watchdog := func(t *task) {
		time.Sleep(time.Duration(10) * time.Second) // wait 10 seconds
		if t.taskStatus == taskStatusRunning {
			t.taskStatus = taskStatusPending
		}
	}
	for {
		select {
		case msg := <-c.heartbeatCh:
			// Map Phase not complete
			if c.doneMap != c.nMap {
				task, ok := c.getNextPendingTask()
				if ok {
					msg.data.Filename = task.filename
					msg.data.JobType = JobTypeMap
					msg.data.MapTaskSeq = task.id
					msg.data.NMap = c.nMap
					msg.data.NReduce = c.nReduce
					updateTask(task)
					go watchdog(task)
				} else {
					msg.data.JobType = JobTypeWait
				}
			} else if c.doneReduce != c.nReduce {
				task, ok := c.getNextPendingTask()
				if ok {
					msg.data.JobType = JobTypeReduce
					msg.data.ReduceTaskSeq = task.id
					msg.data.NMap = c.nMap
					msg.data.NReduce = c.nReduce
					updateTask(task)
					go watchdog(task)
				} else {
					// no pending task for allocate
					msg.data.JobType = JobTypeWait
				}
			} else {
				msg.data.JobType = JobTypeDone
			}
			msg.ok <- struct{}{}
		case msg := <-c.finishCh:
			switch msg.data.JobType {
			case JobTypeMap:
				t := &c.tasks[msg.data.Seq]
				// the task is finish here meas the task assign to two worker
				// one is timeout and one reassigned
				// whether which one comes after reassign, it's valid and we
				// finish the work. Then another one comes and found the task
				// has been finished. In this case, we break the handler
				if t.taskStatus == taskStatusFinish {
					break
				}
				// we dont need to consider task status pending here
				// because if the task status pending here
				// it means that one is timeout and then
				// the timeout one comes before the work has been reassigned
				// so we can finish the task
				// [TODO]: should we DROP Pending task here?
				c.doneMap++
				t.taskStatus = taskStatusFinish
				if c.doneMap == c.nMap {
					// init reduce tasks
					if len(c.tasks) < c.nReduce {
						c.tasks = append(c.tasks, make([]task, c.nReduce-len(c.tasks))...)
					} else {
						c.tasks = c.tasks[:c.nReduce]
					}
					for i := range c.tasks {
						c.tasks[i].taskStatus = taskStatusPending
						c.tasks[i].id = i
					}
				}
			case JobTypeReduce:
				c.doneReduce++
				t := &c.tasks[msg.data.Seq]
				t.taskStatus = taskStatusFinish
			}
			msg.ok <- struct{}{}
		}
	}
}

// HeartBeat
func (c *Coordinator) HeartBeat(_ *HeartbeatRequest, response *HeartbeatResponse) error {
	msg := newControlMessage(response)
	c.heartbeatCh <- msg
	<-msg.ok
	return nil
}

func (c *Coordinator) FinishWork(req *FinishRequest, _ *FinishResponse) error {
	msg := newControlMessage(req)
	c.finishCh <- msg
	<-msg.ok
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) serve() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	// start controller loop
	go c.controllerLoop()
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := c.doneMap == c.nMap && c.doneReduce == c.nReduce

	// Your code here.

	return ret
}

func createDistDir() {
	if _, err := os.Stat("dist"); os.IsNotExist(err) {
		err := os.Mkdir("dist", 0o777)
		if err != nil {
			panic(err)
		}
	} else if err == nil {
		files, err := os.ReadDir("dist")
		if err != nil {
			panic(err)
		}
		for _, file := range files {
			os.RemoveAll(filepath.Join("dist", file.Name()))
		}
	}
	os.Chmod("dist", 0o777)
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	tasks := make([]task, len(files))
	for i, f := range files {
		tasks[i] = task{
			id:         i,
			filename:   f,
			startTime:  time.Time{},
			taskStatus: taskStatusPending,
		}
	}
	c := Coordinator{
		len(files), nReduce, 0, 0, tasks,
		make(chan controlMessage[*HeartbeatResponse]),
		make(chan controlMessage[*FinishRequest]),
	}

	// Your code here.

	c.serve()
	createDistDir()

	log.Println("coordinator has started!")
	return &c
}
