package mapreduce

import (
	"log"
	"sync"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
		go func() {
			for _, wk := range mr.workers {
				mr.registerChannel <- wk
			}
		}()
	}

	debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	var wg sync.WaitGroup
	taskCount := 0
	for {
		if taskCount >= ntasks {
			break
		}

		var file string
		switch phase {
		case mapPhase:
			file = mr.files[taskCount]
		case reducePhase:
			file = ""
		}

		select {
		case wk := <-mr.registerChannel:
			wg.Add(1)
			go func(currTask int) {
				defer wg.Done()

				args := DoTaskArgs{mr.jobName, file, phase, currTask, nios}

				ok := call(wk, "Worker.DoTask", args, new(struct{}))
				if !ok {
					log.Fatalf("Failed to DoTask %v on worker %v with args %v\n", currTask, wk, args)
				}

				select {
				case mr.registerChannel <- wk:
					break
				default:
					log.Printf("Couldn't register worker to recieve new work!")
				}
			}(taskCount)

			taskCount++
		}

		// TODO: What happens if a worker fails? (Part 2 of assignment)
	}

	// Wait for all workers to finish
	wg.Wait()

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.

	debug("Schedule: %v phase done\n", phase)
}
