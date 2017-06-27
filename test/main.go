package main

import "github.com/jamesruan/supervisor"
import (
	"fmt"
	"time"
	"log"
)

type Worker struct {
	stopped chan error
	//...
}

func (w *Worker) Stop() error {
	// some cleanup
	select {
	case <-w.stopped:
	default:
		close(w.stopped)
	}
	return nil
}

func (w *Worker) Kill() {
	// some cleanup
	select {
	case <-w.stopped:
	default:
		close(w.stopped)
	}
}

func (w *Worker) Monitor() supervisor.Monitor {
	return w.stopped
}

func (w *Worker) Start(name string, d time.Duration) {
	w.stopped = make(chan error)
	go func () {
		select {
		case <-w.stopped:
			return
		default:
			log.Printf("%s ON\n", name)
			select {
			case <-time.After(d):
				log.Printf("%s OFF\n", name)
				w.Stop()
				return
			}
		}
	}()
}

func start_worker(argv ...interface{}) supervisor.Supervisable {
	name := argv[0].(string)
	duration := argv[1].(time.Duration)
	worker := new(Worker)
	worker.Start(name, duration)

	return worker
}

func main() {
	childSpecs := make([]*supervisor.ChildSpec, 0)
	for i := 1; i < 4; i++ {
		childSpec := supervisor.NewChildSpec(start_worker, []interface{}{fmt.Sprintf("worker %d", i), time.Duration(4*i)*time.Second}, time.Second)
		childSpecs = append(childSpecs, childSpec)
	}
	sup := supervisor.New(childSpecs, supervisor.RestForOne, 1, 3 * time.Second)
	mon := sup.Monitor()
	select {
	case <-mon:
	}
}
