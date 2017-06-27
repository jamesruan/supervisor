// Package supervisor provides facility for generic supervisor
//
// A Supervisor will start its children and try to maintain the running state of its children.
// If a child was in wrong states, it terminated itself and the supervisor would try to restart it.
// Children are started in the order of list of ChildSpec and if killed, in reverse order.
//
// Example
//
// A Worker implements Supervisable:
//  type Worker struct {
//  	stopped chan error
//  //...
//  }
//
//  func (w *Worker) Stop() error {
//  // some cleanup
//  	close(w.stopped)
//  	return nil
//  }
//
//  func (w *Worker) Kill() {
//  // some cleanup
//  	close(w.stopped)
//  }
//
//  func (w *Worker) Monitor() supervisor.Monitor {
//  	return w.stopped
//  }
//
// Register the Worker to supervior:
//  func start_worker() supervisor.Supervisable {
//  	worker := new(Worker)
//  	// do something to start the worker
//  	return worker
//  }
//
//  func main() {
//  	childSpec := supervisor.ChildSpec{start_worker, 5 * time.Second}
//  	sup := supervisor.New([]supervisor.ChildSpec{childSpec}, supervisor.OneForOne, 1, 5)
//  	mon := sup.Monitor()
//  	select {
//  	case <-mon:
//  	}
//  }
package supervisor

import (
	"fmt"
	"sync"
	"time"
	"log"
)

var (
	killed = fmt.Errorf("killed")
)

type RestartStrategy string

const (
	OneForOne      RestartStrategy = "one_for_one"       // Restart the terminated child.
	OneForAll                      = "one_for_all"       // Stop all child in list for one child's termination and restart all.
	RestForOne                     = "rest_for_all"      // Restart the terminated child all child after the terminated child in the list
	AsyncOneForOne                 = "async_one_for_one" // All child are independent and concurrently restarted in a OneForOne way
)

type Supervisable interface {
	Stop() error // For send stop signal, pending until success
	Kill()       // For brutal kill, should always success
	Monitable    // the child is terminated when the Monitor closed.
}

type Monitable interface {
	Monitor() Monitor // Monitor will receive error
}

// Monitor is closed when corresponding child is terminated
type Monitor <-chan error

type ChildSpec struct {
	start          func(...interface{}) Supervisable // function will called when restart
	args            []interface{}
	shutDownTimeout time.Duration       /* timeout in seconds:
	If Stop() not return in this period, Kill() will be called.
	For 0 or negative value only Kill() will be called. */
	state         childState
}

type childState string
const (
	active childState = "A"
	stopped = "S"
	restarting = "R"
)


// Supervisor also implements Supervisable interface for chaining
type Supervisor struct {
	Strategy         RestartStrategy
	Intensity        int // Restart occurs more than Intensity in Period will terminate this supervisor
	Period           time.Duration
	children_spec    []*ChildSpec
	children         []Supervisable
	downindex        chan int
	stopped          chan error
	stopRecords      []time.Time
	lock             *sync.Mutex
	wg               *sync.WaitGroup
}

// Will try to stop all children
func (s *Supervisor) Stop() error {
	select {
	case <-s.stopped:
		return nil
	default:
		// call all child to stop
		if s.Strategy == AsyncOneForOne {
			wg := new(sync.WaitGroup)
			for i := range s.children {
				rev := len(s.children) - i - 1
				go func(i int){
					wg.Add(1)
					s.tryStop(i)
					wg.Done()
				}(rev)
			}
			wg.Wait()
		} else {
			for i := range s.children {
				rev := len(s.children) - i - 1
				s.tryStop(rev)
			}
		}
		close(s.stopped)
		s.wg.Wait()
		close(s.downindex)
	}
	return nil
}

// Will kill all children
func (s *Supervisor) Kill() {
	select {
	case <-s.stopped:
	default:
		close(s.stopped)
	}
}

// return Monitor will be closed if more the Intensity restarting occurs in Period
func (s *Supervisor) Monitor() Monitor {
	return s.stopped
}

func NewChildSpec(start func(...interface{})Supervisable, args []interface{}, shut_down_timeout time.Duration) *ChildSpec{
	return &ChildSpec{
		start: start,
		args: args,
		shutDownTimeout: shut_down_timeout,
	}
}

func New(children_spec []*ChildSpec, strategy RestartStrategy, intensity int, period time.Duration) Supervisable {
	children := make([]Supervisable, len(children_spec))
	downindex := make(chan int)
	stopped := make(chan error)

	s := &Supervisor{
		Strategy: strategy,
		Intensity: intensity,
		Period: period,
		children_spec: children_spec,
		children: children,
		downindex: downindex,
		stopped: stopped,
		stopRecords: make([]time.Time, 0, intensity),
		lock: new(sync.Mutex),
		wg: new(sync.WaitGroup),
	}

	for i := range children_spec {
		s.startByIndex(i)
	}

	go func () {
		select {
		case <-stopped:
			return
		default:
			for idx := range s.downindex {
				log.Printf("%d down\n", idx)
				sr := make([]time.Time, 0, len(s.stopRecords))

				s.lock.Lock()
				// add record
				s.stopRecords = append(s.stopRecords, time.Now())

				//cleanup outdated records
				for _, t := range s.stopRecords {
					if t.Add(s.Period).Before(time.Now()) {
						continue
					} else {
						sr = append(sr, t)
					}
				}
				s.stopRecords = sr
				s.lock.Unlock()

				if len(sr) > s.Intensity {
					s.Stop()
					return
				}

				s.handleTerminated(idx)
			}
		}
	}()

	return s
}

func (s *Supervisor) tryStop(i int) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	log.Printf("tryStop %d\n", i)

	spec := s.children_spec[i]
	child := s.children[i]

	// start stopping
	ch := make(chan error)
	defer close(ch)
	go func() {
		ch <- child.Stop()
	}()

	select {
	case err := <-ch:
		return err
	case <-time.After(spec.shutDownTimeout):
		return killed
	}
}

func (s *Supervisor) startByIndex(i int) {
	log.Printf("startByIndex %d", i)
	s.lock.Lock()
	defer s.lock.Unlock()

	spec := s.children_spec[i]
	child := spec.start(spec.args...)
	mon := child.Monitor()
	s.children[i] = child

	go func (index int, mon Monitor) {
		s.wg.Add(1)
		defer s.wg.Done()
		select {
		case <-s.stopped:
		case _, ok := <-mon:
			if !ok {
				s.downindex <- i
			}
		}
		return
	}(i, mon)
}

func (s *Supervisor) handleTerminated(i int) {
	switch s.Strategy {
	case OneForOne:
		s.startByIndex(i)
	case OneForAll:
		// stop in reverse order
		for j := range s.children {
			rev := len(s.children) - j - 1
			s.tryStop(rev)
		}
		// start all
		for j := range s.children_spec {
			s.startByIndex(j)
		}
	case RestForOne:
		// stop in reverse order
		for j := range s.children[i:] {
			rev := len(s.children) - i - j - 1
			s.tryStop(rev)
		}
		// start i and child after
		for j := range s.children[i:] {
			s.startByIndex(j + i)
		}
	case AsyncOneForOne:
		go s.startByIndex(i)
	default:
		panic("unreachable code")
	}
}
