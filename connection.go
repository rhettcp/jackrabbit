package jackrabbit

import (
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitConnection holds and manages a single rabbitmq connection and channel.
// It also handles disconnects and reconnects
type RabbitConnection struct {
	connection     *amqp.Connection
	channel        *amqp.Channel
	connChanLock   sync.Mutex
	notifiers      []chan interface{}
	url            string
	connectionDone chan *amqp.Error
	channelDone    chan *amqp.Error
	done           chan interface{}
}

// NewRabbitConnection creates a new connection to Rabbitmq based on the
//
//	rabbitURL
func NewRabbitConnection(rabbitURL string) (*RabbitConnection, error) {
	rConn := &RabbitConnection{
		url:            rabbitURL,
		connectionDone: make(chan *amqp.Error),
		channelDone:    make(chan *amqp.Error),
		done:           make(chan interface{}),
	}
	c, err := amqp.Dial(rabbitURL)
	if err != nil {
		return nil, err
	}
	rConn.connection = c
	ch, err := c.Channel()
	if err != nil {
		return nil, err
	}
	rConn.channel = ch
	rConn.connection.NotifyClose(rConn.connectionDone)
	rConn.channel.NotifyClose(rConn.channelDone)
	rConn.startWatchdog()
	return rConn, nil
}

// NotifyClosed notifies the channel if the connection were to be closed.
// close(c) will be called.
func (r *RabbitConnection) NotifyClosed(c chan interface{}) {
	r.notifiers = append(r.notifiers, c)
}

// Close closes the channel then the connection, then notifies all listeners
func (r *RabbitConnection) Close() {
	r.connChanLock.Lock()
	defer r.connChanLock.Unlock()
	r.channel.Close()
	r.connection.Close()
	r.notifyOfClosed()
}

// GetChannel returns the underlying channel.
// The returned channel is not protected by reconnects
func (r *RabbitConnection) GetChannel() *amqp.Channel {
	return r.channel
}

func (r *RabbitConnection) notifyOfClosed() {
	for _, c := range r.notifiers {
		close(c)
	}
	r.notifiers = make([]chan interface{}, 0)
}

func (r *RabbitConnection) startWatchdog() {
	go func() {
		for {
			select {
			case <-r.done:
				r.Close()
				return
			case <-r.connectionDone:
				failCount := 0
			connection:
				_, err := r.reconnectConnection()
				if failCount >= 3 {
					close(r.done)
				} else if err != nil {
					time.Sleep(100 * time.Millisecond)
					failCount++
					goto connection
				} else if err == nil {
					r.connectionDone = make(chan *amqp.Error)
					r.channelDone = make(chan *amqp.Error)
				}
			case <-r.channelDone:
				failCount := 0
			channel:
				_, err := r.reconnectChannel(true)
				if failCount >= 3 {
					close(r.done)
				} else if err != nil {
					time.Sleep(100 * time.Millisecond)
					failCount++
					goto channel
				} else if err == nil {
					r.channelDone = make(chan *amqp.Error)
				}
			}
		}
	}()
}

func (r *RabbitConnection) reconnectConnection() (ok bool, err error) {
	r.connChanLock.Lock()
	defer r.connChanLock.Unlock()
	r.channel.Close()
	r.connection.Close()
	c, err := amqp.Dial(r.url)
	if err != nil {
		return false, err
	}
	r.connection = c
	return r.reconnectChannel(false)
}

func (r *RabbitConnection) reconnectChannel(shouldLock bool) (ok bool, err error) {
	if shouldLock {
		r.connChanLock.Lock()
	}
	ch, err := r.connection.Channel()
	if err != nil {
		if shouldLock {
			r.connChanLock.Unlock()
		}
		return false, err
	}
	r.channel = ch
	if shouldLock {
		r.connChanLock.Unlock()
	}
	return true, nil
}
