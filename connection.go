package jackrabbit

import (
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rhettcp/go-common/logging"
)

var log = logging.Log

// RabbitConnection holds and manages a single rabbitmq connection and channel.
// It also handles disconnects and reconnects
type RabbitConnection struct {
	connection      *amqp.Connection
	channel         *amqp.Channel
	connChanLock    sync.Mutex
	notifiers       []chan interface{}
	url             string
	connectionDone  chan *amqp.Error
	channelDone     chan *amqp.Error
	reconnectSignal chan interface{}
	done            chan interface{}
}

// NewRabbitConnection creates a new connection to Rabbitmq based on the
//
//	rabbitURL
func NewRabbitConnection(rabbitURL string) (*RabbitConnection, error) {
	rConn := &RabbitConnection{
		url:             rabbitURL,
		connectionDone:  make(chan *amqp.Error),
		channelDone:     make(chan *amqp.Error),
		reconnectSignal: make(chan interface{}),
		done:            make(chan interface{}),
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
				log.Warn("Connection Notification of Closure")
				failCount := 0
			connection:
				_, err := r.reconnectConnection()
				if failCount >= 3 {
					log.Error("Fail count greater than 3")
					close(r.done)
					return
				} else if err != nil {
					log.Error("Error reconnecting: ", err)
					time.Sleep(1000 * time.Millisecond * time.Duration(failCount))
					failCount++
					goto connection
				} else if err == nil {
					r.connectionDone = make(chan *amqp.Error)
					r.channelDone = make(chan *amqp.Error)
				}
				log.Info("Connection Restablished")
				r.reconnectSignal <- struct{}{}
			case <-r.channelDone:
				log.Warn("Channel Notification of Closure")
				failCount := 0
			channel:
				_, err := r.reconnectChannel(true)
				if failCount >= 3 {
					log.Error("Fail count greater than 3")
					close(r.done)
					return
				} else if err != nil {
					time.Sleep(1000 * time.Millisecond * time.Duration(failCount))
					failCount++
					goto channel
				} else if err == nil {
					r.channelDone = make(chan *amqp.Error)
				}
				log.Info("Channel Restablished")
				r.reconnectSignal <- struct{}{}
			}
		}
	}()
}

func (r *RabbitConnection) reconnectConnection() (ok bool, err error) {
	r.connChanLock.Lock()
	defer r.connChanLock.Unlock()
	r.channel.Close()
	r.connection.Close()
	log.Debug("Dialing amqp connection")
	c, err := amqp.Dial(r.url)
	if err != nil {
		return false, err
	}
	log.Debug("Amqp connection restablished")
	r.connection = c
	return r.reconnectChannel(false)
}

func (r *RabbitConnection) reconnectChannel(shouldLock bool) (ok bool, err error) {
	if shouldLock {
		r.connChanLock.Lock()
	}
	log.Debug("creating reconnected channel")
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
	log.Debug("reconnected channel created")
	return true, nil
}
