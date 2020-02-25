package chunk

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"

	. "github.com/claudetech/loggo/default"
	"github.com/dweidenfeld/plexdrive/drive"
)

// Downloader handles concurrent chunk downloads
type Downloader struct {
	Client     *drive.Client
	queue      chan *Request
	callbacks  map[string][]DownloadCallback
	lock       sync.Mutex
	bufferPool *BufferPool
	storage    *Storage
}

// DownloadCallback is called after a download has finished
type DownloadCallback func(error, *Buffer)

// NewDownloader creates a new download manager
func NewDownloader(threads int, client *drive.Client, bufferPool *BufferPool, storage *Storage) (*Downloader, error) {
	manager := Downloader{
		Client:     client,
		queue:      make(chan *Request, 100),
		callbacks:  make(map[string][]DownloadCallback, 100),
		bufferPool: bufferPool,
		storage:    storage,
	}

	for i := 0; i < threads; i++ {
		go manager.thread()
	}

	return &manager, nil
}

// Download starts a new download request
func (d *Downloader) Download(req *Request, callback DownloadCallback) {
	d.lock.Lock()
	_, exists := d.callbacks[req.id]
	d.callbacks[req.id] = append(d.callbacks[req.id], callback)
	if !exists {
		d.queue <- req
	}
	d.lock.Unlock()
}

func (d *Downloader) thread() {
	for {
		req := <-d.queue
		d.download(d.Client.GetNativeClient(), req)
	}
}

func (d *Downloader) download(client *http.Client, req *Request) {
	Log.Debugf("Starting download %v (preload: %v)", req.id, req.preload)
	buffer, err := d.downloadFromAPI(client, req, 0)

	if err := d.storage.Store(req.id, buffer); nil != err {
		Log.Warningf("Coult not store chunk %v", req.id)
	}

	d.lock.Lock()
	callbacks := d.callbacks[req.id]
	for _, callback := range callbacks {
		callback(err, buffer)
	}
	delete(d.callbacks, req.id)
	d.lock.Unlock()

	if nil != buffer {
		buffer.Unref()
	}
}

func (d *Downloader) downloadFromAPI(client *http.Client, request *Request, delay int64) (*Buffer, error) {
	// sleep if request is throttled
	if delay > 0 {
		time.Sleep(time.Duration(delay) * time.Second)
	}

	req, err := http.NewRequest("GET", request.object.DownloadURL, nil)
	if nil != err {
		Log.Debugf("%v", err)
		return nil, fmt.Errorf("Could not create request object %v (%v) from API", request.object.ObjectID, request.object.Name)
	}

	req.Header.Add("Range", fmt.Sprintf("bytes=%v-%v", request.offsetStart, request.offsetEnd))

	Log.Tracef("Sending HTTP Request %v", req)

	res, err := client.Do(req)
	if nil != err {
		Log.Debugf("%v", err)
		return nil, fmt.Errorf("Could not request object %v (%v) from API", request.object.ObjectID, request.object.Name)
	}
	defer res.Body.Close()
	reader := res.Body

	if res.StatusCode != 206 {
		if res.StatusCode != 403 && res.StatusCode != 500 {
			Log.Debugf("Request\n----------\n%v\n----------\n", req)
			Log.Debugf("Response\n----------\n%v\n----------\n", res)
			return nil, fmt.Errorf("Wrong status code %v for %v", res.StatusCode, request.object)
		}

		// throttle requests
		if delay > 8 {
			return nil, fmt.Errorf("Maximum throttle interval has been reached")
		}
		bytes, err := ioutil.ReadAll(reader)
		if nil != err {
			Log.Debugf("%v", err)
			return nil, fmt.Errorf("Could not read body of error")
		}
		body := string(bytes)
		if strings.Contains(body, "dailyLimitExceeded") ||
			strings.Contains(body, "userRateLimitExceeded") ||
			strings.Contains(body, "rateLimitExceeded") ||
			strings.Contains(body, "backendError") ||
			strings.Contains(body, "internalError") {
			if 0 == delay {
				delay = 1
			} else {
				delay = delay * 2
			}
			return d.downloadFromAPI(client, request, delay)
		}

		// return an error if other error occurred
		Log.Debugf("%v", body)
		return nil, fmt.Errorf("Could not read object %v (%v) / StatusCode: %v",
			request.object.ObjectID, request.object.Name, res.StatusCode)
	}

	buffer := d.bufferPool.Get()
	buffer.Ref()
	n, err := buffer.ReadFrom(reader)
	if nil != err {
		buffer.Unref()
		Log.Debugf("%v", err)
		return nil, fmt.Errorf("Could not read objects %v (%v) API response", request.object.ObjectID, request.object.Name)
	}
	Log.Debugf("Downloaded %v bytes of %v (%v)", n, request.object.ObjectID, request.object.Name)

	return buffer, nil
}
