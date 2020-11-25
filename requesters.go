package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"path"
	"strconv"
	"time"

	"io/ioutil"

	randc "crypto/rand"
	"github.com/tigrawap/goader/utils"
	"github.com/valyala/fasthttp"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	aws_req "github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/tigrawap/goader/ops"
)

type nullRequester struct {
}

type sleepRequster struct {
	state *OPState
	db    chan int
}

func (n *nullRequester) request(responses chan *Response, request *Request) {
	responses <- &Response{request, time.Nanosecond, nil}
}

func (requester *sleepRequster) request(responses chan *Response, request *Request) {
	if rand.Intn(10000)-int(requester.state.inFlight) < 0 {
		responses <- &Response{request, 0, errors.New("Bad response")}
		return
	}
	start := time.Now()
	requester.db <- 0
	var timeToSleep = time.Duration(rand.Intn(200)) * time.Millisecond
	time.Sleep(timeToSleep)
	<-requester.db
	responses <- &Response{request, time.Since(start), nil}
}

func newSleepRequster(state *OPState) *sleepRequster {
	r := sleepRequster{
		state: state,
		db:    make(chan int, 10),
	}
	return &r
}

type httpRequester struct {
	client  fasthttp.Client
	timeout time.Duration
	method  string
	auther  HTTPAuther
	state   *OPState
}

func newHTTPRequester(state *OPState, auther HTTPAuther) *httpRequester {
	requester := httpRequester{
		state:  state,
		auther: auther,
	}
	requester.state = state
	if state.op == WRITE {
		requester.method = "PUT"
	} else {
		requester.method = "GET"
	}
	if config.maxLatency == NotSet {
		requester.timeout = 60 * time.Second
	} else {
		requester.timeout = 5 * config.maxLatency
	}
	requester.client.MaxConnsPerHost = config.maxChannels
	return &requester
}

var requestersConfig struct {
	payloadGetter       PayloadGetter
	scratchBufferGetter scratchDataPayloadGetter
	fullData            []byte
	scratchData         []byte
}

func (requester *httpRequester) request(responses chan *Response, request *Request) {
	var req *fasthttp.Request
	defer func() {
		if err := recover(); err != nil { //catch
			responses <- &Response{&Request{targeter: &BadUrlTarget{}, startTime: time.Now()}, time.Nanosecond,
				fmt.Errorf("Error: %s,%v", "panic:", err)}
			return
		}
	}()
	for i := 0; i < 10 && req == nil; i++ {
		req = fasthttp.AcquireRequest()
		if req == nil {
			log.Println("Could not acquire request object from fasthttp, retrying")
		}
	}
	if req == nil {
		log.Fatalln("Could not acquire request object from fasthttp pool after 10 retries")
	}
	defer fasthttp.ReleaseRequest(req)
	req.SetRequestURI(request.getUrl())

	req.Header.SetMethodBytes([]byte(requester.method))
	req.Header.Set("Connection", "keep-alive")
	if requester.method == "PUT" || requester.method == "POST" {
		req.SetBody(requestersConfig.payloadGetter.Get())
	}
	requester.auther.sign(req)
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)
	start := time.Now()
	err := requester.client.DoTimeout(req, resp, requester.timeout)
	timeSpent := time.Since(start)
	statusCode := resp.StatusCode()

	if err != nil {
		responses <- &Response{request, timeSpent,
			fmt.Errorf("Bad request: %s\n%s\n%s", err, resp.Header.String(), resp.Body())}
		return
	}

	switch statusCode {
	case fasthttp.StatusOK, fasthttp.StatusCreated:
		responses <- &Response{request, timeSpent, nil}
	default:
		responses <- &Response{request, timeSpent,
			fmt.Errorf("Error: %d \n%s \n%s ", resp.StatusCode(), resp.Header.String(), resp.Body())}
	}
}

type diskRequester struct {
	state *OPState
}

func newDiskRequester(state *OPState) *diskRequester {
	r := diskRequester{
		state: state,
	}
	return &r
}

func (requester *diskRequester) request(responses chan *Response, request *Request) {
	requester.doRequest(responses, request, false)
}

func (requester *diskRequester) doRequest(responses chan *Response, request *Request, isRetry bool) {
	filename := request.getUrl()

	var err error
	start := time.Now()
	if requester.state.op == WRITE {
		err = ioutil.WriteFile(filename, requestersConfig.payloadGetter.Get(), 0644)
		if os.IsNotExist(err) && config.mkdirs {
			err = os.MkdirAll(path.Dir(filename), 0755)
			if !isRetry {
				requester.doRequest(responses, request, true)
				return
			}
		}
	} else {
		if fd, err := os.OpenFile(filename, os.O_RDONLY, 0644); err == nil {
			defer fd.Close()
			if fi, err := fd.Stat(); err == nil {
				size := fi.Size()
				_, err = fd.Read(requestersConfig.scratchBufferGetter.GetBuffer(size))
				if err != nil {
					log.Printf("Read failed: %v\n", err)
				}
			}
		}
	}
	responses <- &Response{request, time.Since(start), err}
}

type metaOpRequst string

const (
	opUnlink      metaOpRequst = "unlink"
	opTruncate                 = "truncate"
	opMknod                    = "mknod"
	opWrite                    = "write"
	opRead                     = "read"
	opStat                     = "stat"
	opSetattr                  = "setattr"
	opSymlink                  = "symlink"
	opHardLink                 = "hardlink"
	opRename                   = "rename"
	opSetxattr                 = "setxattr"
	opGetxattr                 = "getxattr"
	opListxattr                = "listxattr"
	opRemovexattr              = "removexattr"
)

var allMetaOps = []metaOpRequst{
	opUnlink,
	opTruncate,
	opMknod,
	opWrite,
	opRead,
	opStat,
	opSetattr,
	opSymlink,
	opHardLink,
	opRename,
	opSetxattr,
	opGetxattr,
	opRemovexattr,
}

type metaRequester struct {
	state           *OPState
	ops             []metaOpRequst
	opLen           int //pre-calculated ops len
	xattrPayload    PayloadGetter
	xattrReadBuffer PayloadGetter
}

func (r *metaRequester) request(responses chan *Response, request *Request) {
	r.doRequest(responses, request, false)
}

func (r *metaRequester) doRequest(responses chan *Response, request *Request, isRetry bool) {
	filename := request.getUrl()
	var err error
	start := time.Now()
	op := r.ops[rand.Intn(r.opLen)]
	switch op {
	case opWrite, opRead:
		var fd *os.File
		var flags int
		if op == opWrite {
			flags = os.O_WRONLY | os.O_CREATE
		} else {
			flags = os.O_RDONLY
		}
		if fd, err = os.OpenFile(filename, flags, 0644); err == nil {
			fd.Seek(rand.Int63n(int64(config.fileOffsetLimit)), io.SeekStart)
			if op == opWrite {
				fd.Write(requestersConfig.payloadGetter.Get())
			} else {
				fd.Read(requestersConfig.scratchBufferGetter.GetBuffer(requestersConfig.payloadGetter.GetLength()))
			}
			fd.Close() // TODO: Defer and reuse FD
		}
	case opSetattr:
		err = os.Chown(filename, rand.Int(), rand.Int())
	case opTruncate:
		err = os.Truncate(filename, rand.Int63n(int64(config.fileOffsetLimit)))
	case opUnlink:
		err = os.Remove(filename)
	case opStat:
		_, err = os.Stat(filename)
	case opSetxattr:
		err = ops.Setxattr(filename, getXattrName(), r.xattrPayload.Get(), 0)
	case opGetxattr:
		_, err = ops.Getxattr(filename, getXattrName(), r.xattrReadBuffer.Get())
	case opListxattr:
		_, err = ops.Listxattr(filename, r.xattrReadBuffer.Get())
	case opRemovexattr:
		err = ops.Removexattr(filename, getXattrName())
	case opMknod:
		err = mknod(filename)
	case opSymlink:
		os.Remove(request.getUrl())
		err = os.Symlink(utils.GetAbsolute(request.targeter.get()), request.getUrl())
	case opHardLink:
		os.Remove(request.getUrl())
		err = os.Link(utils.GetAbsolute(request.targeter.get()), request.getUrl())
	case opRename:
		err = os.Rename(request.getUrl(), utils.GetAbsolute(request.targeter.get()))
	default:
		log.Panicln("Unknown IO")
	}
	if os.IsNotExist(err) && config.mkdirs {
		err = os.MkdirAll(path.Dir(filename), 0755)
		if !isRetry {
			r.doRequest(responses, request, true)
			return
		}
	}
	responses <- &Response{request, time.Since(start), err}
}

func newMetaRequester(state *OPState) *metaRequester {
	r := metaRequester{
		state: state,
	}

	xattrData := make([]byte, config.metaXattrLength)
	randc.Read(xattrData)
	xattrTrashData := make([]byte, config.metaXattrLength)
	r.xattrPayload = newFairPayload(xattrData, 0, 100)
	r.xattrReadBuffer = newFairPayload(xattrTrashData, 0, 30)

	for _, op := range config.metaOps {
		metaOp := metaOpRequst(op.op)
		switch metaOp {
		case opSetattr, opStat, opWrite, opUnlink, opTruncate, opMknod, opRead, opSymlink, opHardLink, opRename, opSetxattr, opGetxattr, opRemovexattr, opListxattr:
			for i := 0; i < op.weight; i++ {
				r.ops = append(r.ops, metaOp)
			}
		default:
			log.Panicln("Unknown meta op ", op.op)
		}
	}
	r.opLen = len(r.ops)
	return &r
}

func getXattrName() string {
	return "user.goader.xattr-" + strconv.Itoa(rand.Intn(config.metaXattrKeys))
}

type s3UploaderRequester struct {
	client  fasthttp.Client
	timeout time.Duration
	method  func(r *Request) (time.Duration, error)
	state   *OPState
	s3      *s3.S3
	bucket  string
}

func newS3UploaderRequester(state *OPState, params s3Params) *s3UploaderRequester {
	svc := s3.New(session.Must(session.NewSession(&aws.Config{
		Credentials:      credentials.NewStaticCredentials(params.apiKey, params.secretKey, ""),
		Endpoint:         aws.String(params.endpoint),
		Region:           aws.String(params.region),
		S3ForcePathStyle: aws.Bool(true),
	})))

	requester := s3UploaderRequester{
		state:  state,
		s3:     svc,
		bucket: params.bucket,
	}

	requester.state = state
	if state.op == WRITE {
		uploader := s3manager.NewUploaderWithClient(svc, func(u *s3manager.Uploader) {
			u.PartSize = params.partSize         // 64MB per part
			u.LeavePartsOnError = false          // Don't delete the parts if the upload fails.
			u.Concurrency = params.ulConcurrency // s3manager.DefaultUploadConcurrency
		})

		requester.method = func(r *Request) (time.Duration, error) { return requester.upload(uploader, r) }
	} else {
		downloader := s3manager.NewDownloaderWithClient(svc, func(d *s3manager.Downloader) {
			d.PartSize = params.partSize
			d.Concurrency = params.dlConcurrency
		})

		requester.method = func(r *Request) (time.Duration, error) { return requester.download(downloader, r) }
	}

	if config.maxLatency == NotSet {
		requester.timeout = 60 * time.Second
	} else {
		requester.timeout = 5 * config.maxLatency
	}

	return &requester
}

func (r *s3UploaderRequester) upload(uploader *s3manager.Uploader, request *Request) (time.Duration, error) {
	upParams := &s3manager.UploadInput{
		Bucket: aws.String(r.bucket),
		Key:    aws.String(request.getUrl()),
		Body:   bytes.NewReader(requestersConfig.payloadGetter.Get()),
	}

	ctx := context.Background()
	var cancelFn func()
	if r.timeout > 0 {
		ctx, cancelFn = context.WithTimeout(ctx, r.timeout)
	}

	if cancelFn != nil {
		defer cancelFn()
	}

	start := time.Now()
	_, err := uploader.UploadWithContext(ctx, upParams)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == aws_req.CanceledErrorCode {
			// If the SDK can determine the request or retry delay was canceled
			// by a context the CanceledErrorCode error code will be returned.
			log.Printf("upload canceled due to timeout, %v\n", err)
			return 0, fmt.Errorf("S3 Upload canceled: %w", err)
		} else {
			log.Printf("failed to upload object, %v\n", err)
			return 0, fmt.Errorf("S3 Upload failed: %w", err)
		}
	}

	timeSpent := time.Since(start)

	return timeSpent, nil
}

func (r *s3UploaderRequester) download(downloader *s3manager.Downloader, request *Request) (time.Duration, error) {
	dParams := &s3.GetObjectInput{
		Bucket: aws.String(r.bucket),
		Key:    aws.String(request.getUrl()),
	}

	ctx := context.Background()
	var cancelFn func()
	if r.timeout > 0 {
		ctx, cancelFn = context.WithTimeout(ctx, r.timeout)
	}

	if cancelFn != nil {
		defer cancelFn()
	}

	buf := aws.NewWriteAtBuffer(requestersConfig.scratchBufferGetter.GetBuffer(int64(config.bodySize)))

	start := time.Now()
	_, err := downloader.DownloadWithContext(ctx, buf, dParams)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == aws_req.CanceledErrorCode {
			// If the SDK can determine the request or retry delay was canceled
			// by a context the CanceledErrorCode error code will be returned.
			log.Printf("upload canceled due to timeout, %v\n", err)
			return 0, fmt.Errorf("S3 Download canceled: %w", err)
		} else {
			log.Printf("failed to upload object, %v\n", err)
			return 0, fmt.Errorf("S3 Download failed: %w", err)
		}
	}
	timeSpent := time.Since(start)

	return timeSpent, nil
}

func (r *s3UploaderRequester) request(responses chan *Response, request *Request) {
	defer func() {
		if err := recover(); err != nil { //catch
			responses <- &Response{&Request{targeter: &BadUrlTarget{}, startTime: time.Now()}, time.Nanosecond,
				fmt.Errorf("Error: %s,%v", "panic:", err)}
			return
		}
	}()

	timeSpent, err := r.method(request)

	if err != nil {
		responses <- &Response{request, timeSpent,
			fmt.Errorf("Bad request: %s\n", err)}
		return
	}

	responses <- &Response{request, timeSpent, nil}
}
