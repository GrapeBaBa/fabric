/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package broadcast

import (
	"github.com/hyperledger/fabric/common/semaphore"
	"io"
	"runtime"
	"time"

	cb "github.com/hyperledger/fabric-protos-go/common"
	ab "github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/util"
	"github.com/hyperledger/fabric/orderer/common/msgprocessor"
	"github.com/pkg/errors"
)

var logger = flogging.MustGetLogger("orderer.common.broadcast")

//go:generate counterfeiter -o mock/channel_support_registrar.go --fake-name ChannelSupportRegistrar . ChannelSupportRegistrar

// ChannelSupportRegistrar provides a way for the Handler to look up the Support for a channel
type ChannelSupportRegistrar interface {
	// BroadcastChannelSupport returns the message channel header, whether the message is a config update
	// and the channel resources for a message or an error if the message is not a message which can
	// be processed directly (like CONFIG and ORDERER_TRANSACTION messages)
	BroadcastChannelSupport(msg *cb.Envelope) (*cb.ChannelHeader, bool, ChannelSupport, error)
}

//go:generate counterfeiter -o mock/channel_support.go --fake-name ChannelSupport . ChannelSupport

// ChannelSupport provides the backing resources needed to support broadcast on a channel
type ChannelSupport interface {
	msgprocessor.Processor
	Consenter
}

// Consenter provides methods to send messages through consensus
type Consenter interface {
	// Order accepts a message or returns an error indicating the cause of failure
	// It ultimately passes through to the consensus.Chain interface
	Order(env *cb.Envelope, configSeq uint64) error

	// Configure accepts a reconfiguration or returns an error indicating the cause of failure
	// It ultimately passes through to the consensus.Chain interface
	Configure(config *cb.Envelope, configSeq uint64) error

	// WaitReady blocks waiting for consenter to be ready for accepting new messages.
	// This is useful when consenter needs to temporarily block ingress messages so
	// that in-flight messages can be consumed. It could return error if consenter is
	// in erroneous states. If this blocking behavior is not desired, consenter could
	// simply return nil.
	WaitReady() error
}

// Handler is designed to handle connections from Broadcast AB gRPC service
type Handler struct {
	SupportRegistrar  ChannelSupportRegistrar
	Metrics           *Metrics
	IsMetricsDisabled bool
}

// Handle reads requests from a Broadcast stream, processes them, and returns the responses to the stream
func (bh *Handler) Handle(srv ab.AtomicBroadcast_BroadcastServer) error {
	addr := util.ExtractRemoteAddress(srv.Context())
	logger.Debugf("Starting new broadcast loop for %s", addr)
	respChan := make(chan chan *ab.BroadcastResponse, 60000)
	errChan := make(chan error, 60000)
	sem := semaphore.New(runtime.NumCPU())
	go func() {
		for {
			select {
			case respHolder := <-respChan:
				resp := <-respHolder
				err := srv.Send(resp)
				if resp.Status != cb.Status_SUCCESS {
					errChan <- err
				}
				if err != nil {
					logger.Warningf("Error sending to %s: %s", addr, err)
					errChan <- err
				}
			case <-srv.Context().Done():
				logger.Warningf("Error broadcast server context cancelled to %s: %s", addr, srv.Context().Err())
				return
			}
		}
	}()
	go func() {
		for {
			msg, err := srv.Recv()
			if err == io.EOF {
				logger.Debugf("Received EOF from %s, hangup", addr)
				errChan <- nil
				return
			}
			if err != nil {
				logger.Warningf("Error reading from %s: %s", addr, err)
				errChan <- err
				return
			}
			respHolder := make(chan *ab.BroadcastResponse, 1)
			respChan <- respHolder
			err = sem.Acquire(srv.Context())
			if err != nil {
				logger.Warningf("Error acquire permits %s: %s", addr, err)
				return
			}
			go bh.ProcessMessage(msg, addr, respHolder, sem)

			//select {
			//case <-doneChan:
			//	return
			//default:
			//	continue
			//}
			//err = srv.Send(resp)
			//if resp.Status != cb.Status_SUCCESS {
			//	return err
			//}
			//
			//if err != nil {
			//	logger.Warningf("Error sending to %s: %s", addr, err)
			//	return err
			//}
		}

	}()
	for {
		select {
		case err := <-errChan:
			return err
		case <-srv.Context().Done():
			return srv.Context().Err()
		}

		//msg, err := srv.Recv()
		//if err == io.EOF {
		//	logger.Debugf("Received EOF from %s, hangup", addr)
		//	return nil
		//}
		//if err != nil {
		//	logger.Warningf("Error reading from %s: %s", addr, err)
		//	return err
		//}
		//respHolder := make(chan *ab.BroadcastResponse, 1)
		//bh.RespChan <- respHolder
		//go bh.ProcessMessage(msg, addr, respHolder)

		//err = srv.Send(resp)
		//if resp.Status != cb.Status_SUCCESS {
		//	return err
		//}
		//
		//if err != nil {
		//	logger.Warningf("Error sending to %s: %s", addr, err)
		//	return err
		//}
	}

}

type MetricsTracker struct {
	ValidateStartTime time.Time
	EnqueueStartTime  time.Time
	ValidateDuration  time.Duration
	ChannelID         string
	TxType            string
	Metrics           *Metrics
	IsMetricsDisabled bool
}

func (mt *MetricsTracker) Record(resp *ab.BroadcastResponse) {
	if mt.IsMetricsDisabled {
		return
	}
	labels := []string{
		"status", resp.Status.String(),
		"channel", mt.ChannelID,
		"type", mt.TxType,
	}

	if mt.ValidateDuration == 0 {
		mt.EndValidate()
	}
	mt.Metrics.ValidateDuration.With(labels...).Observe(mt.ValidateDuration.Seconds())

	if mt.EnqueueStartTime != (time.Time{}) {
		enqueueDuration := time.Since(mt.EnqueueStartTime)
		mt.Metrics.EnqueueDuration.With(labels...).Observe(enqueueDuration.Seconds())
	}

	mt.Metrics.ProcessedCount.With(labels...).Add(1)
}

func (mt *MetricsTracker) BeginValidate() {
	if mt.IsMetricsDisabled {
		return
	}
	mt.ValidateStartTime = time.Now()
}

func (mt *MetricsTracker) EndValidate() {
	if mt.IsMetricsDisabled {
		return
	}
	mt.ValidateDuration = time.Since(mt.ValidateStartTime)
}

func (mt *MetricsTracker) BeginEnqueue() {
	if mt.IsMetricsDisabled {
		return
	}
	mt.EnqueueStartTime = time.Now()
}

// ProcessMessage validates and enqueues a single message
func (bh *Handler) ProcessMessage(msg *cb.Envelope, addr string, respHolder chan *ab.BroadcastResponse, sem semaphore.Semaphore) {
	defer sem.Release()
	tracker := &MetricsTracker{
		ChannelID:         "unknown",
		TxType:            "unknown",
		Metrics:           bh.Metrics,
		IsMetricsDisabled: bh.IsMetricsDisabled,
	}
	tracker.BeginValidate()

	chdr, isConfig, processor, err := bh.SupportRegistrar.BroadcastChannelSupport(msg)
	if chdr != nil {
		tracker.ChannelID = chdr.ChannelId
		tracker.TxType = cb.HeaderType(chdr.Type).String()
	}
	if err != nil {
		logger.Warningf("[channel: %s] Could not get message processor for serving %s: %s", tracker.ChannelID, addr, err)
		resp := &ab.BroadcastResponse{Status: cb.Status_BAD_REQUEST, Info: err.Error()}
		respHolder <- resp
		close(respHolder)
		tracker.Record(resp)
		return
	}

	if !isConfig {
		logger.Debugf("[channel: %s] Broadcast is processing normal message from %s with txid '%s' of type %s", chdr.ChannelId, addr, chdr.TxId, cb.HeaderType_name[chdr.Type])

		configSeq, err := processor.ProcessNormalMsg(msg)
		if err != nil {
			logger.Warningf("[channel: %s] Rejecting broadcast of normal message from %s because of error: %s", chdr.ChannelId, addr, err)
			resp := &ab.BroadcastResponse{Status: ClassifyError(err), Info: err.Error()}
			respHolder <- resp
			close(respHolder)
			tracker.Record(resp)
			return
		}
		tracker.EndValidate()

		tracker.BeginEnqueue()
		if err = processor.WaitReady(); err != nil {
			logger.Warningf("[channel: %s] Rejecting broadcast of message from %s with SERVICE_UNAVAILABLE: rejected by Consenter: %s", chdr.ChannelId, addr, err)
			resp := &ab.BroadcastResponse{Status: cb.Status_SERVICE_UNAVAILABLE, Info: err.Error()}
			respHolder <- resp
			close(respHolder)
			tracker.Record(resp)
			return
		}

		err = processor.Order(msg, configSeq)
		if err != nil {
			logger.Warningf("[channel: %s] Rejecting broadcast of normal message from %s with SERVICE_UNAVAILABLE: rejected by Order: %s", chdr.ChannelId, addr, err)
			resp := &ab.BroadcastResponse{Status: cb.Status_SERVICE_UNAVAILABLE, Info: err.Error()}
			respHolder <- resp
			close(respHolder)
			tracker.Record(resp)
			return
		}
	} else { // isConfig
		logger.Debugf("[channel: %s] Broadcast is processing config update message from %s", chdr.ChannelId, addr)

		config, configSeq, err := processor.ProcessConfigUpdateMsg(msg)
		if err != nil {
			logger.Warningf("[channel: %s] Rejecting broadcast of config message from %s because of error: %s", chdr.ChannelId, addr, err)
			resp := &ab.BroadcastResponse{Status: ClassifyError(err), Info: err.Error()}
			respHolder <- resp
			close(respHolder)
			tracker.Record(resp)
			return
		}
		tracker.EndValidate()

		tracker.BeginEnqueue()
		if err = processor.WaitReady(); err != nil {
			logger.Warningf("[channel: %s] Rejecting broadcast of message from %s with SERVICE_UNAVAILABLE: rejected by Consenter: %s", chdr.ChannelId, addr, err)
			resp := &ab.BroadcastResponse{Status: cb.Status_SERVICE_UNAVAILABLE, Info: err.Error()}
			respHolder <- resp
			close(respHolder)
			tracker.Record(resp)
			return
		}

		err = processor.Configure(config, configSeq)
		if err != nil {
			logger.Warningf("[channel: %s] Rejecting broadcast of config message from %s with SERVICE_UNAVAILABLE: rejected by Configure: %s", chdr.ChannelId, addr, err)
			resp := &ab.BroadcastResponse{Status: cb.Status_SERVICE_UNAVAILABLE, Info: err.Error()}
			respHolder <- resp
			close(respHolder)
			tracker.Record(resp)
			return
		}
	}

	logger.Debugf("[channel: %s] Broadcast has successfully enqueued message of type %s from %s", chdr.ChannelId, cb.HeaderType_name[chdr.Type], addr)

	resp := &ab.BroadcastResponse{Status: cb.Status_SUCCESS}
	respHolder <- resp
	close(respHolder)
	tracker.Record(resp)
	return
}

// ClassifyError converts an error type into a status code.
func ClassifyError(err error) cb.Status {
	switch errors.Cause(err) {
	case msgprocessor.ErrChannelDoesNotExist:
		return cb.Status_NOT_FOUND
	case msgprocessor.ErrPermissionDenied:
		return cb.Status_FORBIDDEN
	case msgprocessor.ErrMaintenanceMode:
		return cb.Status_SERVICE_UNAVAILABLE
	default:
		return cb.Status_BAD_REQUEST
	}
}
