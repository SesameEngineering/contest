// Copyright (c) Facebook, Inc. and its affiliates.
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package jobmanager

import (
	"fmt"
	"time"

	"github.com/facebookincubator/contest/pkg/api"
	"github.com/facebookincubator/contest/pkg/job"
	"github.com/facebookincubator/contest/pkg/statectx"
)

func (jm *JobManager) start(ctx statectx.Context, ev *api.Event) *api.EventResponse {
	msg := ev.Msg.(api.EventStartMsg)
	j, err := NewJob(jm.pluginRegistry, msg.JobDescriptor)
	if err != nil {
		return &api.EventResponse{Err: err}
	}
	// The job descriptor has been validated correctly, now use the JobRequestEmitter
	// interface to obtain a JobRequest object with a valid id
	request := job.Request{
		JobName:         j.Name,
		Requestor:       string(ev.Msg.Requestor()),
		ServerID:        ev.ServerID,
		RequestTime:     time.Now(),
		JobDescriptor:   msg.JobDescriptor,
		TestDescriptors: j.TestDescriptors,
	}
	jobID, err := jm.jsm.StoreJobRequest(&request)
	if err != nil {
		return &api.EventResponse{
			Requestor: ev.Msg.Requestor(),
			Err:       fmt.Errorf("could not create job request: %v", err)}
	}
	j.ID = jobID

	go jm.runJob(ctx, j, nil)

	return &api.EventResponse{
		JobID:     j.ID,
		Requestor: ev.Msg.Requestor(),
		Err:       nil,
		Status: &job.Status{
			Name:      j.Name,
			State:     string(job.EventJobStarted),
			StartTime: time.Now(),
		},
	}
}

func (jm *JobManager) runJob(ctx statectx.Context, j *job.Job, resumeState *job.PauseEventPayload) {
	jm.jobsWg.Add(1)
	defer jm.jobsWg.Done()

	if err := jm.emitEvent(j.ID, job.EventJobStarted); err != nil {
		log.Errorf("failed to emit event: %v", err)
		return
	}

	jobCtx, jobPause, jobCancel := statectx.WithParent(ctx)
	jm.jobsMu.Lock()
	jm.jobs[j.ID] = &jobInfo{job: j, pause: jobPause, cancel: jobCancel}
	jm.jobsMu.Unlock()

	start := time.Now()
	runReports, finalReports, resumeState, err := jm.jobRunner.Run(jobCtx, j, resumeState)
	duration := time.Since(start)
	log.Debugf("Job %d: runner finished, err %v", j.ID, err)
	switch err {
	case statectx.ErrCanceled:
		_ = jm.emitEvent(j.ID, job.EventJobCancelled)
		return
	case statectx.ErrPaused:
		if err := jm.emitEventPayload(j.ID, job.EventJobPaused, resumeState); err != nil {
			_ = jm.emitErrEvent(j.ID, job.EventJobPauseFailed, fmt.Errorf("Job %+v failed pausing: %v", j, err))
		} else {
			log.Infof("Successfully paused job %d (run %d, %d targets)", j.ID, resumeState.RunID, len(resumeState.Targets))
			log.Debugf("Job %d pause state: %+v", j.ID, resumeState)
		}
		return
	}
	select {
	case <-ctx.Paused():
		// We were asked to pause but failed to do so.
		pauseErr := fmt.Errorf("Job %+v failed pausing: %v", j, err)
		log.Errorf("%v", pauseErr)
		_ = jm.emitErrEvent(j.ID, job.EventJobPauseFailed, pauseErr)
		return
	default:
	}
	log.Infof("Job %d finished", j.ID)

	// store job report before emitting the job status event, to avoid a
	// race condition when waiting on a job status where the event is marked
	// as completed but no report exists.
	jobReport := job.JobReport{
		JobID:        j.ID,
		RunReports:   runReports,
		FinalReports: finalReports,
	}
	if storageErr := jm.jsm.StoreJobReport(&jobReport); storageErr != nil {
		log.Warningf("Could not emit job report: %v", storageErr)
	}
	// at this point it is safe to emit the job status event. Note: this is
	// checking `err` from the `jm.jobRunner.Run()` call above.
	if err != nil {
		_ = jm.emitErrEvent(j.ID, job.EventJobFailed, fmt.Errorf("Job %d failed after %s: %w", j.ID, duration, err))
	} else {
		log.Infof("Job %+v completed after %s", j, duration)
		err = jm.emitEvent(j.ID, job.EventJobCompleted)
		if err != nil {
			log.Warningf("event emission failed: %v", err)
		}
	}
}
