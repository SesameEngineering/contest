// Copyright (c) Facebook, Inc. and its affiliates.
//
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package jobmanager

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/facebookincubator/contest/pkg/event/frameworkevent"
	"github.com/facebookincubator/contest/pkg/job"
	"github.com/facebookincubator/contest/pkg/statectx"
	"github.com/facebookincubator/contest/pkg/storage"
	"github.com/facebookincubator/contest/pkg/types"
)

func (jm *JobManager) resumeJobs(ctx statectx.Context) error {
	q, err := storage.BuildJobQuery(
		storage.QueryJobStates(job.JobStatePaused),
	)
	if err != nil {
		return fmt.Errorf("failed to build job query: %w", err)
	}
	pausedJobs, err := jm.jsm.ListJobs(q)
	if err != nil {
		return fmt.Errorf("failed to list paused jobs: %w", err)
	}
	log.Infof("Found %d paused jobs", len(pausedJobs))
	if len(pausedJobs) == 0 {
		return nil
	}
	for _, jobID := range pausedJobs {
		if err := jm.resumeJob(ctx, jobID); err != nil {
			log.Errorf("failed to resume job %d: %v, failing it", jobID, err)
			_ = jm.emitErrEvent(jobID, job.EventJobFailed, fmt.Errorf("failed to resume job %d: %w", jobID, err))
		}
	}
	return nil
}

func (jm *JobManager) resumeJob(ctx statectx.Context, jobID types.JobID) error {
	log.Debugf("attempting to resume job %d", jobID)
	results, err := jm.frameworkEvManager.Fetch(
		frameworkevent.QueryJobID(jobID),
		frameworkevent.QueryEventName(job.EventJobPaused),
	)
	if err != nil {
		return fmt.Errorf("failed to query resume state for job %d: %w", jobID, err)
	}
	if len(results) == 0 {
		return fmt.Errorf("no resume state found for job %d", jobID)
	}
	// Sort by EmitTime in descending order.
	sort.Slice(results, func(i, j int) bool { return results[i].EmitTime.After(results[j].EmitTime) })
	var resumeState job.PauseEventPayload
	if err := json.Unmarshal(*results[0].Payload, &resumeState); err != nil {
		return fmt.Errorf("invald resume state for job %d: %w", jobID, err)
	}
	if resumeState.Version != job.CurrentPauseEventPayloadVersion {
		return fmt.Errorf("incompatible resume state version (want %d, got %d)",
			job.CurrentPauseEventPayloadVersion, resumeState.Version)
	}
	req, err := jm.jsm.GetJobRequest(jobID)
	if err != nil {
		return fmt.Errorf("failed to retrieve job descriptor for %d: %w", jobID, err)
	}
	j, err := NewJob(jm.pluginRegistry, req.JobDescriptor)
	if err != nil {
		return fmt.Errorf("failed to create job %d: %w", jobID, err)
	}
	j.ID = jobID
	log.Debugf("running resumed job %d", j.ID)
	go jm.runJob(ctx, j, &resumeState)
	return nil
}
