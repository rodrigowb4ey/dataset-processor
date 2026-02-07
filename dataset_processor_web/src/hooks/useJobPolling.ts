import { useEffect, useState } from "react";

import { ApiError, getJob } from "../lib/api";
import type { JobPublic, JobState } from "../types/api";

const ACTIVE_STATES: JobState[] = ["queued", "started", "retrying"];

type UseJobPollingOptions = {
  enabled?: boolean;
  intervalMs?: number;
};

function isActiveState(state: JobState): boolean {
  return ACTIVE_STATES.includes(state);
}

export function useJobPolling(jobId: string | null, options?: UseJobPollingOptions) {
  const enabled = options?.enabled ?? true;
  const intervalMs = options?.intervalMs ?? 1200;

  const [job, setJob] = useState<JobPublic | null>(null);
  const [error, setError] = useState<ApiError | null>(null);

  useEffect(() => {
    if (!jobId || !enabled) {
      return;
    }

    let stopped = false;
    let timeoutId: number | null = null;

    const scheduleNext = () => {
      timeoutId = window.setTimeout(() => {
        void pollOnce();
      }, intervalMs);
    };

    const pollOnce = async () => {
      try {
        const nextJob = await getJob(jobId);
        if (stopped) {
          return;
        }

        setJob(nextJob);
        setError(null);

        if (isActiveState(nextJob.state)) {
          scheduleNext();
        }
      } catch (caughtError) {
        if (stopped) {
          return;
        }

        if (caughtError instanceof ApiError) {
          setError(caughtError);
        } else {
          setError(new ApiError(500, "Failed to refresh job status.", null));
        }
      }
    };

    void pollOnce();

    return () => {
      stopped = true;
      if (timeoutId !== null) {
        window.clearTimeout(timeoutId);
      }
    };
  }, [enabled, intervalMs, jobId]);

  const isPolling =
    Boolean(jobId) && enabled && error === null && (job === null || isActiveState(job.state));

  return { job, error, isPolling };
}
