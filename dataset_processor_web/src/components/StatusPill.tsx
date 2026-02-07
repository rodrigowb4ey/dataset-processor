import { Badge } from "@mantine/core";

import type { DatasetStatus, JobState } from "../types/api";

type Status = DatasetStatus | JobState;

const STATUS_STYLES: Record<Status, { color: string; label: string }> = {
  uploaded: { color: "cyan", label: "Uploaded" },
  processing: { color: "orange", label: "Processing" },
  done: { color: "teal", label: "Done" },
  failed: { color: "red", label: "Failed" },

  queued: { color: "indigo", label: "Queued" },
  started: { color: "yellow", label: "Started" },
  retrying: { color: "grape", label: "Retrying" },
  success: { color: "teal", label: "Success" },
  failure: { color: "red", label: "Failure" },
};

export function StatusPill({ status }: { status: Status }) {
  const style = STATUS_STYLES[status];
  return (
    <Badge color={style.color} radius="xl" variant="light">
      {style.label}
    </Badge>
  );
}
