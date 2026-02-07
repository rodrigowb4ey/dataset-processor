export type DatasetStatus = "uploaded" | "processing" | "done" | "failed";

export type JobState = "queued" | "started" | "retrying" | "success" | "failure";

export interface DatasetUploadPublic {
  id: string;
  name: string;
  status: DatasetStatus;
  checksum_sha256: string;
  size_bytes: number;
}

export interface DatasetPublic {
  id: string;
  name: string;
  status: DatasetStatus;
  row_count: number | null;
  latest_job_id: string | null;
  report_available: boolean;
  error: string | null;
}

export interface DatasetList {
  datasets: DatasetPublic[];
}

export interface JobEnqueuePublic {
  job_id: string;
  dataset_id: string;
  state: JobState;
  progress: number;
}

export interface JobPublic {
  id: string;
  dataset_id: string;
  state: JobState;
  progress: number;
  error: string | null;
  queued_at: string;
  started_at: string | null;
  finished_at: string | null;
}

export interface JobList {
  jobs: JobPublic[];
}

export interface NumericFieldStats {
  min: number;
  max: number;
  mean: number;
}

export interface OutlierExample {
  row_index: number;
  value: number;
}

export interface OutlierSummary {
  count: number;
  examples: OutlierExample[];
}

export interface ReportPayload {
  dataset_id: string;
  generated_at: string;
  row_count: number;
  null_counts: Record<string, number>;
  numeric: Record<string, NumericFieldStats>;
  anomalies: {
    duplicates_count: number;
    outliers: Record<string, OutlierSummary>;
  };
}
