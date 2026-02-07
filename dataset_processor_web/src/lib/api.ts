import type {
  DatasetList,
  DatasetPublic,
  DatasetUploadPublic,
  JobEnqueuePublic,
  JobList,
  JobPublic,
  ReportPayload,
} from "../types/api";

type ValidationIssue = {
  loc?: Array<string | number>;
  msg?: string;
};

const API_BASE_URL = (import.meta.env.VITE_API_BASE_URL ?? "/api").replace(/\/$/, "");

export class ApiError extends Error {
  statusCode: number;
  requestId?: string;
  detail: unknown;

  constructor(statusCode: number, message: string, detail: unknown, requestId?: string) {
    super(message);
    this.name = "ApiError";
    this.statusCode = statusCode;
    this.requestId = requestId;
    this.detail = detail;
  }
}

function isValidationIssue(value: unknown): value is ValidationIssue {
  if (!value || typeof value !== "object") {
    return false;
  }
  return "msg" in value;
}

export function normalizeErrorDetail(detail: unknown): string {
  if (typeof detail === "string" && detail.trim() !== "") {
    return detail;
  }

  if (Array.isArray(detail)) {
    const issues = detail
      .filter(isValidationIssue)
      .map((issue) => {
        const location = Array.isArray(issue.loc) ? issue.loc.join(".") : "request";
        const message = issue.msg ?? "invalid value";
        return `${location}: ${message}`;
      });
    if (issues.length > 0) {
      return issues.join("; ");
    }
  }

  return "Request failed. Please try again.";
}

function buildPath(path: string): string {
  const normalizedPath = path.startsWith("/") ? path : `/${path}`;
  return `${API_BASE_URL}${normalizedPath}`;
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(buildPath(path), init);
  const requestId = response.headers.get("X-Request-ID") ?? undefined;
  const contentType = response.headers.get("content-type") ?? "";

  let payload: unknown = null;
  if (contentType.includes("application/json")) {
    payload = await response.json().catch(() => null);
  } else {
    const textPayload = await response.text();
    payload = textPayload || null;
  }

  if (!response.ok) {
    let detail: unknown = payload;
    if (payload && typeof payload === "object" && "detail" in payload) {
      detail = (payload as { detail: unknown }).detail;
    }
    throw new ApiError(response.status, normalizeErrorDetail(detail), detail, requestId);
  }

  return payload as T;
}

export async function uploadDataset(name: string, file: File): Promise<DatasetUploadPublic> {
  const form = new FormData();
  form.append("name", name);
  form.append("file", file, file.name);

  return request<DatasetUploadPublic>("/datasets", {
    method: "POST",
    body: form,
  });
}

export async function listDatasets(): Promise<DatasetList> {
  return request<DatasetList>("/datasets");
}

export async function getDataset(datasetId: string): Promise<DatasetPublic> {
  return request<DatasetPublic>(`/datasets/${datasetId}`);
}

export async function enqueueDatasetProcessing(datasetId: string): Promise<JobEnqueuePublic> {
  return request<JobEnqueuePublic>(`/datasets/${datasetId}/process`, {
    method: "POST",
  });
}

export async function getDatasetReport(datasetId: string): Promise<ReportPayload> {
  return request<ReportPayload>(`/datasets/${datasetId}/report`);
}

export async function listJobs(): Promise<JobList> {
  return request<JobList>("/jobs");
}

export async function getJob(jobId: string): Promise<JobPublic> {
  return request<JobPublic>(`/jobs/${jobId}`);
}
