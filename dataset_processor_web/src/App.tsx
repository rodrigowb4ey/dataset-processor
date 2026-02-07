import { useCallback, useEffect, useMemo, useState } from "react";

import {
  ActionIcon,
  Button,
  FileInput,
  Grid,
  Group,
  Loader,
  Paper,
  Progress,
  Select,
  Stack,
  Table,
  Text,
  TextInput,
  ThemeIcon,
  Title,
} from "@mantine/core";
import { useForm } from "@mantine/form";
import { notifications } from "@mantine/notifications";
import {
  IconBolt,
  IconDatabase,
  IconFileAnalytics,
  IconPlayerPlay,
  IconRefresh,
  IconUpload,
} from "@tabler/icons-react";

import { ReportPanel } from "./components/ReportPanel";
import { StatusPill } from "./components/StatusPill";
import { useJobPolling } from "./hooks/useJobPolling";
import {
  ApiError,
  enqueueDatasetProcessing,
  getDataset,
  getDatasetReport,
  getJob,
  listDatasets,
  listJobs,
  uploadDataset,
} from "./lib/api";
import { formatDateTime, formatFileSize } from "./lib/format";
import type { DatasetPublic, JobPublic, JobState, ReportPayload } from "./types/api";
import "./App.css";

type UploadFormValues = {
  name: string;
  file: File | null;
};

const ACTIVE_JOB_STATES: JobState[] = ["queued", "started", "retrying"];

function isActiveJobState(state: JobState): boolean {
  return ACTIVE_JOB_STATES.includes(state);
}

function showApiError(error: unknown, fallbackMessage: string): void {
  if (error instanceof ApiError) {
    notifications.show({
      title: `Request failed (${error.statusCode})`,
      message: error.requestId
        ? `${error.message} [request ${error.requestId}]`
        : error.message,
      color: "red",
    });
    return;
  }

  notifications.show({
    title: "Unexpected error",
    message: fallbackMessage,
    color: "red",
  });
}

function App() {
  const [datasets, setDatasets] = useState<DatasetPublic[]>([]);
  const [selectedDatasetId, setSelectedDatasetId] = useState<string | null>(null);
  const [selectedDataset, setSelectedDataset] = useState<DatasetPublic | null>(null);
  const [jobs, setJobs] = useState<JobPublic[]>([]);
  const [currentJob, setCurrentJob] = useState<JobPublic | null>(null);
  const [report, setReport] = useState<ReportPayload | null>(null);

  const [loadingDatasets, setLoadingDatasets] = useState(false);
  const [loadingDatasetDetails, setLoadingDatasetDetails] = useState(false);
  const [loadingJobs, setLoadingJobs] = useState(false);
  const [loadingReport, setLoadingReport] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [enqueueing, setEnqueueing] = useState(false);

  const [activeJobId, setActiveJobId] = useState<string | null>(null);

  const uploadForm = useForm<UploadFormValues>({
    initialValues: {
      name: "",
      file: null,
    },
    validate: {
      name: (value) =>
        value.trim().length > 0 ? null : "Please provide a friendly dataset name.",
      file: (value) => {
        if (!value) {
          return "Attach a CSV or JSON file.";
        }

        const allowedTypes = ["text/csv", "application/json"];
        const lowerName = value.name.toLowerCase();
        const validExtension = lowerName.endsWith(".csv") || lowerName.endsWith(".json");

        if (!allowedTypes.includes(value.type) && !validExtension) {
          return "Only CSV or JSON files are allowed.";
        }
        return null;
      },
    },
  });

  const loadJobs = useCallback(async () => {
    setLoadingJobs(true);
    try {
      const payload = await listJobs();
      setJobs(payload.jobs);
    } catch (error) {
      showApiError(error, "Failed to fetch jobs.");
    } finally {
      setLoadingJobs(false);
    }
  }, []);

  const loadReport = useCallback(async (datasetId: string, silentIfNotReady: boolean) => {
    setLoadingReport(true);
    try {
      const payload = await getDatasetReport(datasetId);
      setReport(payload);
    } catch (error) {
      if (error instanceof ApiError && error.statusCode === 404 && silentIfNotReady) {
        setReport(null);
      } else {
        showApiError(error, "Failed to fetch report.");
      }
    } finally {
      setLoadingReport(false);
    }
  }, []);

  const loadDatasetDetails = useCallback(
    async (datasetId: string, loadReportIfReady: boolean) => {
      setLoadingDatasetDetails(true);
      try {
        const dataset = await getDataset(datasetId);
        setSelectedDataset(dataset);

        if (dataset.latest_job_id) {
          const latestJob = await getJob(dataset.latest_job_id);
          setCurrentJob(latestJob);
          setActiveJobId(isActiveJobState(latestJob.state) ? latestJob.id : null);
        } else {
          setCurrentJob(null);
          setActiveJobId(null);
        }

        if (!dataset.report_available) {
          setReport(null);
        }

        if (dataset.report_available && loadReportIfReady) {
          await loadReport(dataset.id, true);
        }
      } catch (error) {
        showApiError(error, "Failed to fetch dataset details.");
      } finally {
        setLoadingDatasetDetails(false);
      }
    },
    [loadReport],
  );

  const loadDatasets = useCallback(
    async (preferredDatasetId: string | null) => {
      setLoadingDatasets(true);
      try {
        const payload = await listDatasets();
        setDatasets(payload.datasets);

        const preferredExists =
          preferredDatasetId !== null && payload.datasets.some((dataset) => dataset.id === preferredDatasetId);
        const nextId = preferredExists ? preferredDatasetId : payload.datasets[0]?.id ?? null;
        setSelectedDatasetId(nextId);
        if (nextId) {
          await loadDatasetDetails(nextId, true);
        } else {
          setSelectedDataset(null);
          setCurrentJob(null);
          setReport(null);
          setActiveJobId(null);
        }
      } catch (error) {
        showApiError(error, "Failed to load datasets.");
      } finally {
        setLoadingDatasets(false);
      }
    },
    [loadDatasetDetails],
  );

  useEffect(() => {
    void loadDatasets(null);
    void loadJobs();
  }, [loadDatasets, loadJobs]);

  const { job: polledJob, error: pollingError, isPolling } = useJobPolling(activeJobId, {
    enabled: activeJobId !== null,
    intervalMs: 1200,
  });

  useEffect(() => {
    if (!pollingError) {
      return;
    }

    showApiError(pollingError, "Failed to poll job status.");
  }, [pollingError]);

  useEffect(() => {
    if (!polledJob) {
      return;
    }

    setCurrentJob(polledJob);
    if (polledJob.state === "success" || polledJob.state === "failure") {
      setActiveJobId(null);
      void loadJobs();

      if (selectedDatasetId) {
        void loadDatasetDetails(selectedDatasetId, polledJob.state === "success");
      }
    }
  }, [loadDatasetDetails, loadJobs, polledJob, selectedDatasetId]);

  const datasetOptions = useMemo(
    () =>
      datasets.map((dataset) => ({
        value: dataset.id,
        label: `${dataset.name} (${dataset.status})`,
      })),
    [datasets],
  );

  const visibleJobs = useMemo(() => {
    if (!selectedDatasetId) {
      return jobs;
    }
    return jobs.filter((job) => job.dataset_id === selectedDatasetId);
  }, [jobs, selectedDatasetId]);

  const handleDatasetSelection = async (datasetId: string | null) => {
    setSelectedDatasetId(datasetId);
    setCurrentJob(null);
    setReport(null);
    setActiveJobId(null);

    if (!datasetId) {
      setSelectedDataset(null);
      return;
    }

    await loadDatasetDetails(datasetId, true);
  };

  const handleUpload = uploadForm.onSubmit(async (values) => {
    if (!values.file) {
      return;
    }

    setUploading(true);
    try {
      const payload = await uploadDataset(values.name.trim(), values.file);
      notifications.show({
        title: "Dataset uploaded",
        message: `Saved ${payload.name} (${formatFileSize(payload.size_bytes)}).`,
        color: "teal",
      });

      uploadForm.reset();
      await loadDatasets(payload.id);
      await loadJobs();
    } catch (error) {
      showApiError(error, "Upload failed.");
    } finally {
      setUploading(false);
    }
  });

  const handleEnqueue = async () => {
    if (!selectedDatasetId) {
      return;
    }

    setEnqueueing(true);
    try {
      const enqueuePayload = await enqueueDatasetProcessing(selectedDatasetId);
      const nextJob = await getJob(enqueuePayload.job_id);
      setCurrentJob(nextJob);
      setActiveJobId(isActiveJobState(nextJob.state) ? nextJob.id : null);

      notifications.show({
        title: "Processing request accepted",
        message: `Job ${enqueuePayload.job_id.slice(0, 8)} is ${enqueuePayload.state}.`,
        color: "blue",
      });

      await loadJobs();
      if (!isActiveJobState(nextJob.state)) {
        await loadDatasetDetails(selectedDatasetId, nextJob.state === "success");
      }
    } catch (error) {
      showApiError(error, "Failed to start processing.");
    } finally {
      setEnqueueing(false);
    }
  };

  return (
    <div className="app-shell">
      <div className="ambient-orb ambient-orb--one" />
      <div className="ambient-orb ambient-orb--two" />

      <div className="app-content">
        <Paper className="hero-card" p="xl" radius="xl" withBorder>
          <Group justify="space-between" align="flex-start">
            <Group align="flex-start" wrap="nowrap">
              <ThemeIcon variant="gradient" gradient={{ from: "teal", to: "cyan", deg: 135 }} size={54}>
                <IconDatabase size={28} />
              </ThemeIcon>
              <div>
                <Title order={1}>Dataset Processor Studio</Title>
                <Text c="dimmed" mt={4}>
                  Upload datasets, trigger async workers, track jobs, and inspect generated reports.
                </Text>
              </div>
            </Group>

            <ActionIcon
              variant="light"
              color="teal"
              size="lg"
              onClick={() => {
                void loadDatasets(selectedDatasetId);
                void loadJobs();
              }}
            >
              <IconRefresh size={18} />
            </ActionIcon>
          </Group>
        </Paper>

        <Grid mt="md" gutter="md">
          <Grid.Col span={{ base: 12, lg: 4 }}>
            <Stack>
              <Paper className="surface-card stagger-card" p="lg" radius="xl" withBorder>
                <Group gap="xs">
                  <IconUpload size={18} />
                  <Title order={3}>Upload Dataset</Title>
                </Group>
                <Text c="dimmed" size="sm" mt={4}>
                  Accepted content types: CSV and JSON.
                </Text>

                <form onSubmit={handleUpload}>
                  <Stack gap="sm" mt="md">
                    <TextInput
                      label="Dataset name"
                      placeholder="January sales"
                      required
                      {...uploadForm.getInputProps("name")}
                    />
                    <FileInput
                      label="File"
                      placeholder="Choose a dataset"
                      accept="text/csv,application/json"
                      required
                      clearable
                      {...uploadForm.getInputProps("file")}
                    />
                    <Button type="submit" loading={uploading} leftSection={<IconUpload size={16} />}>
                      Upload
                    </Button>
                  </Stack>
                </form>
              </Paper>

              <Paper className="surface-card stagger-card" p="lg" radius="xl" withBorder>
                <Group gap="xs">
                  <IconFileAnalytics size={18} />
                  <Title order={3}>Dataset Overview</Title>
                </Group>

                <Select
                  mt="md"
                  label="Select dataset"
                  placeholder={loadingDatasets ? "Loading datasets..." : "Pick one dataset"}
                  data={datasetOptions}
                  value={selectedDatasetId}
                  onChange={(value) => {
                    void handleDatasetSelection(value);
                  }}
                  searchable
                  clearable
                  rightSection={loadingDatasets ? <Loader size={14} /> : null}
                />

                {loadingDatasetDetails ? (
                  <Group justify="center" py="lg">
                    <Loader color="teal" size="sm" />
                  </Group>
                ) : null}

                {!loadingDatasetDetails && selectedDataset ? (
                  <Stack mt="md" gap={6}>
                    <Group justify="space-between">
                      <Text size="sm" fw={500}>
                        {selectedDataset.name}
                      </Text>
                      <StatusPill status={selectedDataset.status} />
                    </Group>
                    <Text size="sm" c="dimmed">
                      ID: {selectedDataset.id}
                    </Text>
                    <Text size="sm" c="dimmed">
                      Row count: {selectedDataset.row_count ?? "-"}
                    </Text>
                    <Text size="sm" c="dimmed">
                      Report available: {selectedDataset.report_available ? "yes" : "no"}
                    </Text>
                    {selectedDataset.error ? (
                      <Text size="sm" c="red">
                        Error: {selectedDataset.error}
                      </Text>
                    ) : null}
                  </Stack>
                ) : null}
              </Paper>

              <Paper className="surface-card stagger-card" p="lg" radius="xl" withBorder>
                <Group gap="xs">
                  <IconBolt size={18} />
                  <Title order={3}>Processing</Title>
                </Group>

                <Stack mt="md" gap="sm">
                  <Button
                    leftSection={<IconPlayerPlay size={16} />}
                    onClick={() => {
                      void handleEnqueue();
                    }}
                    disabled={!selectedDatasetId}
                    loading={enqueueing}
                  >
                    Start processing
                  </Button>
                  <Button
                    variant="light"
                    leftSection={<IconRefresh size={16} />}
                    onClick={() => {
                      if (!selectedDatasetId) {
                        return;
                      }
                      void loadDatasetDetails(selectedDatasetId, true);
                    }}
                    disabled={!selectedDatasetId}
                  >
                    Refresh dataset
                  </Button>
                </Stack>

                {currentJob ? (
                  <Stack mt="md" gap={6}>
                    <Group justify="space-between">
                      <Text size="sm" fw={500}>
                        Job {currentJob.id.slice(0, 8)}
                      </Text>
                      <StatusPill status={currentJob.state} />
                    </Group>

                    <Progress value={currentJob.progress} animated={isPolling} striped={isPolling} radius="xl" />

                    <Text size="sm" c="dimmed">
                      Progress: {currentJob.progress}%
                    </Text>
                    <Text size="sm" c="dimmed">
                      Queued: {formatDateTime(currentJob.queued_at)}
                    </Text>
                    <Text size="sm" c="dimmed">
                      Finished: {formatDateTime(currentJob.finished_at)}
                    </Text>
                    {currentJob.error ? (
                      <Text size="sm" c="red">
                        Error: {currentJob.error}
                      </Text>
                    ) : null}
                  </Stack>
                ) : null}
              </Paper>
            </Stack>
          </Grid.Col>

          <Grid.Col span={{ base: 12, lg: 8 }}>
            <ReportPanel
              report={report}
              loading={loadingReport}
              disabled={!selectedDatasetId}
              onRefresh={() => {
                if (!selectedDatasetId) {
                  return;
                }
                void loadReport(selectedDatasetId, false);
              }}
            />
          </Grid.Col>
        </Grid>

        <Paper className="surface-card" p="lg" radius="xl" withBorder mt="md">
          <Group justify="space-between" mb="sm">
            <Title order={3}>Jobs Ledger</Title>
            <Button
              variant="subtle"
              leftSection={<IconRefresh size={16} />}
              onClick={() => {
                void loadJobs();
              }}
              loading={loadingJobs}
            >
              Refresh jobs
            </Button>
          </Group>

          <Table striped highlightOnHover withTableBorder withColumnBorders>
            <Table.Thead>
              <Table.Tr>
                <Table.Th>Job</Table.Th>
                <Table.Th>Dataset</Table.Th>
                <Table.Th>Status</Table.Th>
                <Table.Th>Progress</Table.Th>
                <Table.Th>Queued at</Table.Th>
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {loadingJobs ? (
                <Table.Tr>
                  <Table.Td colSpan={5}>Loading jobs...</Table.Td>
                </Table.Tr>
              ) : null}

              {!loadingJobs && visibleJobs.length === 0 ? (
                <Table.Tr>
                  <Table.Td colSpan={5}>No jobs found for the current filter.</Table.Td>
                </Table.Tr>
              ) : null}

              {!loadingJobs
                ? visibleJobs.map((job) => (
                    <Table.Tr key={job.id}>
                      <Table.Td>{job.id.slice(0, 8)}</Table.Td>
                      <Table.Td>{job.dataset_id.slice(0, 8)}</Table.Td>
                      <Table.Td>
                        <StatusPill status={job.state} />
                      </Table.Td>
                      <Table.Td>{job.progress}%</Table.Td>
                      <Table.Td>{formatDateTime(job.queued_at)}</Table.Td>
                    </Table.Tr>
                  ))
                : null}
            </Table.Tbody>
          </Table>
        </Paper>
      </div>
    </div>
  );
}

export default App;
