import {
  Accordion,
  Button,
  Group,
  Loader,
  Paper,
  ScrollArea,
  SimpleGrid,
  Stack,
  Table,
  Text,
  Title,
} from "@mantine/core";
import { IconRefresh } from "@tabler/icons-react";

import { formatDateTime, formatNumber } from "../lib/format";
import type { ReportPayload } from "../types/api";

type ReportPanelProps = {
  report: ReportPayload | null;
  loading: boolean;
  onRefresh: () => void;
  disabled: boolean;
};

function formatOutlierExamples(
  examples: Array<{ row_index: number; value: number }>,
): string {
  if (examples.length === 0) {
    return "-";
  }

  return examples
    .map((example) => `row ${example.row_index}: ${formatNumber(example.value)}`)
    .join(" | ");
}

export function ReportPanel({ report, loading, onRefresh, disabled }: ReportPanelProps) {
  const nullEntries = Object.entries(report?.null_counts ?? {});
  const numericEntries = Object.entries(report?.numeric ?? {});
  const outlierEntries = Object.entries(report?.anomalies.outliers ?? {});

  return (
    <Paper className="surface-card" p="xl" radius="xl" withBorder>
      <Group justify="space-between" align="center" mb="md">
        <div>
          <Title order={3}>Report Explorer</Title>
          <Text c="dimmed" size="sm">
            Digest computed stats, null density, and anomaly traces.
          </Text>
        </div>
        <Button
          variant="subtle"
          leftSection={<IconRefresh size={16} />}
          onClick={onRefresh}
          disabled={disabled}
        >
          Refresh
        </Button>
      </Group>

      {loading ? (
        <Group justify="center" py="xl">
          <Loader color="teal" />
        </Group>
      ) : null}

      {!loading && !report ? (
        <Text c="dimmed" size="sm">
          No report loaded yet. Process a dataset, then click refresh.
        </Text>
      ) : null}

      {!loading && report ? (
        <Stack gap="lg">
          <SimpleGrid cols={{ base: 1, sm: 2, lg: 4 }}>
            <Paper className="metric-card" p="md" radius="lg" withBorder>
              <Text c="dimmed" size="xs" tt="uppercase">
                Dataset ID
              </Text>
              <Text fw={600} size="sm" mt={4}>
                {report.dataset_id.slice(0, 8)}
              </Text>
            </Paper>
            <Paper className="metric-card" p="md" radius="lg" withBorder>
              <Text c="dimmed" size="xs" tt="uppercase">
                Generated
              </Text>
              <Text fw={600} size="sm" mt={4}>
                {formatDateTime(report.generated_at)}
              </Text>
            </Paper>
            <Paper className="metric-card" p="md" radius="lg" withBorder>
              <Text c="dimmed" size="xs" tt="uppercase">
                Rows
              </Text>
              <Text fw={600} size="sm" mt={4}>
                {formatNumber(report.row_count)}
              </Text>
            </Paper>
            <Paper className="metric-card" p="md" radius="lg" withBorder>
              <Text c="dimmed" size="xs" tt="uppercase">
                Duplicates
              </Text>
              <Text fw={600} size="sm" mt={4}>
                {formatNumber(report.anomalies.duplicates_count)}
              </Text>
            </Paper>
          </SimpleGrid>

          <div>
            <Title order={5}>Null counts by field</Title>
            <ScrollArea h={180} mt="xs">
              <Table striped withTableBorder withColumnBorders highlightOnHover>
                <Table.Thead>
                  <Table.Tr>
                    <Table.Th>Field</Table.Th>
                    <Table.Th>Nulls</Table.Th>
                  </Table.Tr>
                </Table.Thead>
                <Table.Tbody>
                  {nullEntries.length > 0 ? (
                    nullEntries.map(([field, value]) => (
                      <Table.Tr key={field}>
                        <Table.Td>{field}</Table.Td>
                        <Table.Td>{formatNumber(value)}</Table.Td>
                      </Table.Tr>
                    ))
                  ) : (
                    <Table.Tr>
                      <Table.Td colSpan={2}>No fields found.</Table.Td>
                    </Table.Tr>
                  )}
                </Table.Tbody>
              </Table>
            </ScrollArea>
          </div>

          <div>
            <Title order={5}>Numeric field stats</Title>
            <ScrollArea h={220} mt="xs">
              <Table striped withTableBorder withColumnBorders highlightOnHover>
                <Table.Thead>
                  <Table.Tr>
                    <Table.Th>Field</Table.Th>
                    <Table.Th>Min</Table.Th>
                    <Table.Th>Mean</Table.Th>
                    <Table.Th>Max</Table.Th>
                  </Table.Tr>
                </Table.Thead>
                <Table.Tbody>
                  {numericEntries.length > 0 ? (
                    numericEntries.map(([field, stats]) => (
                      <Table.Tr key={field}>
                        <Table.Td>{field}</Table.Td>
                        <Table.Td>{formatNumber(stats.min)}</Table.Td>
                        <Table.Td>{formatNumber(stats.mean)}</Table.Td>
                        <Table.Td>{formatNumber(stats.max)}</Table.Td>
                      </Table.Tr>
                    ))
                  ) : (
                    <Table.Tr>
                      <Table.Td colSpan={4}>No numeric-only columns detected.</Table.Td>
                    </Table.Tr>
                  )}
                </Table.Tbody>
              </Table>
            </ScrollArea>
          </div>

          <div>
            <Title order={5}>Outliers (IQR)</Title>
            {outlierEntries.length > 0 ? (
              <Accordion variant="separated" mt="xs">
                {outlierEntries.map(([field, data]) => (
                  <Accordion.Item key={field} value={field}>
                    <Accordion.Control>{`${field} (${data.count})`}</Accordion.Control>
                    <Accordion.Panel>
                      <Text size="sm">{formatOutlierExamples(data.examples)}</Text>
                    </Accordion.Panel>
                  </Accordion.Item>
                ))}
              </Accordion>
            ) : (
              <Text c="dimmed" size="sm" mt="xs">
                No outliers detected.
              </Text>
            )}
          </div>
        </Stack>
      ) : null}
    </Paper>
  );
}
