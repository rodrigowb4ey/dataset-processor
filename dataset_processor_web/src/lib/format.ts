const numberFormatter = new Intl.NumberFormat("en-US", {
  maximumFractionDigits: 2,
});

const dateFormatter = new Intl.DateTimeFormat("en-US", {
  dateStyle: "medium",
  timeStyle: "short",
});

export function formatNumber(value: number): string {
  return numberFormatter.format(value);
}

export function formatDateTime(isoValue: string | null): string {
  if (!isoValue) {
    return "-";
  }

  const timestamp = Date.parse(isoValue);
  if (Number.isNaN(timestamp)) {
    return "-";
  }
  return dateFormatter.format(new Date(timestamp));
}

export function formatFileSize(valueBytes: number): string {
  if (valueBytes < 1024) {
    return `${valueBytes} B`;
  }

  const kb = valueBytes / 1024;
  if (kb < 1024) {
    return `${numberFormatter.format(kb)} KB`;
  }

  const mb = kb / 1024;
  if (mb < 1024) {
    return `${numberFormatter.format(mb)} MB`;
  }

  const gb = mb / 1024;
  return `${numberFormatter.format(gb)} GB`;
}
