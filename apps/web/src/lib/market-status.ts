export function formatMarketStatusLabel(status: string | null | undefined): string {
  if (status === null || status === undefined || status.trim().length === 0) {
    return "-";
  }

  return status === "Open" ? "Live" : status;
}
