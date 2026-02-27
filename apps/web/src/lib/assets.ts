const DEFAULT_ASSET_QUALITY = 70;

function clampQuality(rawQuality: number): number {
  if (!Number.isFinite(rawQuality)) {
    return DEFAULT_ASSET_QUALITY;
  }
  return Math.max(1, Math.min(100, Math.round(rawQuality)));
}

function clampWidth(rawWidth: number): number {
  if (!Number.isFinite(rawWidth)) {
    return 1;
  }
  return Math.max(1, Math.round(rawWidth));
}

export function withAssetVariant(
  assetUrl: string | null | undefined,
  width: number,
  quality = DEFAULT_ASSET_QUALITY,
): string | null {
  if (!assetUrl) {
    return null;
  }

  const trimmed = assetUrl.trim();
  if (!trimmed) {
    return null;
  }

  const safeWidth = clampWidth(width);
  const safeQuality = clampQuality(quality);

  const [pathAndQuery, hash = ""] = trimmed.split("#", 2);
  const [path, query = ""] = pathAndQuery.split("?", 2);
  const params = new URLSearchParams(query);
  params.set("w", String(safeWidth));
  params.set("q", String(safeQuality));

  const queryString = params.toString();
  const rebuilt = `${path}${queryString ? `?${queryString}` : ""}`;
  return hash ? `${rebuilt}#${hash}` : rebuilt;
}
