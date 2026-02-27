import React from "react";
import { RiArrowDownSLine, RiArrowUpSLine, RiExpandUpDownLine, RiRefreshLine } from "@remixicon/react";
import * as RechartsPrimitive from "recharts";

import { useSessionContext } from "@/app/session";
import { describeError } from "@/app/session";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { ChartContainer, ChartTooltip, ChartTooltipContent, type ChartConfig } from "@/components/ui/chart";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { Input } from "@/components/ui/input";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import * as api from "@/lib/api";
import { withAssetVariant } from "@/lib/assets";
import { withRetry } from "@/lib/retry";
import { cn } from "@/lib/utils";
import { toast } from "sonner";

type TradingPageProps = {
  marketId: string;
};

const ORDERBOOK_OPEN_STORAGE_KEY = "pebble-orderbook-open";
const CHART_RANGE_STORAGE_KEY = "pebble-chart-range";
const TRADING_OPEN_ORDERS_PAGE_SIZE_STORAGE_KEY = "pebble-trading-open-orders-page-size";
const TRADING_FILLS_HISTORY_PAGE_SIZE_STORAGE_KEY = "pebble-trading-fills-history-page-size";
type ChartRange = api.MarketChartRange;
const CHART_RANGES: ReadonlyArray<ChartRange> = ["1H", "6H", "1D", "1W", "1M", "ALL"];
const TRADING_PAGE_SIZES = [10, 20, 50] as const;
const CHART_ALL_SAMPLE_POINTS = 720;
const CHART_STREAM_BATCH_MS = 180;
const CHART_STREAM_MAX_BUFFERED_FILLS = 1024;

type ChartBinSpec = {
  samplePoints: number;
  binSizeMs: number;
};

type NormalizedChartFill = {
  fill_sequence: number;
  price_ticks: number;
  matched_at_ms: number;
};
type SampledChartPoint = {
  ts: number;
  price_ticks: number | null;
};

type NormalizedChartState = {
  fills: NormalizedChartFill[];
  previousFill: NormalizedChartFill | null;
  samples: SampledChartPoint[];
};

type SortDirection = "asc" | "desc";

type OpenOrdersSortKey = "orderId" | "side" | "outcome" | "price" | "remaining";
type MarketFillsSortKey = "fillId" | "outcome" | "price" | "quantity" | "matchedAt";
type MyFillsSortKey = "fillId" | "role" | "outcome" | "price" | "quantity" | "matchedAt";
type MyOrdersHistorySortKey = "orderId" | "status" | "side" | "outcome" | "price" | "quantity" | "updatedAt";
type FillsHistoryTab = "market-fills" | "my-fills" | "my-orders";

function readStoredPageSize(
  storageKey: string,
  fallback: (typeof TRADING_PAGE_SIZES)[number],
): (typeof TRADING_PAGE_SIZES)[number] {
  if (typeof window === "undefined") {
    return fallback;
  }

  const rawValue = window.localStorage.getItem(storageKey);
  const parsed = Number(rawValue);
  if ((TRADING_PAGE_SIZES as ReadonlyArray<number>).includes(parsed)) {
    return parsed as (typeof TRADING_PAGE_SIZES)[number];
  }
  return fallback;
}

const USD_FORMATTER = new Intl.NumberFormat(undefined, {
  style: "currency",
  currency: "USD",
  minimumFractionDigits: 0,
  maximumFractionDigits: 2,
});

function formatShares(value: number | null | undefined): string {
  if (value === null || value === undefined) {
    return "-";
  }

  return value.toLocaleString();
}

function formatUsdFromCents(value: number | null | undefined): string {
  if (value === null || value === undefined) {
    return "-";
  }

  return USD_FORMATTER.format(value / 100);
}

function formatTime(value: string | null | undefined): string {
  if (!value) {
    return "-";
  }

  const millis = Date.parse(value);
  if (Number.isNaN(millis)) {
    return value;
  }

  return new Date(millis).toLocaleString();
}

function compareText(left: string, right: string): number {
  return left.localeCompare(right, undefined, { sensitivity: "base" });
}

function compareNullableNumber(
  left: number | null | undefined,
  right: number | null | undefined,
): number {
  if (left == null && right == null) {
    return 0;
  }
  if (left == null) {
    return 1;
  }
  if (right == null) {
    return -1;
  }
  return left - right;
}

function compareTimestampStrings(left: string | null | undefined, right: string | null | undefined): number {
  const leftTimestamp = left ? parseTimestampMs(left) : null;
  const rightTimestamp = right ? parseTimestampMs(right) : null;
  return compareNullableNumber(leftTimestamp, rightTimestamp);
}

function renderSortIcon<T extends string>(
  state: { key: T; direction: SortDirection },
  key: T,
): React.ReactNode {
  if (state.key !== key) {
    return <RiExpandUpDownLine className="size-3 text-muted-foreground" />;
  }
  if (state.direction === "asc") {
    return <RiArrowUpSLine className="size-3" />;
  }
  return <RiArrowDownSLine className="size-3" />;
}

function chartBinSpecForRange(range: ChartRange): ChartBinSpec | null {
  switch (range) {
    case "1H":
      return { samplePoints: 60, binSizeMs: 60_000 };
    case "6H":
      return { samplePoints: 360, binSizeMs: 60_000 };
    case "1D":
      return { samplePoints: 288, binSizeMs: 5 * 60_000 };
    case "1W":
      return { samplePoints: 336, binSizeMs: 30 * 60_000 };
    case "1M":
      return { samplePoints: 240, binSizeMs: 3 * 60 * 60_000 };
    case "ALL":
      return null;
  }
}

function chartSamplePointsForRange(range: ChartRange): number {
  return chartBinSpecForRange(range)?.samplePoints ?? CHART_ALL_SAMPLE_POINTS;
}

function isChartRange(value: string | null): value is ChartRange {
  return value !== null && (CHART_RANGES as ReadonlyArray<string>).includes(value);
}

function parseTimestampMs(value: string): number | null {
  const parsed = Date.parse(value);
  if (Number.isNaN(parsed)) {
    return null;
  }
  return parsed;
}

function xAxisDateTimeFormatOptions(
  range: ChartRange,
  windowDurationMs: number,
): Intl.DateTimeFormatOptions {
  if (range === "1H" || range === "6H") {
    return { hour: "numeric", minute: "2-digit" };
  }
  if (range === "1D") {
    return { hour: "numeric", month: "short", day: "numeric" };
  }
  if (range === "ALL" && windowDurationMs <= 2 * 24 * 60 * 60 * 1000) {
    return { hour: "numeric", minute: "2-digit" };
  }
  return { month: "short", day: "numeric" };
}

function buildChanceTicks(min: number, max: number, segments = 4): number[] {
  if (segments <= 0 || min >= max) {
    return [Math.max(0, Math.min(100, Math.round(min)))];
  }

  const values = new Set<number>();
  for (let i = 0; i <= segments; i += 1) {
    const raw = min + ((max - min) * i) / segments;
    const rounded = Math.max(0, Math.min(100, Math.round(raw)));
    values.add(rounded);
  }

  if (!values.has(max)) {
    values.add(max);
  }

  return [...values].sort((left, right) => left - right);
}

function normalizeChartState(snapshot: api.MarketChartSnapshotView | null): NormalizedChartState {
  if (!snapshot) {
    return { fills: [], previousFill: null, samples: [] };
  }

  const fills = snapshot.fills
    .map((fill) => {
      const matchedAtMs = parseTimestampMs(fill.matched_at);
      if (matchedAtMs === null) {
        return null;
      }
      return {
        fill_sequence: fill.fill_sequence,
        price_ticks: fill.price_ticks,
        matched_at_ms: matchedAtMs,
      };
    })
    .filter((fill): fill is NormalizedChartFill => fill !== null)
    .sort((left, right) => left.fill_sequence - right.fill_sequence);

  const previousFill = snapshot.previous_fill
    ? (() => {
        const matchedAtMs = parseTimestampMs(snapshot.previous_fill.matched_at);
        if (matchedAtMs === null) {
          return null;
        }
        return {
          fill_sequence: snapshot.previous_fill.fill_sequence,
          price_ticks: snapshot.previous_fill.price_ticks,
          matched_at_ms: matchedAtMs,
        } satisfies NormalizedChartFill;
      })()
    : null;

  const rawSamples = Array.isArray(snapshot.samples) ? snapshot.samples : [];
  const samples = rawSamples
    .map((sample) => {
      const tsMs = parseTimestampMs(sample.ts);
      if (tsMs === null) {
        return null;
      }
      return {
        ts: tsMs,
        price_ticks: sample.price_ticks,
      } satisfies SampledChartPoint;
    })
    .filter((sample): sample is SampledChartPoint => sample !== null)
    .sort((left, right) => left.ts - right.ts);

  return { fills, previousFill, samples };
}

type MergedSamplesState = {
  samples: api.MarketChartSamplePointView[];
  startAt: string;
  endAt: string;
};

function inferBinSizeMsFromSamples(samples: api.MarketChartSamplePointView[]): number | null {
  if (samples.length < 2) {
    return null;
  }

  let previousTs: number | null = null;
  for (const sample of samples) {
    const currentTs = parseTimestampMs(sample.ts);
    if (currentTs === null) {
      continue;
    }
    if (previousTs !== null) {
      const diff = currentTs - previousTs;
      if (diff > 0) {
        return diff;
      }
    }
    previousTs = currentTs;
  }

  return null;
}

function trimAllRangeSamples(
  samples: api.MarketChartSamplePointView[],
  maxSamples: number,
): { samples: api.MarketChartSamplePointView[]; dropped: number } {
  if (maxSamples <= 0 || samples.length <= maxSamples) {
    return { samples, dropped: 0 };
  }

  const dropped = samples.length - maxSamples;
  return {
    samples: samples.slice(dropped),
    dropped,
  };
}

function normalizeAllRangeSnapshot(
  snapshot: api.MarketChartSnapshotView,
): api.MarketChartSnapshotView {
  if (snapshot.range !== "ALL") {
    return snapshot;
  }

  const maxSamples = chartSamplePointsForRange("ALL");
  const trimmed = trimAllRangeSamples(snapshot.samples, maxSamples);
  if (trimmed.dropped === 0) {
    return snapshot;
  }

  const nextStartAt = trimmed.samples[0]?.ts ?? snapshot.start_at;
  return {
    ...snapshot,
    start_at: nextStartAt,
    sample_points: trimmed.samples.length,
    samples: trimmed.samples,
  };
}

function mergeSnapshotSamplesWithUpdates(
  current: api.MarketChartSnapshotView,
  fills: api.MarketChartUpdateFillView[],
  range: ChartRange,
): MergedSamplesState {
  if (current.samples.length === 0) {
    return {
      samples: current.samples,
      startAt: current.start_at,
      endAt: current.end_at,
    };
  }

  const binSpec = chartBinSpecForRange(range);

  if (!binSpec) {
    if (range !== "ALL") {
      return {
        samples: current.samples,
        startAt: current.start_at,
        endAt: current.end_at,
      };
    }

    const startMs = parseTimestampMs(current.start_at);
    const endMs = parseTimestampMs(current.end_at);
    const inferredBinSizeMs = inferBinSizeMsFromSamples(current.samples);
    if (startMs === null || endMs === null || inferredBinSizeMs === null) {
      if (fills.length === 0) {
        return {
          samples: current.samples,
          startAt: current.start_at,
          endAt: current.end_at,
        };
      }

      const nextSamples = current.samples.map((sample) => ({ ...sample }));
      const lastFill = fills.at(-1);
      if (lastFill && nextSamples.length > 0) {
        const lastSample = nextSamples[nextSamples.length - 1];
        if (lastSample) {
          nextSamples[nextSamples.length - 1] = {
            ...lastSample,
            price_ticks: lastFill.price_ticks,
          };
        }
      }

      return {
        samples: nextSamples,
        startAt: current.start_at,
        endAt: current.end_at,
      };
    }

    const maxSamples = chartSamplePointsForRange("ALL");
    let nextSamples: api.MarketChartSamplePointView[] | null = null;
    let nextStartMs = startMs;
    let nextEndMs = endMs;
    let latestKnownPrice = current.samples[current.samples.length - 1]?.price_ticks ?? null;

    const ensureSamples = (): api.MarketChartSamplePointView[] => {
      if (nextSamples === null) {
        const cloned = current.samples.map((sample) => ({ ...sample }));
        const trimmed = trimAllRangeSamples(cloned, maxSamples);
        nextStartMs += trimmed.dropped * inferredBinSizeMs;
        nextSamples = trimmed.samples;
      }
      return nextSamples;
    };

    const trimToMaxSamples = (samples: api.MarketChartSamplePointView[]) => {
      while (samples.length > maxSamples) {
        samples.shift();
        nextStartMs += inferredBinSizeMs;
      }
    };

    const advanceToTimestamp = (targetMs: number) => {
      if (targetMs < nextEndMs + inferredBinSizeMs) {
        return;
      }

      const binsToAdvance = Math.floor((targetMs - nextEndMs) / inferredBinSizeMs);
      for (let advance = 0; advance < binsToAdvance; advance += 1) {
        const samples = ensureSamples();
        nextEndMs += inferredBinSizeMs;
        const carryPrice = samples[samples.length - 1]?.price_ticks ?? latestKnownPrice;
        latestKnownPrice = carryPrice;
        samples.push({
          ts: new Date(nextEndMs).toISOString(),
          price_ticks: carryPrice,
        });
        trimToMaxSamples(samples);
      }
    };

    if (fills.length === 0) {
      advanceToTimestamp(Date.now());
    }

    for (const fill of fills) {
      const fillMs = Date.parse(fill.matched_at);
      if (Number.isNaN(fillMs)) {
        continue;
      }

      advanceToTimestamp(fillMs);

      if (fillMs < nextStartMs) {
        continue;
      }

      const samples = ensureSamples();
      const index = Math.floor((fillMs - nextStartMs) / inferredBinSizeMs);
      if (index < 0 || index >= samples.length) {
        continue;
      }

      const sample = samples[index];
      if (!sample) {
        continue;
      }

      latestKnownPrice = fill.price_ticks;
      samples[index] = {
        ...sample,
        price_ticks: fill.price_ticks,
      };
    }

    advanceToTimestamp(Date.now());

    const startAt = nextStartMs === startMs ? current.start_at : new Date(nextStartMs).toISOString();
    const endAt = nextEndMs === endMs ? current.end_at : new Date(nextEndMs).toISOString();
    return {
      samples: nextSamples ?? current.samples,
      startAt,
      endAt,
    };
  }

  const startMs = parseTimestampMs(current.start_at);
  const endMs = parseTimestampMs(current.end_at);
  if (startMs === null || endMs === null) {
    return {
      samples: current.samples,
      startAt: current.start_at,
      endAt: current.end_at,
    };
  }

  let nextSamples: api.MarketChartSamplePointView[] | null = null;
  let nextStartMs = startMs;
  let nextEndMs = endMs;
  let latestKnownPrice = current.samples[current.samples.length - 1]?.price_ticks ?? 50;

  const ensureSamples = (): api.MarketChartSamplePointView[] => {
    if (nextSamples === null) {
      nextSamples = current.samples.map((sample) => ({ ...sample }));
    }
    return nextSamples;
  };

  const advanceToTimestamp = (targetMs: number) => {
    if (targetMs < nextEndMs + binSpec.binSizeMs) {
      return;
    }

    const binsToAdvance = Math.floor((targetMs - nextEndMs) / binSpec.binSizeMs);
    for (let advance = 0; advance < binsToAdvance; advance += 1) {
      const samples = ensureSamples();
      samples.shift();
      nextStartMs += binSpec.binSizeMs;
      nextEndMs += binSpec.binSizeMs;
      const carryPrice = samples[samples.length - 1]?.price_ticks ?? latestKnownPrice;
      latestKnownPrice = carryPrice;
      samples.push({
        ts: new Date(nextEndMs).toISOString(),
        price_ticks: carryPrice,
      });
    }
  };

  if (fills.length === 0) {
    advanceToTimestamp(Date.now());
  }

  for (const fill of fills) {
    const fillMs = Date.parse(fill.matched_at);
    if (Number.isNaN(fillMs)) {
      continue;
    }

    advanceToTimestamp(fillMs);

    if (fillMs < nextEndMs) {
      continue;
    }

    const samples = ensureSamples();
    const lastSample = samples[samples.length - 1];
    if (!lastSample) {
      continue;
    }

    latestKnownPrice = fill.price_ticks;
    samples[samples.length - 1] = {
      ...lastSample,
      price_ticks: fill.price_ticks,
    };
  }
  advanceToTimestamp(Date.now());

  const startAt = nextStartMs === startMs ? current.start_at : new Date(nextStartMs).toISOString();
  const endAt = nextEndMs === endMs ? current.end_at : new Date(nextEndMs).toISOString();
  return {
    samples: nextSamples ?? current.samples,
    startAt,
    endAt,
  };
}

function mergeChartSnapshotWithUpdates(
  current: api.MarketChartSnapshotView | null,
  payload: api.MarketChartUpdatesEvent,
  range: ChartRange,
): api.MarketChartSnapshotView | null {
  if (!current || current.market_id !== payload.market_id) {
    return current;
  }

  const mergedSampleState = mergeSnapshotSamplesWithUpdates(current, payload.fills, range);
  const samplesUnchanged =
    mergedSampleState.samples === current.samples &&
    mergedSampleState.startAt === current.start_at &&
    mergedSampleState.endAt === current.end_at;
  if (samplesUnchanged) {
    return current;
  }

  const nextLastFillSequence =
    payload.fills.length === 0 ? current.last_fill_sequence : payload.last_fill_sequence;

  return {
    ...current,
    start_at: mergedSampleState.startAt,
    end_at: mergedSampleState.endAt,
    fills: [],
    samples: mergedSampleState.samples,
    last_fill_sequence: nextLastFillSequence,
  };
}

type LooseComponent = React.ComponentType<Record<string, unknown>>;
const RechartsLineChart = RechartsPrimitive.LineChart as unknown as LooseComponent;
const RechartsLine = RechartsPrimitive.Line as unknown as LooseComponent;
const RechartsXAxis = RechartsPrimitive.XAxis as unknown as LooseComponent;
const RechartsYAxis = RechartsPrimitive.YAxis as unknown as LooseComponent;
const RechartsCartesianGrid = RechartsPrimitive.CartesianGrid as unknown as LooseComponent;
const RechartsTooltip = ChartTooltip as unknown as LooseComponent;

function ChartEndpointPulseDot(dotProps: Record<string, unknown>) {
  const cx = Number(dotProps.cx);
  const cy = Number(dotProps.cy);
  if (!Number.isFinite(cx) || !Number.isFinite(cy)) {
    return null;
  }

  return (
    <g pointerEvents="none">
      <circle cx={cx} cy={cy} r={7} fill="var(--color-price_ticks)" opacity={0.22}>
        <animate attributeName="r" values="7;14" dur="1.8s" repeatCount="indefinite" />
        <animate attributeName="opacity" values="0.28;0" dur="1.8s" repeatCount="indefinite" />
      </circle>
      <circle cx={cx} cy={cy} r={4.2} fill="var(--color-price_ticks)" />
    </g>
  );
}

function formatTicks(value: number | null | undefined): string {
  if (value === null || value === undefined) {
    return "-";
  }

  return `${value}c`;
}

type OutcomeTone = "yes" | "no" | "neutral";

function getOutcomeTone(outcome: string, outcomes: string[]): OutcomeTone {
  const normalized = outcome.trim().toLowerCase();
  if (normalized === "yes" || normalized === "true") {
    return "yes";
  }
  if (normalized === "no" || normalized === "false") {
    return "no";
  }

  const index = outcomes.findIndex((entry) => entry === outcome);
  if (index === 0) {
    return "yes";
  }
  if (index === 1) {
    return "no";
  }
  return "neutral";
}

function getOutcomeSelectorClass(tone: OutcomeTone, active: boolean): string {
  if (tone === "yes") {
    return active
      ? "!border-emerald-600 !bg-emerald-600 !text-white hover:!bg-emerald-500"
      : "border-emerald-500/45 bg-transparent text-emerald-800 hover:bg-emerald-500/10 dark:text-emerald-200";
  }
  if (tone === "no") {
    return active
      ? "!border-red-600 !bg-red-600 !text-white hover:!bg-red-500"
      : "border-red-500/45 bg-transparent text-red-800 hover:bg-red-500/10 dark:text-red-200";
  }
  return active
    ? "!border-primary !bg-primary !text-primary-foreground hover:!bg-primary/90"
    : "border-border bg-muted text-foreground hover:bg-muted/80";
}

function selectChartOutcome(outcomes: string[]): string | null {
  if (outcomes.length === 0) {
    return null;
  }

  const explicitYes = outcomes.find((entry) => {
    const normalized = entry.trim().toLowerCase();
    return normalized === "yes" || normalized === "true";
  });

  return explicitYes ?? outcomes[0] ?? null;
}

export function TradingPage(props: TradingPageProps) {
  const { apiBase, auth, session } = useSessionContext();

  const [market, setMarket] = React.useState<api.MarketRow | null>(null);
  const [marketMetadata, setMarketMetadata] = React.useState<api.MarketMetadataView | null>(null);
  const [marketStats, setMarketStats] = React.useState<api.PublicMarketStatsView | null>(null);
  const [book, setBook] = React.useState<api.MarketOrderBookView | null>(null);
  const [marketFills, setMarketFills] = React.useState<api.MarketFillView[]>([]);
  const [myOrders, setMyOrders] = React.useState<api.OrderRowView[]>([]);
  const [myFills, setMyFills] = React.useState<api.FillRowView[]>([]);

  const [status, setStatus] = React.useState<string>("loading...");
  const [adminActionStatus, setAdminActionStatus] = React.useState<string>("idle");

  const [side, setSide] = React.useState<api.OrderSide>("Buy");
  const [orderType, setOrderType] = React.useState<api.OrderType>("Limit");
  const [outcome, setOutcome] = React.useState<string>("");
  const [priceTicks, setPriceTicks] = React.useState<string>("50");
  const [quantityMinor, setQuantityMinor] = React.useState<string>("1");
  const [resolveOutcome, setResolveOutcome] = React.useState<string>("");
  const [chartRange, setChartRange] = React.useState<ChartRange>(() => {
    if (typeof window === "undefined") {
      return "ALL";
    }

    const storedValue = window.localStorage.getItem(CHART_RANGE_STORAGE_KEY);
    return isChartRange(storedValue) ? storedValue : "ALL";
  });
  const [chartSnapshot, setChartSnapshot] = React.useState<api.MarketChartSnapshotView | null>(null);
  const [orderBookOpen, setOrderBookOpen] = React.useState<boolean>(() => {
    if (typeof window === "undefined") {
      return false;
    }

    return window.localStorage.getItem(ORDERBOOK_OPEN_STORAGE_KEY) === "1";
  });
  const [orderBookOutcome, setOrderBookOutcome] = React.useState<string>("");

  const [placingOrder, setPlacingOrder] = React.useState<boolean>(false);
  const [cancellingOrderId, setCancellingOrderId] = React.useState<string | null>(null);
  const [adminPendingAction, setAdminPendingAction] = React.useState<string | null>(null);
  const [manualRefreshPending, setManualRefreshPending] = React.useState<boolean>(false);
  const [openOrdersSort, setOpenOrdersSort] = React.useState<{
    key: OpenOrdersSortKey;
    direction: SortDirection;
  }>({
    key: "orderId",
    direction: "asc",
  });
  const [openOrdersPageSize, setOpenOrdersPageSize] =
    React.useState<(typeof TRADING_PAGE_SIZES)[number]>(() =>
      readStoredPageSize(TRADING_OPEN_ORDERS_PAGE_SIZE_STORAGE_KEY, 10),
    );
  const [openOrdersPage, setOpenOrdersPage] = React.useState<number>(1);
  const [fillsHistoryTab, setFillsHistoryTab] = React.useState<FillsHistoryTab>("market-fills");
  const [fillsHistoryPageSize, setFillsHistoryPageSize] =
    React.useState<(typeof TRADING_PAGE_SIZES)[number]>(() =>
      readStoredPageSize(TRADING_FILLS_HISTORY_PAGE_SIZE_STORAGE_KEY, 10),
    );
  const [marketFillsSort, setMarketFillsSort] = React.useState<{
    key: MarketFillsSortKey;
    direction: SortDirection;
  }>({
    key: "matchedAt",
    direction: "desc",
  });
  const [myFillsSort, setMyFillsSort] = React.useState<{
    key: MyFillsSortKey;
    direction: SortDirection;
  }>({
    key: "matchedAt",
    direction: "desc",
  });
  const [myOrdersHistorySort, setMyOrdersHistorySort] = React.useState<{
    key: MyOrdersHistorySortKey;
    direction: SortDirection;
  }>({
    key: "updatedAt",
    direction: "desc",
  });
  const [marketFillsPage, setMarketFillsPage] = React.useState<number>(1);
  const [myFillsPage, setMyFillsPage] = React.useState<number>(1);
  const [myOrdersHistoryPage, setMyOrdersHistoryPage] = React.useState<number>(1);
  const refreshInFlightRef = React.useRef<boolean>(false);
  const chartOutcome = React.useMemo(() => {
    return selectChartOutcome(market?.outcomes ?? []);
  }, [market?.outcomes]);

  const refresh = React.useCallback(async () => {
    if (refreshInFlightRef.current) {
      return;
    }

    refreshInFlightRef.current = true;
    setStatus("loading...");
    try {
      const [nextMarket, nextBook, nextFills, metadataRows, nextMarketStats] = await withRetry(
        async () =>
          Promise.all([
            api.getMarket(apiBase, props.marketId),
            api.getMarketOrderBook(apiBase, props.marketId),
            api.listMarketFills(apiBase, props.marketId, 200),
            api.listMarketMetadata(apiBase, {
              market_id: props.marketId,
              limit: 1,
            }).catch(() => [] as api.MarketMetadataView[]),
            api.getPublicMarketStatsById(apiBase, props.marketId).catch(
              () => null as api.PublicMarketStatsView | null,
            ),
          ]),
        {
          attempts: 3,
          initialDelayMs: 250,
        },
      );

      setMarket(nextMarket);
      setBook(nextBook);
      setMarketFills(nextFills);
      setMarketMetadata(metadataRows[0] ?? null);
      setMarketStats(nextMarketStats);
      setStatus("ok");

      if (!auth.userApiKey.trim()) {
        setMyOrders([]);
        setMyFills([]);
      } else {
        try {
          const [nextMyOrders, nextMyFills] = await withRetry(
            async () =>
              Promise.all([
                api.listMyOrders(apiBase, auth, props.marketId),
                api.listMyFills(apiBase, auth, props.marketId),
              ]),
            {
              attempts: 2,
              initialDelayMs: 200,
            },
          );
          setMyOrders(nextMyOrders);
          setMyFills(nextMyFills);
        } catch {
          setMyOrders([]);
          setMyFills([]);
        }
      }
    } catch (err: unknown) {
      setMarket(null);
      setMarketMetadata(null);
      setMarketStats(null);
      setBook(null);
      setMarketFills([]);
      setMyOrders([]);
      setMyFills([]);
      setStatus(describeError(err));
    } finally {
      refreshInFlightRef.current = false;
    }
  }, [apiBase, auth, props.marketId]);

  React.useEffect(() => {
    let disposed = false;
    let timerId: number | null = null;

    const refreshLoop = async () => {
      if (disposed) {
        return;
      }

      await refresh();

      if (disposed) {
        return;
      }
      timerId = window.setTimeout(() => {
        void refreshLoop();
      }, 1_000);
    };

    void refreshLoop();

    return () => {
      disposed = true;
      if (timerId !== null) {
        window.clearTimeout(timerId);
      }
    };
  }, [refresh]);

  React.useEffect(() => {
    if (market?.outcomes.length) {
      if (!outcome || !market.outcomes.includes(outcome)) {
        setOutcome(market.outcomes[0]);
      }
      if (!resolveOutcome || !market.outcomes.includes(resolveOutcome)) {
        setResolveOutcome(market.outcomes[0]);
      }
      if (!orderBookOutcome || !market.outcomes.includes(orderBookOutcome)) {
        setOrderBookOutcome(market.outcomes[0]);
      }
    }
  }, [market, outcome, resolveOutcome, orderBookOutcome]);

  React.useEffect(() => {
    window.localStorage.setItem(ORDERBOOK_OPEN_STORAGE_KEY, orderBookOpen ? "1" : "0");
  }, [orderBookOpen]);

  React.useEffect(() => {
    window.localStorage.setItem(CHART_RANGE_STORAGE_KEY, chartRange);
  }, [chartRange]);

  React.useEffect(() => {
    window.localStorage.setItem(TRADING_OPEN_ORDERS_PAGE_SIZE_STORAGE_KEY, String(openOrdersPageSize));
  }, [openOrdersPageSize]);

  React.useEffect(() => {
    window.localStorage.setItem(TRADING_FILLS_HISTORY_PAGE_SIZE_STORAGE_KEY, String(fillsHistoryPageSize));
  }, [fillsHistoryPageSize]);

  React.useEffect(() => {
    if (!chartOutcome) {
      setChartSnapshot(null);
      return;
    }

    let disposed = false;
    let stream: EventSource | null = null;
    let onFillsHandler: ((event: MessageEvent<string>) => void) | null = null;
    let streamFlushTimerId: number | null = null;
    let pendingFills: api.MarketChartUpdateFillView[] = [];
    let pendingLastFillSequence: number | null = null;

    const flushPendingStreamUpdate = () => {
      streamFlushTimerId = null;
      if (disposed || pendingLastFillSequence === null || pendingFills.length === 0) {
        return;
      }

      const payload: api.MarketChartUpdatesEvent = {
        market_id: props.marketId,
        fills: pendingFills,
        last_fill_sequence: pendingLastFillSequence,
      };
      pendingFills = [];
      pendingLastFillSequence = null;
      setChartSnapshot((current) => mergeChartSnapshotWithUpdates(current, payload, chartRange));
    };

    const schedulePendingStreamUpdate = () => {
      if (streamFlushTimerId !== null) {
        return;
      }
      streamFlushTimerId = window.setTimeout(flushPendingStreamUpdate, CHART_STREAM_BATCH_MS);
    };

    const loadChart = async () => {
      try {
        const snapshot = await withRetry(
          async () =>
            api.getMarketChartSnapshot(
              apiBase,
              props.marketId,
              chartRange,
              chartOutcome,
              chartSamplePointsForRange(chartRange),
            ),
          {
            attempts: 3,
            initialDelayMs: 250,
          },
        );
        if (disposed) {
          return;
        }
        const normalizedSnapshot = normalizeAllRangeSnapshot(snapshot);
        setChartSnapshot({
          ...normalizedSnapshot,
          fills: [],
        });

        stream = api.createMarketChartUpdatesStream(
          apiBase,
          props.marketId,
          chartOutcome,
          snapshot.last_fill_sequence,
        );

        const onFills = (event: MessageEvent<string>) => {
          if (disposed) {
            return;
          }
          try {
            const payload = JSON.parse(event.data) as api.MarketChartUpdatesEvent;
            if (payload.market_id !== props.marketId || payload.fills.length === 0) {
              return;
            }

            pendingFills.push(...payload.fills);
            if (pendingFills.length > CHART_STREAM_MAX_BUFFERED_FILLS) {
              pendingFills = pendingFills.slice(-CHART_STREAM_MAX_BUFFERED_FILLS);
            }
            pendingLastFillSequence = payload.last_fill_sequence;
            schedulePendingStreamUpdate();
          } catch {
            // Ignore malformed payloads and continue processing future updates.
          }
        };

        onFillsHandler = onFills;
        stream.addEventListener("fills", onFills as EventListener);
      } catch {
        if (!disposed) {
          setChartSnapshot(null);
        }
      }
    };

    void loadChart();

    return () => {
      disposed = true;
      if (streamFlushTimerId !== null) {
        window.clearTimeout(streamFlushTimerId);
      }
      if (stream) {
        if (onFillsHandler) {
          stream.removeEventListener("fills", onFillsHandler as EventListener);
        }
        stream.close();
      }
    };
  }, [apiBase, chartOutcome, chartRange, props.marketId]);

  React.useEffect(() => {
    const binSpec = chartBinSpecForRange(chartRange);
    if (!binSpec || !chartSnapshot || chartSnapshot.market_id !== props.marketId) {
      return;
    }

    const endMs = parseTimestampMs(chartSnapshot.end_at);
    if (endMs === null) {
      return;
    }
    const nextBoundaryMs = endMs + binSpec.binSizeMs;
    const delayMs = Math.max(100, nextBoundaryMs - Date.now() + 25);

    const timerId = window.setTimeout(() => {
      setChartSnapshot((current) => {
        if (!current || current.market_id !== props.marketId) {
          return current;
        }

        return mergeChartSnapshotWithUpdates(
          current,
          {
            market_id: current.market_id,
            fills: [],
            last_fill_sequence: current.last_fill_sequence ?? -1,
          },
          chartRange,
        );
      });
    }, delayMs);

    return () => {
      window.clearTimeout(timerId);
    };
  }, [chartRange, chartSnapshot, props.marketId]);

  const openOrders = React.useMemo(() => {
    return myOrders.filter((row) => row.status === "Open" || row.status === "PartiallyFilled");
  }, [myOrders]);
  const sortedOpenOrders = React.useMemo(() => {
    const rows = [...openOrders];
    rows.sort((left, right) => {
      let comparison = 0;
      switch (openOrdersSort.key) {
        case "orderId":
          comparison = compareText(left.order_id, right.order_id);
          break;
        case "side":
          comparison = compareText(left.side, right.side);
          break;
        case "outcome":
          comparison = compareText(left.outcome, right.outcome);
          break;
        case "price":
          comparison = compareNullableNumber(left.price_ticks, right.price_ticks);
          break;
        case "remaining":
          comparison = compareNullableNumber(left.remaining_minor, right.remaining_minor);
          break;
      }

      if (comparison === 0) {
        comparison = compareText(left.order_id, right.order_id);
      }

      if (openOrdersSort.direction === "desc") {
        return -comparison;
      }
      return comparison;
    });
    return rows;
  }, [openOrders, openOrdersSort]);
  const openOrdersPageCount = React.useMemo(() => {
    return Math.max(1, Math.ceil(sortedOpenOrders.length / openOrdersPageSize));
  }, [openOrdersPageSize, sortedOpenOrders.length]);
  const pagedOpenOrders = React.useMemo(() => {
    const startIndex = (openOrdersPage - 1) * openOrdersPageSize;
    return sortedOpenOrders.slice(startIndex, startIndex + openOrdersPageSize);
  }, [openOrdersPage, openOrdersPageSize, sortedOpenOrders]);
  const sortedMarketFills = React.useMemo(() => {
    const rows = [...marketFills];
    rows.sort((left, right) => {
      let comparison = 0;
      switch (marketFillsSort.key) {
        case "fillId":
          comparison = compareText(left.fill_id, right.fill_id);
          break;
        case "outcome":
          comparison = compareText(left.outcome, right.outcome);
          break;
        case "price":
          comparison = compareNullableNumber(left.price_ticks, right.price_ticks);
          break;
        case "quantity":
          comparison = compareNullableNumber(left.quantity_minor, right.quantity_minor);
          break;
        case "matchedAt":
          comparison = compareTimestampStrings(left.matched_at, right.matched_at);
          break;
      }

      if (comparison === 0) {
        comparison = compareText(left.fill_id, right.fill_id);
      }

      if (marketFillsSort.direction === "desc") {
        return -comparison;
      }
      return comparison;
    });
    return rows;
  }, [marketFills, marketFillsSort]);
  const marketFillsPageCount = React.useMemo(() => {
    return Math.max(1, Math.ceil(sortedMarketFills.length / fillsHistoryPageSize));
  }, [fillsHistoryPageSize, sortedMarketFills.length]);
  const pagedMarketFills = React.useMemo(() => {
    const startIndex = (marketFillsPage - 1) * fillsHistoryPageSize;
    return sortedMarketFills.slice(startIndex, startIndex + fillsHistoryPageSize);
  }, [fillsHistoryPageSize, marketFillsPage, sortedMarketFills]);
  const sortedMyFills = React.useMemo(() => {
    const rows = [...myFills];
    rows.sort((left, right) => {
      let comparison = 0;
      switch (myFillsSort.key) {
        case "fillId":
          comparison = compareText(left.fill_id, right.fill_id);
          break;
        case "role":
          comparison = compareText(left.perspective_role, right.perspective_role);
          break;
        case "outcome":
          comparison = compareText(left.outcome, right.outcome);
          break;
        case "price":
          comparison = compareNullableNumber(left.price_ticks, right.price_ticks);
          break;
        case "quantity":
          comparison = compareNullableNumber(left.quantity_minor, right.quantity_minor);
          break;
        case "matchedAt":
          comparison = compareTimestampStrings(left.matched_at, right.matched_at);
          break;
      }

      if (comparison === 0) {
        comparison = compareText(left.fill_id, right.fill_id);
      }

      if (myFillsSort.direction === "desc") {
        return -comparison;
      }
      return comparison;
    });
    return rows;
  }, [myFills, myFillsSort]);
  const myFillsPageCount = React.useMemo(() => {
    return Math.max(1, Math.ceil(sortedMyFills.length / fillsHistoryPageSize));
  }, [fillsHistoryPageSize, sortedMyFills.length]);
  const pagedMyFills = React.useMemo(() => {
    const startIndex = (myFillsPage - 1) * fillsHistoryPageSize;
    return sortedMyFills.slice(startIndex, startIndex + fillsHistoryPageSize);
  }, [fillsHistoryPageSize, myFillsPage, sortedMyFills]);
  const sortedMyOrdersHistory = React.useMemo(() => {
    const rows = [...myOrders];
    rows.sort((left, right) => {
      let comparison = 0;
      switch (myOrdersHistorySort.key) {
        case "orderId":
          comparison = compareText(left.order_id, right.order_id);
          break;
        case "status":
          comparison = compareText(left.status, right.status);
          break;
        case "side":
          comparison = compareText(left.side, right.side);
          break;
        case "outcome":
          comparison = compareText(left.outcome, right.outcome);
          break;
        case "price":
          comparison = compareNullableNumber(left.price_ticks, right.price_ticks);
          break;
        case "quantity":
          comparison = compareNullableNumber(left.quantity_minor, right.quantity_minor);
          break;
        case "updatedAt":
          comparison = compareTimestampStrings(left.updated_at, right.updated_at);
          break;
      }

      if (comparison === 0) {
        comparison = compareText(left.order_id, right.order_id);
      }

      if (myOrdersHistorySort.direction === "desc") {
        return -comparison;
      }
      return comparison;
    });
    return rows;
  }, [myOrders, myOrdersHistorySort]);
  const myOrdersHistoryPageCount = React.useMemo(() => {
    return Math.max(1, Math.ceil(sortedMyOrdersHistory.length / fillsHistoryPageSize));
  }, [fillsHistoryPageSize, sortedMyOrdersHistory.length]);
  const pagedMyOrdersHistory = React.useMemo(() => {
    const startIndex = (myOrdersHistoryPage - 1) * fillsHistoryPageSize;
    return sortedMyOrdersHistory.slice(startIndex, startIndex + fillsHistoryPageSize);
  }, [fillsHistoryPageSize, myOrdersHistoryPage, sortedMyOrdersHistory]);

  const normalizedChartState = React.useMemo(
    () => normalizeChartState(chartSnapshot),
    [chartSnapshot],
  );
  const sampledChartSeries = React.useMemo<SampledChartPoint[]>(
    () => normalizedChartState.samples,
    [normalizedChartState.samples],
  );

  const chartLastFilledPoint = React.useMemo(() => {
    for (let index = sampledChartSeries.length - 1; index >= 0; index -= 1) {
      const point = sampledChartSeries[index];
      const priceTicks = point?.price_ticks;
      if (priceTicks !== null && priceTicks !== undefined) {
        return {
          ts: point.ts,
          price_ticks: priceTicks,
        };
      }
    }
    return null;
  }, [sampledChartSeries]);
  const chartLastFilledPointIndex = React.useMemo(() => {
    for (let index = sampledChartSeries.length - 1; index >= 0; index -= 1) {
      const point = sampledChartSeries[index];
      if (point?.price_ticks !== null && point?.price_ticks !== undefined) {
        return index;
      }
    }
    return -1;
  }, [sampledChartSeries]);
  const renderChartEndpointDot = React.useCallback(
    (dotProps: Record<string, unknown>) => {
      const index = Number(dotProps.index);
      if (!Number.isFinite(index) || index !== chartLastFilledPointIndex) {
        return null;
      }

      return <ChartEndpointPulseDot {...dotProps} />;
    },
    [chartLastFilledPointIndex],
  );
  const latestSamplePrice = chartLastFilledPoint?.price_ticks ?? null;
  const latestPrice =
    latestSamplePrice ??
    normalizedChartState.fills.at(-1)?.price_ticks ??
    normalizedChartState.previousFill?.price_ticks ??
    null;
  const latestChance = latestPrice === null ? null : Math.max(0, Math.min(100, latestPrice));
  const isLoading = status === "loading...";
  const isError = !isLoading && status !== "ok";
  const marketStatusLabel = market?.status === "Open" ? "Live" : (market?.status ?? "-");
  const marketVolume24hMinor = React.useMemo(() => {
    if (marketStats?.volume_24h_minor !== undefined && marketStats?.volume_24h_minor !== null) {
      return marketStats.volume_24h_minor;
    }

    const cutoffMs = Date.now() - 24 * 60 * 60 * 1_000;
    return marketFills.reduce((sum, fill) => {
      const matchedAtMs = parseTimestampMs(fill.matched_at);
      if (matchedAtMs === null || matchedAtMs < cutoffMs) {
        return sum;
      }
      return sum + fill.price_ticks * fill.quantity_minor;
    }, 0);
  }, [marketFills, marketStats?.volume_24h_minor]);
  const marketOpenInterestMinor = marketStats?.open_interest_minor ?? 0;
  const heroBackgroundImageUrl = withAssetVariant(
    marketMetadata?.hero_background_image_url,
    1260,
    70,
  );
  const onOpenOrdersSortClick = React.useCallback((key: OpenOrdersSortKey) => {
    setOpenOrdersSort((previous) => {
      if (previous.key === key) {
        return {
          key,
          direction: previous.direction === "asc" ? "desc" : "asc",
        };
      }
      return {
        key,
        direction: "asc",
      };
    });
    setOpenOrdersPage(1);
  }, []);
  const onMarketFillsSortClick = React.useCallback((key: MarketFillsSortKey) => {
    setMarketFillsSort((previous) => {
      if (previous.key === key) {
        return {
          key,
          direction: previous.direction === "asc" ? "desc" : "asc",
        };
      }
      return {
        key,
        direction: "asc",
      };
    });
    setMarketFillsPage(1);
  }, []);
  const onMyFillsSortClick = React.useCallback((key: MyFillsSortKey) => {
    setMyFillsSort((previous) => {
      if (previous.key === key) {
        return {
          key,
          direction: previous.direction === "asc" ? "desc" : "asc",
        };
      }
      return {
        key,
        direction: "asc",
      };
    });
    setMyFillsPage(1);
  }, []);
  const onMyOrdersHistorySortClick = React.useCallback((key: MyOrdersHistorySortKey) => {
    setMyOrdersHistorySort((previous) => {
      if (previous.key === key) {
        return {
          key,
          direction: previous.direction === "asc" ? "desc" : "asc",
        };
      }
      return {
        key,
        direction: "asc",
      };
    });
    setMyOrdersHistoryPage(1);
  }, []);

  React.useEffect(() => {
    setOpenOrdersPage((current) => Math.min(current, openOrdersPageCount));
  }, [openOrdersPageCount]);
  React.useEffect(() => {
    setMarketFillsPage((current) => Math.min(current, marketFillsPageCount));
  }, [marketFillsPageCount]);
  React.useEffect(() => {
    setMyFillsPage((current) => Math.min(current, myFillsPageCount));
  }, [myFillsPageCount]);
  React.useEffect(() => {
    setMyOrdersHistoryPage((current) => Math.min(current, myOrdersHistoryPageCount));
  }, [myOrdersHistoryPageCount]);

  const bookOutcome = orderBookOutcome || outcome;
  const asksForOutcome = React.useMemo(() => {
    return (book?.asks ?? [])
      .filter((level) => level.outcome === bookOutcome)
      .sort((left, right) => right.price_ticks - left.price_ticks)
      .slice(0, 8);
  }, [book, bookOutcome]);
  const bidsForOutcome = React.useMemo(() => {
    return (book?.bids ?? [])
      .filter((level) => level.outcome === bookOutcome)
      .sort((left, right) => right.price_ticks - left.price_ticks)
      .slice(0, 8);
  }, [book, bookOutcome]);
  const maxBookDepth = React.useMemo(() => {
    const quantities = [...asksForOutcome, ...bidsForOutcome].map((level) => level.quantity_minor);
    return Math.max(1, ...quantities);
  }, [asksForOutcome, bidsForOutcome]);

  const bestAsk = React.useMemo(() => {
    const asks = (book?.asks ?? []).filter((level) => level.outcome === bookOutcome);
    if (asks.length === 0) {
      return null;
    }

    return asks.reduce((minPrice, level) => Math.min(minPrice, level.price_ticks), asks[0]?.price_ticks ?? 0);
  }, [book, bookOutcome]);
  const bestBid = React.useMemo(() => {
    const bids = (book?.bids ?? []).filter((level) => level.outcome === bookOutcome);
    if (bids.length === 0) {
      return null;
    }

    return bids.reduce((maxPrice, level) => Math.max(maxPrice, level.price_ticks), bids[0]?.price_ticks ?? 0);
  }, [book, bookOutcome]);
  const spread = bestAsk !== null && bestBid !== null ? bestAsk - bestBid : null;
  const bestAskByOutcome = React.useMemo(() => {
    const byOutcome = new Map<string, number>();
    for (const level of book?.asks ?? []) {
      const current = byOutcome.get(level.outcome);
      if (current === undefined || level.price_ticks < current) {
        byOutcome.set(level.outcome, level.price_ticks);
      }
    }
    return byOutcome;
  }, [book]);
  const bestBidByOutcome = React.useMemo(() => {
    const byOutcome = new Map<string, number>();
    for (const level of book?.bids ?? []) {
      const current = byOutcome.get(level.outcome);
      if (current === undefined || level.price_ticks > current) {
        byOutcome.set(level.outcome, level.price_ticks);
      }
    }
    return byOutcome;
  }, [book]);

  const buyLabel = market?.outcomes[0] ?? "Yes";
  const sellLabel = market?.outcomes[1] ?? "No";
  const buyLabelTone = getOutcomeTone(buyLabel, market?.outcomes ?? []);
  const sellLabelTone = getOutcomeTone(sellLabel, market?.outcomes ?? []);
  const buyLabelPrice =
    side === "Buy" ? (bestAskByOutcome.get(buyLabel) ?? null) : (bestBidByOutcome.get(buyLabel) ?? null);
  const sellLabelPrice =
    side === "Buy" ? (bestAskByOutcome.get(sellLabel) ?? null) : (bestBidByOutcome.get(sellLabel) ?? null);
  const submitOrderLabel = outcome ? `${side} ${outcome}` : side;
  const chartConfig = React.useMemo<ChartConfig>(
    () => ({
      price_ticks: {
        label: `${chartOutcome ?? "YES"} chance`,
        color: "var(--primary)",
      },
    }),
    [chartOutcome],
  );
  const { chartYMin, chartYMax, chartYTicks } = React.useMemo(() => {
    let chartDataMin: number | null = null;
    let chartDataMax: number | null = null;
    for (const row of sampledChartSeries) {
      const value = row.price_ticks;
      if (value === null || value === undefined) {
        continue;
      }

      if (chartDataMin === null || value < chartDataMin) {
        chartDataMin = value;
      }
      if (chartDataMax === null || value > chartDataMax) {
        chartDataMax = value;
      }
    }

    if (chartDataMin === null || chartDataMax === null) {
      return {
        chartYMin: 0,
        chartYMax: 100,
        chartYTicks: buildChanceTicks(0, 100),
      };
    }

    if (chartDataMin === chartDataMax) {
      const min = Math.max(0, chartDataMin - 5);
      const max = Math.min(100, chartDataMax + 5);
      return {
        chartYMin: min,
        chartYMax: max,
        chartYTicks: buildChanceTicks(min, max),
      };
    }

    const padding = Math.max(2, Math.ceil((chartDataMax - chartDataMin) * 0.18));
    const min = Math.max(0, Math.floor((chartDataMin - padding) / 5) * 5);
    const max = Math.min(100, Math.ceil((chartDataMax + padding) / 5) * 5);
    return {
      chartYMin: min,
      chartYMax: max,
      chartYTicks: buildChanceTicks(min, max),
    };
  }, [sampledChartSeries]);
  const chartXTicks = React.useMemo(() => {
    if (sampledChartSeries.length === 0) {
      return [] as number[];
    }

    const firstTimestamp = sampledChartSeries[0]?.ts ?? 0;
    const lastTimestamp = sampledChartSeries.at(-1)?.ts ?? firstTimestamp;
    if (firstTimestamp === lastTimestamp) {
      return [firstTimestamp];
    }

    const ticks = new Set<number>();
    for (let i = 0; i <= 4; i += 1) {
      ticks.add(Math.round(firstTimestamp + ((lastTimestamp - firstTimestamp) * i) / 4));
    }
    ticks.add(firstTimestamp);
    ticks.add(lastTimestamp);
    return [...ticks].sort((left, right) => left - right);
  }, [sampledChartSeries]);
  const chartWindowDurationMs = React.useMemo(() => {
    if (sampledChartSeries.length < 2) {
      return 0;
    }
    const firstTimestamp = sampledChartSeries[0]?.ts ?? 0;
    const lastTimestamp = sampledChartSeries.at(-1)?.ts ?? firstTimestamp;
    return Math.max(0, lastTimestamp - firstTimestamp);
  }, [sampledChartSeries]);
  const chartXAxisTickFormatter = React.useMemo(() => {
    const formatter = new Intl.DateTimeFormat(
      undefined,
      xAxisDateTimeFormatOptions(chartRange, chartWindowDurationMs),
    );

    return (value: number | string) => {
      const timestampMs = typeof value === "number" ? value : Number(value);
      if (!Number.isFinite(timestampMs)) {
        return "";
      }

      return formatter.format(new Date(timestampMs));
    };
  }, [chartRange, chartWindowDurationMs]);

  const placeOrder = React.useCallback(async () => {
    if (!market) {
      toast.error("Market is not loaded.");
      return;
    }
    if (!session?.account_id) {
      toast.error("Connect a user wallet key to place orders.");
      return;
    }

    const parsedQty = Number(quantityMinor);
    let parsedPrice: number | undefined;

    if (orderType === "Limit") {
      const candidatePrice = Number(priceTicks);
      if (!Number.isInteger(candidatePrice) || candidatePrice <= 0) {
        toast.error("Price must be an integer cents value greater than 0 for limit orders.");
        return;
      }
      parsedPrice = candidatePrice;
    }
    if (!Number.isInteger(parsedQty) || parsedQty <= 0) {
      toast.error("Quantity must be a whole number greater than 0.");
      return;
    }
    if (!outcome) {
      toast.error("Select an outcome first.");
      return;
    }

    setPlacingOrder(true);
    const toastId = toast.loading(`Placing ${orderType.toLowerCase()} order...`);

    try {
      const placeWithNonce = (nonce: number) =>
        api.placeOrder(apiBase, auth, {
          market_id: props.marketId,
          outcome,
          side,
          order_type: orderType,
          nonce,
          price_ticks: parsedPrice,
          quantity_minor: parsedQty,
        });

      let nonce = api.nextOrderNonce(await api.listMyOrders(apiBase, auth));
      let result: api.PlaceOrderResponse;
      try {
        result = await placeWithNonce(nonce);
      } catch (err: unknown) {
        const expectedNonce = api.expectedNonceFromError(err);
        if (expectedNonce === null) {
          throw err;
        }
        nonce = expectedNonce;
        result = await placeWithNonce(expectedNonce);
      }

      toast.success(`Order placed: ${result.order_id}`, {
        id: toastId,
        description: `${result.order_type}/${result.tif}, ${result.status}, nonce=${nonce}, fills=${result.fills.length}`,
      });
      await refresh();
    } catch (err: unknown) {
      toast.error("Order placement failed.", {
        id: toastId,
        description: describeError(err),
      });
    } finally {
      setPlacingOrder(false);
    }
  }, [
    apiBase,
    auth,
    market,
    outcome,
    orderType,
    priceTicks,
    props.marketId,
    quantityMinor,
    refresh,
    session?.account_id,
    side,
  ]);
  const handleHeroRefresh = React.useCallback(() => {
    setManualRefreshPending(true);
    void refresh().finally(() => {
      setManualRefreshPending(false);
    });
  }, [refresh]);

  return (
    <div className="space-y-6">
      <Card
        className={cn(
          "relative overflow-hidden",
          heroBackgroundImageUrl
            ? "border-none ring-1 ring-border"
            : "bg-gradient-to-br from-emerald-50 via-background to-red-50 dark:from-emerald-950/20 dark:via-background dark:to-red-950/20",
          heroBackgroundImageUrl ? "" : "hero-gradient-motion",
          "py-0"
        )}
      >
        {heroBackgroundImageUrl ? (
          <>
            <img
              src={heroBackgroundImageUrl}
              alt=""
              className="pointer-events-none absolute inset-0 h-full w-full object-cover"
              loading="lazy"
            />
            <div className="pointer-events-none absolute inset-0 bg-gradient-to-r from-background via-background/94 to-background/82" />
          </>
        ) : null}
        {heroBackgroundImageUrl ? null : (
          <>
            <div className="hero-gradient-focus-a absolute -top-24 -left-14 size-56 bg-emerald-300/20 blur-3xl dark:bg-emerald-500/10" />
            <div className="hero-gradient-focus-b absolute -right-20 -bottom-28 size-56 bg-red-300/20 blur-3xl dark:bg-red-500/10" />
          </>
        )}
        <div className="relative flex h-full flex-col gap-5 p-5 md:p-6">
          <CardHeader className="space-y-4 p-0">
            <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
              <div className="min-w-0 space-y-1">
                <div className="text-xs font-semibold uppercase tracking-[0.18em] text-muted-foreground">
                  Market
                </div>
                <div className="flex min-w-0 items-start">
                  <CardTitle className="text-balance text-xl md:text-3xl">
                    {market?.question ?? `Loading market ${props.marketId}`}
                  </CardTitle>
                </div>
                <CardDescription className="flex flex-wrap items-center gap-2 pt-1 text-xs">
                  <span>ID: {props.marketId}</span>
                  <span className="text-border">|</span>
                  <span>Outcomes: {market?.outcomes.join(" / ") ?? "-"}</span>
                </CardDescription>
              </div>

              <div className="flex shrink-0 items-center gap-2 self-start">
                <Badge variant={market?.status === "Open" ? "default" : "outline"}>
                  {marketStatusLabel}
                </Badge>
                <Button
                  size="icon-xs"
                  variant="outline"
                  disabled={manualRefreshPending}
                  aria-label="Refresh market"
                  onClick={handleHeroRefresh}
                >
                  <RiRefreshLine className={cn("size-4", manualRefreshPending ? "animate-spin" : "")} />
                </Button>
              </div>
            </div>
          </CardHeader>

          <CardContent className="mt-auto p-0">
            <div className="flex flex-wrap items-end justify-end gap-x-6 gap-y-2 text-right">
              <div className="space-y-0.5 text-sm">
                <div className="text-xs uppercase tracking-wide text-muted-foreground">24h Volume</div>
                <div className="font-medium text-foreground">{formatUsdFromCents(marketVolume24hMinor)}</div>
              </div>
              <div className="space-y-0.5 text-sm">
                <div className="text-xs uppercase tracking-wide text-muted-foreground">Open Interest</div>
                <div className="font-medium text-foreground">{formatUsdFromCents(marketOpenInterestMinor)}</div>
              </div>
              <div className="space-y-1">
                <div className="text-4xl leading-none font-semibold md:text-5xl">
                  {latestChance === null ? "-" : `${latestChance}%`}
                </div>
                <div className="text-xs uppercase tracking-wide text-muted-foreground">Chance</div>
              </div>
            </div>
          </CardContent>
        </div>
      </Card>

      {isError ? (
        <Alert variant="destructive">
          <AlertTitle>Trading data failed</AlertTitle>
          <AlertDescription>{status}</AlertDescription>
        </Alert>
      ) : null}

      {isLoading && !market ? (
        <div className="grid gap-4 xl:grid-cols-[1.65fr_1fr]">
          <Card>
            <CardHeader>
              <Skeleton className="h-6 w-28" />
              <Skeleton className="h-4 w-2/3" />
            </CardHeader>
            <CardContent className="space-y-3">
              <Skeleton className="h-64 w-full" />
              <Skeleton className="h-4 w-1/2" />
            </CardContent>
          </Card>
          <Card>
            <CardHeader>
              <Skeleton className="h-6 w-32" />
              <Skeleton className="h-4 w-2/3" />
            </CardHeader>
            <CardContent className="space-y-2">
              <Skeleton className="h-8 w-full" />
              <Skeleton className="h-8 w-full" />
              <Skeleton className="h-8 w-full" />
              <Skeleton className="h-10 w-full" />
            </CardContent>
          </Card>
        </div>
      ) : null}

      <div className="grid gap-4 xl:grid-cols-[1.65fr_1fr]">
        <Card className="overflow-hidden border-border/80">
          <CardContent className="space-y-3">
            {sampledChartSeries.length === 0 ? (
              <Card className="border-dashed shadow-none">
                <CardContent className="px-4 py-7 text-center text-sm text-muted-foreground">
                  No fills yet for this market.
                </CardContent>
              </Card>
            ) : (
              <div className="space-y-1">
                <div className="relative">
                  <ChartContainer config={chartConfig} className="h-64 w-full !aspect-auto bg-muted/5">
                    <RechartsLineChart
                      data={sampledChartSeries}
                      margin={{ top: 8, right: 8, left: 8, bottom: 8 }}
                    >
                      <RechartsCartesianGrid vertical={false} strokeDasharray="3 3" />
                      <RechartsXAxis
                        dataKey="ts"
                        type="number"
                        domain={["dataMin", "dataMax"]}
                        ticks={chartXTicks}
                        axisLine={false}
                        tickLine={false}
                        tickMargin={8}
                        minTickGap={20}
                        tickFormatter={chartXAxisTickFormatter}
                      />
                      <RechartsYAxis
                        orientation="right"
                        domain={[chartYMin, chartYMax]}
                        ticks={chartYTicks}
                        axisLine={false}
                        tickLine={false}
                        tickMargin={8}
                        allowDecimals={false}
                        tickFormatter={(value: number | string) => `${Number(value)}%`}
                      />
                      <RechartsTooltip
                        cursor={false}
                        content={
                          <ChartTooltipContent
                            hideIndicator
                            labelFormatter={(_, payload) => {
                              const timestamp = payload?.[0]?.payload?.ts;
                              if (typeof timestamp !== "number") {
                                return "";
                              }
                              return new Date(timestamp).toLocaleString();
                            }}
                            formatter={(value) => {
                              if (value === null) {
                                return <span className="font-medium text-muted-foreground">No trade yet</span>;
                              }
                              const numericValue = Number(value);
                              if (!Number.isFinite(numericValue)) {
                                return <span>{String(value)}</span>;
                              }
                              return <span className="font-medium">{numericValue}%</span>;
                            }}
                          />
                        }
                      />
                      <RechartsLine
                        dataKey="price_ticks"
                        type="monotoneX"
                        stroke="var(--color-price_ticks)"
                        strokeWidth={2.5}
                        strokeLinejoin="round"
                        strokeLinecap="round"
                        connectNulls={false}
                        dot={renderChartEndpointDot}
                        activeDot={false}
                        isAnimationActive
                        animateNewValues
                        animationDuration={520}
                        animationEasing="ease-out"
                      />
                    </RechartsLineChart>
                  </ChartContainer>
                  <div className="pointer-events-none absolute inset-0 flex items-center justify-center" aria-hidden="true">
                    <span className="select-none text-6xl font-bold tracking-[-0.07em] lowercase text-muted-foreground/10">
                      pebble
                    </span>
                  </div>
                </div>

                <div className="flex flex-wrap items-center justify-end gap-2 text-xs text-muted-foreground">
                  <div className="flex flex-wrap items-center gap-1">
                    {CHART_RANGES.map((range) => (
                      <Button
                        key={range}
                        size="xs"
                        variant={chartRange === range ? "secondary" : "ghost"}
                        className={cn(
                          "h-7 px-2 font-semibold",
                          chartRange === range ? "text-foreground" : "text-muted-foreground",
                        )}
                        onClick={() => setChartRange(range)}
                      >
                        {range}
                      </Button>
                    ))}
                  </div>
                </div>
              </div>
            )}
          </CardContent>
        </Card>

        <Card className="h-fit border-border/80 xl:sticky xl:top-6">
          <CardHeader className="border-b pb-2">
            <div className="flex items-end justify-between gap-2">
              <Tabs
                className="gap-0"
                value={side}
                onValueChange={(value) => setSide((value as api.OrderSide | null) ?? "Buy")}
              >
                <TabsList variant="line" className="h-8 gap-1">
                  <TabsTrigger
                    value="Buy"
                    className="h-8 px-2 text-base font-semibold after:bg-emerald-600 data-active:text-emerald-700 dark:data-active:text-emerald-300"
                  >
                    Buy
                  </TabsTrigger>
                  <TabsTrigger
                    value="Sell"
                    className="h-8 px-2 text-base font-semibold after:bg-red-600 data-active:text-red-700 dark:data-active:text-red-300"
                  >
                    Sell
                  </TabsTrigger>
                </TabsList>
              </Tabs>

              <Select
                value={orderType}
                onValueChange={(value) => setOrderType((value as api.OrderType | null) ?? "Limit")}
              >
                <SelectTrigger size="sm" className="min-w-24">
                  <SelectValue placeholder="Order Type" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="Limit">Limit</SelectItem>
                  <SelectItem value="Market">Market</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </CardHeader>

          <CardContent className="space-y-3 pt-3">
            {market?.outcomes.length === 2 ? (
              <div className="grid grid-cols-2 gap-2">
                <Button
                  type="button"
                  variant="outline"
                  className={cn(
                    "h-9 font-semibold",
                    getOutcomeSelectorClass(buyLabelTone, outcome === buyLabel),
                  )}
                  onClick={() => setOutcome(buyLabel)}
                >
                  {buyLabel} {formatTicks(buyLabelPrice)}
                </Button>
                <Button
                  type="button"
                  variant="outline"
                  className={cn(
                    "h-9 font-semibold",
                    getOutcomeSelectorClass(sellLabelTone, outcome === sellLabel),
                  )}
                  onClick={() => setOutcome(sellLabel)}
                >
                  {sellLabel} {formatTicks(sellLabelPrice)}
                </Button>
              </div>
            ) : (
              <Select value={outcome} onValueChange={(value) => setOutcome(value ?? "")}>
                <SelectTrigger size="sm" className="w-full">
                  <SelectValue placeholder="Outcome" />
                </SelectTrigger>
                <SelectContent>
                  {market?.outcomes.map((entry) => (
                    <SelectItem key={entry} value={entry}>
                      {entry}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            )}

            <div className={cn("grid gap-2", orderType === "Limit" ? "grid-cols-2" : "grid-cols-1")}>
              {orderType === "Limit" ? (
                <label className="space-y-1 text-[11px] text-muted-foreground">
                  <span>Price (cents)</span>
                  <Input
                    value={priceTicks}
                    onChange={(event) => setPriceTicks(event.target.value)}
                    placeholder="1-99"
                  />
                </label>
              ) : null}
              <label className="space-y-1 text-[11px] text-muted-foreground">
                <span>Quantity (shares)</span>
                <Input
                  value={quantityMinor}
                  onChange={(event) => setQuantityMinor(event.target.value)}
                  placeholder="shares"
                />
              </label>
            </div>

            <div className="flex flex-wrap items-center gap-1.5 text-[11px]">
              <Button
                size="xs"
                variant="outline"
                className="h-7 px-2"
                onClick={() => {
                  const current = Number(quantityMinor);
                  if (Number.isFinite(current)) {
                    setQuantityMinor(String(Math.max(1, Math.trunc(current) + 1)));
                  } else {
                    setQuantityMinor("1");
                  }
                }}
              >
                +1
              </Button>
              <Button
                size="xs"
                variant="outline"
                className="h-7 px-2"
                onClick={() => {
                  const current = Number(quantityMinor);
                  if (Number.isFinite(current)) {
                    setQuantityMinor(String(Math.max(1, Math.trunc(current) + 5)));
                  } else {
                    setQuantityMinor("5");
                  }
                }}
              >
                +5
              </Button>
              <Button
                size="xs"
                variant="outline"
                className="h-7 px-2"
                onClick={() => {
                  const current = Number(quantityMinor);
                  if (Number.isFinite(current)) {
                    setQuantityMinor(String(Math.max(1, Math.trunc(current) + 10)));
                  } else {
                    setQuantityMinor("10");
                  }
                }}
              >
                +10
              </Button>
              {orderType === "Limit" ? (
                <>
                  <Button
                    size="xs"
                    variant="outline"
                    className="h-7 px-2"
                    onClick={() => {
                      if (bestBid !== null) {
                        setPriceTicks(String(bestBid));
                      }
                    }}
                  >
                    Bid {formatTicks(bestBid)}
                  </Button>
                  <Button
                    size="xs"
                    variant="outline"
                    className="h-7 px-2"
                    onClick={() => {
                      if (bestAsk !== null) {
                        setPriceTicks(String(bestAsk));
                      }
                    }}
                  >
                    Ask {formatTicks(bestAsk)}
                  </Button>
                </>
              ) : null}
            </div>

            <div className="text-[11px] text-muted-foreground">
              {orderType === "Limit" ? "Limit (GTC, price in cents)." : "Market (IOC, immediate-or-cancel)."}
            </div>

            <div className="grid gap-2">
              <Button
                disabled={placingOrder || !session?.account_id || !market || market.status !== "Open"}
                className={cn(
                  "h-9",
                  side === "Sell"
                    ? "bg-red-600 text-white hover:bg-red-500"
                    : "bg-emerald-600 text-white hover:bg-emerald-500",
                )}
                onClick={() => {
                  void placeOrder();
                }}
              >
                {placingOrder ? `Placing ${submitOrderLabel}...` : submitOrderLabel}
              </Button>
            </div>
          </CardContent>
        </Card>
      </div>

      <Collapsible open={orderBookOpen} onOpenChange={(open) => setOrderBookOpen(open)}>
        <Card className="overflow-hidden border-border/80 gap-0 py-0">
          <CollapsibleTrigger
            className="group flex w-full items-center justify-between gap-2 bg-muted/20 px-4 py-3 text-left"
          >
            <div>
              <div className="text-sm font-semibold">Order Book</div>
            </div>
            <div className="flex items-center gap-3 text-xs text-muted-foreground">
              <span>Bid: {formatTicks(bestBid)}</span>
              <span>Ask: {formatTicks(bestAsk)}</span>
              <span>Spread: {formatTicks(spread)}</span>
              {orderBookOpen ? <RiArrowUpSLine className="size-5" /> : <RiArrowDownSLine className="size-5" />}
            </div>
          </CollapsibleTrigger>

          <CollapsibleContent>
            <CardContent className="space-y-2 pt-3 pb-3">
              {market?.outcomes.length ? (
                <div className="flex flex-wrap gap-2">
                  {market.outcomes.map((entry) => {
                    const tone = getOutcomeTone(entry, market.outcomes);
                    const active = entry === bookOutcome;
                    return (
                      <Button
                        key={entry}
                        size="xs"
                        variant="outline"
                        className={cn("h-7", getOutcomeSelectorClass(tone, active))}
                        onClick={() => setOrderBookOutcome(entry)}
                      >
                        {entry}
                      </Button>
                    );
                  })}
                </div>
              ) : null}

              <Table>
                <TableHeader>
                  <TableRow className="bg-muted/20 hover:bg-muted/20">
                    <TableHead>Price (c)</TableHead>
                    <TableHead>Qty</TableHead>
                    <TableHead className="text-right">Orders</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {asksForOutcome.map((level) => {
                    const depthPct = Math.max(6, Math.round((level.quantity_minor / maxBookDepth) * 100));
                    return (
                      <TableRow
                        key={`ask-${level.outcome}-${level.price_ticks}`}
                        style={{
                          backgroundImage: `linear-gradient(to right, rgb(239 68 68 / 0.14) ${depthPct}%, transparent ${depthPct}%)`,
                        }}
                      >
                        <TableCell className="font-semibold text-red-600 dark:text-red-300">
                          {formatTicks(level.price_ticks)}
                        </TableCell>
                        <TableCell>{formatShares(level.quantity_minor)}</TableCell>
                        <TableCell className="text-right">{level.order_count}</TableCell>
                      </TableRow>
                    );
                  })}

                  <TableRow className="bg-muted/15 hover:bg-muted/15">
                    <TableCell colSpan={2}>Last: {formatTicks(latestPrice)}</TableCell>
                    <TableCell className="text-right">Spread: {formatTicks(spread)}</TableCell>
                  </TableRow>

                  {bidsForOutcome.map((level) => {
                    const depthPct = Math.max(6, Math.round((level.quantity_minor / maxBookDepth) * 100));
                    return (
                      <TableRow
                        key={`bid-${level.outcome}-${level.price_ticks}`}
                        style={{
                          backgroundImage: `linear-gradient(to right, rgb(16 185 129 / 0.16) ${depthPct}%, transparent ${depthPct}%)`,
                        }}
                      >
                        <TableCell className="font-semibold text-emerald-600 dark:text-emerald-300">
                          {formatTicks(level.price_ticks)}
                        </TableCell>
                        <TableCell>{formatShares(level.quantity_minor)}</TableCell>
                        <TableCell className="text-right">{level.order_count}</TableCell>
                      </TableRow>
                    );
                  })}

                  {asksForOutcome.length === 0 && bidsForOutcome.length === 0 ? (
                    <TableRow>
                      <TableCell colSpan={3} className="py-5 text-center text-sm text-muted-foreground">
                        No depth yet for outcome {bookOutcome || "-"}.
                      </TableCell>
                    </TableRow>
                  ) : null}
                </TableBody>
              </Table>
            </CardContent>
          </CollapsibleContent>
        </Card>
      </Collapsible>

      <div className="grid gap-4">
        <Card>
          <CardHeader>
            <CardTitle>Open Orders</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>
                    <Button
                      type="button"
                      variant="ghost"
                      size="xs"
                      className="-ml-2"
                      onClick={() => onOpenOrdersSortClick("orderId")}
                    >
                      Order
                      {renderSortIcon(openOrdersSort, "orderId")}
                    </Button>
                  </TableHead>
                  <TableHead>
                    <Button
                      type="button"
                      variant="ghost"
                      size="xs"
                      className="-ml-2"
                      onClick={() => onOpenOrdersSortClick("side")}
                    >
                      Side
                      {renderSortIcon(openOrdersSort, "side")}
                    </Button>
                  </TableHead>
                  <TableHead>
                    <Button
                      type="button"
                      variant="ghost"
                      size="xs"
                      className="-ml-2"
                      onClick={() => onOpenOrdersSortClick("outcome")}
                    >
                      Outcome
                      {renderSortIcon(openOrdersSort, "outcome")}
                    </Button>
                  </TableHead>
                  <TableHead>
                    <Button
                      type="button"
                      variant="ghost"
                      size="xs"
                      className="-ml-2"
                      onClick={() => onOpenOrdersSortClick("price")}
                    >
                      Price (c)
                      {renderSortIcon(openOrdersSort, "price")}
                    </Button>
                  </TableHead>
                  <TableHead>
                    <Button
                      type="button"
                      variant="ghost"
                      size="xs"
                      className="-ml-2"
                      onClick={() => onOpenOrdersSortClick("remaining")}
                    >
                      Remaining
                      {renderSortIcon(openOrdersSort, "remaining")}
                    </Button>
                  </TableHead>
                  <TableHead>Action</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {pagedOpenOrders.map((order) => (
                  <TableRow key={order.order_id}>
                    <TableCell>{order.order_id}</TableCell>
                    <TableCell>{order.side}</TableCell>
                    <TableCell>{order.outcome}</TableCell>
                    <TableCell>{formatTicks(order.price_ticks)}</TableCell>
                    <TableCell>{formatShares(order.remaining_minor)}</TableCell>
                    <TableCell>
                      <Button
                        size="xs"
                        variant="outline"
                        disabled={cancellingOrderId === order.order_id}
                        onClick={async () => {
                          setCancellingOrderId(order.order_id);
                          const toastId = toast.loading(`Cancelling ${order.order_id}...`);
                          try {
                            const result = await api.cancelOrder(apiBase, auth, order.order_id);
                            toast.success(`Order cancelled: ${result.order_id}`, {
                              id: toastId,
                              description: `Released ${formatUsdFromCents(result.released_locked_minor)}`,
                            });
                            await refresh();
                          } catch (err: unknown) {
                            toast.error("Order cancel failed.", {
                              id: toastId,
                              description: describeError(err),
                            });
                          } finally {
                            setCancellingOrderId(null);
                          }
                        }}
                      >
                        Cancel
                      </Button>
                    </TableCell>
                  </TableRow>
                ))}
                {sortedOpenOrders.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={6} className="text-muted-foreground">
                      No open orders for this market.
                    </TableCell>
                  </TableRow>
                ) : null}
              </TableBody>
            </Table>
            <div className="flex flex-wrap items-center justify-between gap-2">
              <div className="flex items-center gap-1">
                <span className="text-xs text-muted-foreground">Rows</span>
                {TRADING_PAGE_SIZES.map((size) => (
                  <Button
                    key={size}
                    type="button"
                    variant={openOrdersPageSize === size ? "default" : "outline"}
                    size="xs"
                    onClick={() => {
                      setOpenOrdersPageSize(size);
                      setOpenOrdersPage(1);
                    }}
                  >
                    {size}
                  </Button>
                ))}
              </div>
              <div className="flex items-center gap-2">
                <Button
                  type="button"
                  variant="outline"
                  size="xs"
                  disabled={openOrdersPage <= 1}
                  onClick={() => setOpenOrdersPage((current) => Math.max(1, current - 1))}
                >
                  Previous
                </Button>
                <span className="text-xs text-muted-foreground">
                  Page {openOrdersPage} of {openOrdersPageCount}
                </span>
                <Button
                  type="button"
                  variant="outline"
                  size="xs"
                  disabled={openOrdersPage >= openOrdersPageCount}
                  onClick={() => setOpenOrdersPage((current) => Math.min(openOrdersPageCount, current + 1))}
                >
                  Next
                </Button>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Fills and History</CardTitle>
        </CardHeader>
        <CardContent>
          <Tabs
            value={fillsHistoryTab}
            onValueChange={(value) => setFillsHistoryTab((value as FillsHistoryTab | null) ?? "market-fills")}
          >
            <TabsList variant="line">
              <TabsTrigger value="market-fills">Market fills</TabsTrigger>
              <TabsTrigger value="my-fills">My fills</TabsTrigger>
              <TabsTrigger value="my-orders">My order history</TabsTrigger>
            </TabsList>

            <TabsContent value="market-fills" className="space-y-3">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>
                      <Button
                        type="button"
                        variant="ghost"
                        size="xs"
                        className="-ml-2"
                        onClick={() => onMarketFillsSortClick("fillId")}
                      >
                        Fill
                        {renderSortIcon(marketFillsSort, "fillId")}
                      </Button>
                    </TableHead>
                    <TableHead>
                      <Button
                        type="button"
                        variant="ghost"
                        size="xs"
                        className="-ml-2"
                        onClick={() => onMarketFillsSortClick("outcome")}
                      >
                        Outcome
                        {renderSortIcon(marketFillsSort, "outcome")}
                      </Button>
                    </TableHead>
                    <TableHead>
                      <Button
                        type="button"
                        variant="ghost"
                        size="xs"
                        className="-ml-2"
                        onClick={() => onMarketFillsSortClick("price")}
                      >
                        Price (c)
                        {renderSortIcon(marketFillsSort, "price")}
                      </Button>
                    </TableHead>
                    <TableHead>
                      <Button
                        type="button"
                        variant="ghost"
                        size="xs"
                        className="-ml-2"
                        onClick={() => onMarketFillsSortClick("quantity")}
                      >
                        Qty
                        {renderSortIcon(marketFillsSort, "quantity")}
                      </Button>
                    </TableHead>
                    <TableHead>
                      <Button
                        type="button"
                        variant="ghost"
                        size="xs"
                        className="-ml-2"
                        onClick={() => onMarketFillsSortClick("matchedAt")}
                      >
                        Matched
                        {renderSortIcon(marketFillsSort, "matchedAt")}
                      </Button>
                    </TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {pagedMarketFills.map((fill) => (
                    <TableRow key={fill.fill_id}>
                      <TableCell>{fill.fill_id}</TableCell>
                      <TableCell>{fill.outcome}</TableCell>
                      <TableCell>{formatTicks(fill.price_ticks)}</TableCell>
                      <TableCell>{formatShares(fill.quantity_minor)}</TableCell>
                      <TableCell>{formatTime(fill.matched_at)}</TableCell>
                    </TableRow>
                  ))}
                  {sortedMarketFills.length === 0 ? (
                    <TableRow>
                      <TableCell colSpan={5} className="text-muted-foreground">
                        No market fills yet.
                      </TableCell>
                    </TableRow>
                  ) : null}
                </TableBody>
              </Table>
              <div className="flex flex-wrap items-center justify-between gap-2">
                <div className="flex items-center gap-1">
                  <span className="text-xs text-muted-foreground">Rows</span>
                  {TRADING_PAGE_SIZES.map((size) => (
                    <Button
                      key={size}
                      type="button"
                      variant={fillsHistoryPageSize === size ? "default" : "outline"}
                      size="xs"
                      onClick={() => {
                        setFillsHistoryPageSize(size);
                        setMarketFillsPage(1);
                        setMyFillsPage(1);
                        setMyOrdersHistoryPage(1);
                      }}
                    >
                      {size}
                    </Button>
                  ))}
                </div>
                <div className="flex items-center gap-2">
                  <Button
                    type="button"
                    variant="outline"
                    size="xs"
                    disabled={marketFillsPage <= 1}
                    onClick={() => setMarketFillsPage((current) => Math.max(1, current - 1))}
                  >
                    Previous
                  </Button>
                  <span className="text-xs text-muted-foreground">
                    Page {marketFillsPage} of {marketFillsPageCount}
                  </span>
                  <Button
                    type="button"
                    variant="outline"
                    size="xs"
                    disabled={marketFillsPage >= marketFillsPageCount}
                    onClick={() =>
                      setMarketFillsPage((current) => Math.min(marketFillsPageCount, current + 1))
                    }
                  >
                    Next
                  </Button>
                </div>
              </div>
            </TabsContent>

            <TabsContent value="my-fills" className="space-y-3">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>
                      <Button
                        type="button"
                        variant="ghost"
                        size="xs"
                        className="-ml-2"
                        onClick={() => onMyFillsSortClick("fillId")}
                      >
                        Fill
                        {renderSortIcon(myFillsSort, "fillId")}
                      </Button>
                    </TableHead>
                    <TableHead>
                      <Button
                        type="button"
                        variant="ghost"
                        size="xs"
                        className="-ml-2"
                        onClick={() => onMyFillsSortClick("role")}
                      >
                        Role
                        {renderSortIcon(myFillsSort, "role")}
                      </Button>
                    </TableHead>
                    <TableHead>
                      <Button
                        type="button"
                        variant="ghost"
                        size="xs"
                        className="-ml-2"
                        onClick={() => onMyFillsSortClick("outcome")}
                      >
                        Outcome
                        {renderSortIcon(myFillsSort, "outcome")}
                      </Button>
                    </TableHead>
                    <TableHead>
                      <Button
                        type="button"
                        variant="ghost"
                        size="xs"
                        className="-ml-2"
                        onClick={() => onMyFillsSortClick("price")}
                      >
                        Price (c)
                        {renderSortIcon(myFillsSort, "price")}
                      </Button>
                    </TableHead>
                    <TableHead>
                      <Button
                        type="button"
                        variant="ghost"
                        size="xs"
                        className="-ml-2"
                        onClick={() => onMyFillsSortClick("quantity")}
                      >
                        Qty
                        {renderSortIcon(myFillsSort, "quantity")}
                      </Button>
                    </TableHead>
                    <TableHead>
                      <Button
                        type="button"
                        variant="ghost"
                        size="xs"
                        className="-ml-2"
                        onClick={() => onMyFillsSortClick("matchedAt")}
                      >
                        Matched
                        {renderSortIcon(myFillsSort, "matchedAt")}
                      </Button>
                    </TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {pagedMyFills.map((fill) => (
                    <TableRow key={fill.fill_id}>
                      <TableCell>{fill.fill_id}</TableCell>
                      <TableCell>{fill.perspective_role}</TableCell>
                      <TableCell>{fill.outcome}</TableCell>
                      <TableCell>{formatTicks(fill.price_ticks)}</TableCell>
                      <TableCell>{formatShares(fill.quantity_minor)}</TableCell>
                      <TableCell>{formatTime(fill.matched_at)}</TableCell>
                    </TableRow>
                  ))}
                  {sortedMyFills.length === 0 ? (
                    <TableRow>
                      <TableCell colSpan={6} className="text-muted-foreground">
                        No personal fills for this market.
                      </TableCell>
                    </TableRow>
                  ) : null}
                </TableBody>
              </Table>
              <div className="flex flex-wrap items-center justify-between gap-2">
                <div className="flex items-center gap-1">
                  <span className="text-xs text-muted-foreground">Rows</span>
                  {TRADING_PAGE_SIZES.map((size) => (
                    <Button
                      key={size}
                      type="button"
                      variant={fillsHistoryPageSize === size ? "default" : "outline"}
                      size="xs"
                      onClick={() => {
                        setFillsHistoryPageSize(size);
                        setMarketFillsPage(1);
                        setMyFillsPage(1);
                        setMyOrdersHistoryPage(1);
                      }}
                    >
                      {size}
                    </Button>
                  ))}
                </div>
                <div className="flex items-center gap-2">
                  <Button
                    type="button"
                    variant="outline"
                    size="xs"
                    disabled={myFillsPage <= 1}
                    onClick={() => setMyFillsPage((current) => Math.max(1, current - 1))}
                  >
                    Previous
                  </Button>
                  <span className="text-xs text-muted-foreground">
                    Page {myFillsPage} of {myFillsPageCount}
                  </span>
                  <Button
                    type="button"
                    variant="outline"
                    size="xs"
                    disabled={myFillsPage >= myFillsPageCount}
                    onClick={() => setMyFillsPage((current) => Math.min(myFillsPageCount, current + 1))}
                  >
                    Next
                  </Button>
                </div>
              </div>
            </TabsContent>

            <TabsContent value="my-orders" className="space-y-3">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>
                      <Button
                        type="button"
                        variant="ghost"
                        size="xs"
                        className="-ml-2"
                        onClick={() => onMyOrdersHistorySortClick("orderId")}
                      >
                        Order
                        {renderSortIcon(myOrdersHistorySort, "orderId")}
                      </Button>
                    </TableHead>
                    <TableHead>
                      <Button
                        type="button"
                        variant="ghost"
                        size="xs"
                        className="-ml-2"
                        onClick={() => onMyOrdersHistorySortClick("status")}
                      >
                        Status
                        {renderSortIcon(myOrdersHistorySort, "status")}
                      </Button>
                    </TableHead>
                    <TableHead>
                      <Button
                        type="button"
                        variant="ghost"
                        size="xs"
                        className="-ml-2"
                        onClick={() => onMyOrdersHistorySortClick("side")}
                      >
                        Side
                        {renderSortIcon(myOrdersHistorySort, "side")}
                      </Button>
                    </TableHead>
                    <TableHead>
                      <Button
                        type="button"
                        variant="ghost"
                        size="xs"
                        className="-ml-2"
                        onClick={() => onMyOrdersHistorySortClick("outcome")}
                      >
                        Outcome
                        {renderSortIcon(myOrdersHistorySort, "outcome")}
                      </Button>
                    </TableHead>
                    <TableHead>
                      <Button
                        type="button"
                        variant="ghost"
                        size="xs"
                        className="-ml-2"
                        onClick={() => onMyOrdersHistorySortClick("price")}
                      >
                        Price (c)
                        {renderSortIcon(myOrdersHistorySort, "price")}
                      </Button>
                    </TableHead>
                    <TableHead>
                      <Button
                        type="button"
                        variant="ghost"
                        size="xs"
                        className="-ml-2"
                        onClick={() => onMyOrdersHistorySortClick("quantity")}
                      >
                        Qty
                        {renderSortIcon(myOrdersHistorySort, "quantity")}
                      </Button>
                    </TableHead>
                    <TableHead>
                      <Button
                        type="button"
                        variant="ghost"
                        size="xs"
                        className="-ml-2"
                        onClick={() => onMyOrdersHistorySortClick("updatedAt")}
                      >
                        Updated
                        {renderSortIcon(myOrdersHistorySort, "updatedAt")}
                      </Button>
                    </TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {pagedMyOrdersHistory.map((order) => (
                    <TableRow key={order.order_id}>
                      <TableCell>{order.order_id}</TableCell>
                      <TableCell>{order.status}</TableCell>
                      <TableCell>{order.side}</TableCell>
                      <TableCell>{order.outcome}</TableCell>
                      <TableCell>{formatTicks(order.price_ticks)}</TableCell>
                      <TableCell>{formatShares(order.quantity_minor)}</TableCell>
                      <TableCell>{formatTime(order.updated_at)}</TableCell>
                    </TableRow>
                  ))}
                  {sortedMyOrdersHistory.length === 0 ? (
                    <TableRow>
                      <TableCell colSpan={7} className="text-muted-foreground">
                        No personal orders for this market.
                      </TableCell>
                    </TableRow>
                  ) : null}
                </TableBody>
              </Table>
              <div className="flex flex-wrap items-center justify-between gap-2">
                <div className="flex items-center gap-1">
                  <span className="text-xs text-muted-foreground">Rows</span>
                  {TRADING_PAGE_SIZES.map((size) => (
                    <Button
                      key={size}
                      type="button"
                      variant={fillsHistoryPageSize === size ? "default" : "outline"}
                      size="xs"
                      onClick={() => {
                        setFillsHistoryPageSize(size);
                        setMarketFillsPage(1);
                        setMyFillsPage(1);
                        setMyOrdersHistoryPage(1);
                      }}
                    >
                      {size}
                    </Button>
                  ))}
                </div>
                <div className="flex items-center gap-2">
                  <Button
                    type="button"
                    variant="outline"
                    size="xs"
                    disabled={myOrdersHistoryPage <= 1}
                    onClick={() => setMyOrdersHistoryPage((current) => Math.max(1, current - 1))}
                  >
                    Previous
                  </Button>
                  <span className="text-xs text-muted-foreground">
                    Page {myOrdersHistoryPage} of {myOrdersHistoryPageCount}
                  </span>
                  <Button
                    type="button"
                    variant="outline"
                    size="xs"
                    disabled={myOrdersHistoryPage >= myOrdersHistoryPageCount}
                    onClick={() =>
                      setMyOrdersHistoryPage((current) => Math.min(myOrdersHistoryPageCount, current + 1))
                    }
                  >
                    Next
                  </Button>
                </div>
              </div>
            </TabsContent>
          </Tabs>
        </CardContent>
      </Card>

      {session?.is_admin ? (
        <Card>
          <CardHeader>
            <CardTitle>Admin Controls</CardTitle>
            <CardDescription>
              Close or resolve this market. These actions require an active admin wallet key.
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-3">
            <div className="grid gap-2 sm:grid-cols-[1fr_auto_auto]">
              <Select value={resolveOutcome} onValueChange={(value) => setResolveOutcome(value ?? "")}>
                <SelectTrigger>
                  <SelectValue placeholder="Resolve outcome" />
                </SelectTrigger>
                <SelectContent>
                  {market?.outcomes.map((entry) => (
                    <SelectItem key={entry} value={entry}>
                      {entry}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>

              <Button
                variant="outline"
                disabled={adminPendingAction !== null || market?.status !== "Open"}
                onClick={async () => {
                  setAdminPendingAction("close");
                  setAdminActionStatus("closing market...");
                  try {
                    const result = await api.closeMarket(apiBase, auth, props.marketId);
                    setAdminActionStatus(`closed ${result.contract_id}`);
                    await refresh();
                  } catch (err: unknown) {
                    setAdminActionStatus(describeError(err));
                  } finally {
                    setAdminPendingAction(null);
                  }
                }}
              >
                Close market
              </Button>

              <Button
                disabled={adminPendingAction !== null || !resolveOutcome}
                onClick={async () => {
                  setAdminPendingAction("resolve");
                  setAdminActionStatus("resolving market...");
                  try {
                    const result = await api.resolveMarket(apiBase, auth, props.marketId, resolveOutcome);
                    setAdminActionStatus(`resolved ${result.contract_id} -> ${resolveOutcome}`);
                    await refresh();
                  } catch (err: unknown) {
                    setAdminActionStatus(describeError(err));
                  } finally {
                    setAdminPendingAction(null);
                  }
                }}
              >
                Resolve market
              </Button>
            </div>

            <div className="text-xs text-muted-foreground">{adminActionStatus}</div>
          </CardContent>
        </Card>
      ) : null}
    </div>
  );
}
