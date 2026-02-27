import { Link } from "@tanstack/react-router";
import React from "react";

import { useSessionContext } from "@/app/session";
import { describeError } from "@/app/session";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardAction, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import * as api from "@/lib/api";
import { withRetry } from "@/lib/retry";
import {
  RiArrowDownSLine,
  RiArrowUpSLine,
  RiExpandUpDownLine,
  RiRefreshLine,
} from "@remixicon/react";
import { toast } from "sonner";

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

function formatTicks(value: number | null | undefined): string {
  if (value === null || value === undefined) {
    return "-";
  }

  return `${value}c`;
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

function buildPolyline(points: number[], width: number, height: number): string {
  if (points.length === 0) {
    return "";
  }

  const min = Math.min(...points);
  const max = Math.max(...points);
  const range = Math.max(max - min, 1);
  const xPad = 12;
  const yPad = 12;

  return points
    .map((value, index) => {
      const x = xPad + (index / Math.max(points.length - 1, 1)) * (width - xPad * 2);
      const y =
        height -
        yPad -
        ((value - min) / range) *
          (height - yPad * 2);
      return `${x.toFixed(2)},${y.toFixed(2)}`;
    })
    .join(" ");
}

function CardTitleWithTooltip({ title, subtitle }: { title: string; subtitle: string }) {
  return (
    <Tooltip>
      <TooltipTrigger render={<CardTitle className="w-fit cursor-help" />}>{title}</TooltipTrigger>
      <TooltipContent align="start">{subtitle}</TooltipContent>
    </Tooltip>
  );
}

type PositionSortKey =
  | "market"
  | "outcome"
  | "netQuantity"
  | "avgEntry"
  | "mark"
  | "markValue"
  | "realizedPnl";

type SortDirection = "asc" | "desc";

const POSITION_PAGE_SIZES = [10, 20, 50] as const;
const PORTFOLIO_OPEN_POSITIONS_PAGE_SIZE_STORAGE_KEY = "pebble-portfolio-open-positions-page-size";
const PORTFOLIO_ACTIVITY_PAGE_SIZE_STORAGE_KEY = "pebble-portfolio-activity-page-size";
const PORTFOLIO_REFRESH_INTERVAL_MS = 1_000;

type ActivityTab = "open-orders" | "orders" | "fills" | "deposits" | "withdrawals" | "receipts";
type OpenOrdersSortKey = "order" | "market" | "status" | "outcome" | "price" | "remaining";
type OrdersSortKey = "order" | "status" | "market" | "side" | "outcome" | "qty" | "updated";
type FillsSortKey = "fill" | "role" | "market" | "outcome" | "price" | "qty" | "matched";
type DepositsSortKey = "deposit" | "expected" | "status" | "created";
type WithdrawalsSortKey = "withdrawal" | "amount" | "state" | "status" | "created";
type ReceiptsSortKey = "receipt" | "kind" | "status" | "amount" | "reference" | "created";

function readStoredPositionPageSize(): (typeof POSITION_PAGE_SIZES)[number] {
  if (typeof window === "undefined") {
    return 10;
  }

  const rawValue = window.localStorage.getItem(PORTFOLIO_OPEN_POSITIONS_PAGE_SIZE_STORAGE_KEY);
  const parsed = Number(rawValue);
  if ((POSITION_PAGE_SIZES as ReadonlyArray<number>).includes(parsed)) {
    return parsed as (typeof POSITION_PAGE_SIZES)[number];
  }
  return 10;
}

function readStoredActivityPageSize(): (typeof POSITION_PAGE_SIZES)[number] {
  if (typeof window === "undefined") {
    return 10;
  }

  const rawValue = window.localStorage.getItem(PORTFOLIO_ACTIVITY_PAGE_SIZE_STORAGE_KEY);
  const parsed = Number(rawValue);
  if ((POSITION_PAGE_SIZES as ReadonlyArray<number>).includes(parsed)) {
    return parsed as (typeof POSITION_PAGE_SIZES)[number];
  }
  return 10;
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

function compareTime(left: string | null | undefined, right: string | null | undefined): number {
  const leftMillis = left ? Date.parse(left) : Number.NaN;
  const rightMillis = right ? Date.parse(right) : Number.NaN;
  const normalizedLeft = Number.isNaN(leftMillis) ? null : leftMillis;
  const normalizedRight = Number.isNaN(rightMillis) ? null : rightMillis;
  return compareNullableNumber(normalizedLeft, normalizedRight);
}

function renderSortIcon<Key extends string>(
  sort: { key: Key; direction: SortDirection },
  key: Key,
): React.ReactNode {
  if (sort.key !== key) {
    return <RiExpandUpDownLine className="size-3 text-muted-foreground" />;
  }
  if (sort.direction === "asc") {
    return <RiArrowUpSLine className="size-3" />;
  }
  return <RiArrowDownSLine className="size-3" />;
}

export function PortfolioPage() {
  const { apiBase, auth } = useSessionContext();

  const [summary, setSummary] = React.useState<api.MySummaryView | null>(null);
  const [positions, setPositions] = React.useState<api.PositionView[]>([]);
  const [history, setHistory] = React.useState<api.PortfolioHistoryView[]>([]);
  const [orders, setOrders] = React.useState<api.OrderRowView[]>([]);
  const [fills, setFills] = React.useState<api.FillRowView[]>([]);
  const [deposits, setDeposits] = React.useState<api.DepositPendingView[]>([]);
  const [withdrawals, setWithdrawals] = React.useState<api.WithdrawalPendingView[]>([]);
  const [receipts, setReceipts] = React.useState<api.ReceiptView[]>([]);

  const [status, setStatus] = React.useState<string>("loading...");
  const refreshPendingCountRef = React.useRef<number>(0);
  const knownReceiptIdsRef = React.useRef<Set<string>>(new Set());
  const receiptBaselineInitializedRef = React.useRef<boolean>(false);
  const [isRefreshing, setIsRefreshing] = React.useState<boolean>(false);
  const [depositAmount, setDepositAmount] = React.useState<string>("1000");
  const [depositing, setDepositing] = React.useState<boolean>(false);
  const [withdrawAmount, setWithdrawAmount] = React.useState<string>("1000");
  const [withdrawing, setWithdrawing] = React.useState<boolean>(false);
  const [positionSort, setPositionSort] = React.useState<{
    key: PositionSortKey;
    direction: SortDirection;
  }>({
    key: "market",
    direction: "asc",
  });
  const [positionsPageSize, setPositionsPageSize] =
    React.useState<(typeof POSITION_PAGE_SIZES)[number]>(() => readStoredPositionPageSize());
  const [positionsPage, setPositionsPage] = React.useState<number>(1);
  const [activityTab, setActivityTab] = React.useState<ActivityTab>("open-orders");
  const [activityPageSize, setActivityPageSize] =
    React.useState<(typeof POSITION_PAGE_SIZES)[number]>(() => readStoredActivityPageSize());
  const [openOrdersSort, setOpenOrdersSort] = React.useState<{
    key: OpenOrdersSortKey;
    direction: SortDirection;
  }>({ key: "order", direction: "asc" });
  const [ordersSort, setOrdersSort] = React.useState<{ key: OrdersSortKey; direction: SortDirection }>({
    key: "updated",
    direction: "desc",
  });
  const [fillsSort, setFillsSort] = React.useState<{ key: FillsSortKey; direction: SortDirection }>({
    key: "matched",
    direction: "desc",
  });
  const [depositsSort, setDepositsSort] = React.useState<{
    key: DepositsSortKey;
    direction: SortDirection;
  }>({ key: "created", direction: "desc" });
  const [withdrawalsSort, setWithdrawalsSort] = React.useState<{
    key: WithdrawalsSortKey;
    direction: SortDirection;
  }>({ key: "created", direction: "desc" });
  const [receiptsSort, setReceiptsSort] = React.useState<{
    key: ReceiptsSortKey;
    direction: SortDirection;
  }>({ key: "created", direction: "desc" });
  const [openOrdersPage, setOpenOrdersPage] = React.useState<number>(1);
  const [ordersPage, setOrdersPage] = React.useState<number>(1);
  const [fillsPage, setFillsPage] = React.useState<number>(1);
  const [depositsPage, setDepositsPage] = React.useState<number>(1);
  const [withdrawalsPage, setWithdrawalsPage] = React.useState<number>(1);
  const [receiptsPage, setReceiptsPage] = React.useState<number>(1);

  React.useEffect(() => {
    knownReceiptIdsRef.current.clear();
    receiptBaselineInitializedRef.current = false;
  }, [apiBase, auth.userApiKey]);

  const refresh = React.useCallback(async () => {
    refreshPendingCountRef.current += 1;
    setIsRefreshing(true);
    setStatus("loading...");
    try {
      const [
        nextSummary,
        nextPositions,
        nextHistory,
        nextOrders,
        nextFills,
        nextDeposits,
        nextWithdrawals,
        nextReceipts,
      ] = await withRetry(
        async () =>
          Promise.all([
            api.getMySummary(apiBase, auth),
            api.listMyPositions(apiBase, auth, 200),
            api.listMyPortfolioHistory(apiBase, auth, 200),
            api.listMyOrders(apiBase, auth),
            api.listMyFills(apiBase, auth),
            api.listMyDepositPendings(apiBase, auth),
            api.listMyWithdrawalPendings(apiBase, auth),
            api.listMyReceipts(apiBase, auth),
          ]),
        {
          attempts: 3,
          initialDelayMs: 250,
        },
      );

      setSummary(nextSummary);
      setPositions(nextPositions);
      setHistory(nextHistory);
      setOrders(nextOrders);
      setFills(nextFills);
      setDeposits(nextDeposits);
      setWithdrawals(nextWithdrawals);

      const knownReceiptIds = knownReceiptIdsRef.current;
      if (!receiptBaselineInitializedRef.current) {
        knownReceiptIds.clear();
        for (const receipt of nextReceipts) {
          knownReceiptIds.add(receipt.receipt_id);
        }
        receiptBaselineInitializedRef.current = true;
      } else {
        const newlySeenReceipts: api.ReceiptView[] = [];
        for (const receipt of nextReceipts) {
          if (!knownReceiptIds.has(receipt.receipt_id)) {
            newlySeenReceipts.push(receipt);
          }
          knownReceiptIds.add(receipt.receipt_id);
        }

        const sortedNewlySeenReceipts = newlySeenReceipts.sort((left, right) =>
          compareTime(left.created_at, right.created_at),
        );
        for (const receipt of sortedNewlySeenReceipts) {
          toast.success(`New ${receipt.kind.toLowerCase()} receipt`, {
            description: (
              <div className="space-y-1">
                <div className="text-xs text-muted-foreground">
                  Amount: {formatUsdFromCents(receipt.amount_minor)}
                </div>
                <div className="flex items-center gap-2">
                  <span className="text-xs text-muted-foreground">ID</span>
                  <span className="block max-w-60 truncate font-mono text-xs">
                    {receipt.receipt_id}
                  </span>
                </div>
              </div>
            ),
          });
        }
      }

      setReceipts(nextReceipts);
      setStatus("ok");
    } catch (err: unknown) {
      setSummary(null);
      setPositions([]);
      setHistory([]);
      setOrders([]);
      setFills([]);
      setDeposits([]);
      setWithdrawals([]);
      setReceipts([]);
      setStatus(describeError(err));
    } finally {
      refreshPendingCountRef.current = Math.max(0, refreshPendingCountRef.current - 1);
      if (refreshPendingCountRef.current === 0) {
        setIsRefreshing(false);
      }
    }
  }, [apiBase, auth]);

  React.useEffect(() => {
    void refresh();

    const id = window.setInterval(() => {
      void refresh();
    }, PORTFOLIO_REFRESH_INTERVAL_MS);

    return () => {
      window.clearInterval(id);
    };
  }, [refresh]);

  React.useEffect(() => {
    window.localStorage.setItem(
      PORTFOLIO_OPEN_POSITIONS_PAGE_SIZE_STORAGE_KEY,
      String(positionsPageSize),
    );
  }, [positionsPageSize]);

  React.useEffect(() => {
    window.localStorage.setItem(PORTFOLIO_ACTIVITY_PAGE_SIZE_STORAGE_KEY, String(activityPageSize));
  }, [activityPageSize]);

  const openOrders = React.useMemo(() => {
    return orders.filter((row) => row.status === "Open" || row.status === "PartiallyFilled");
  }, [orders]);
  const sortedActivityOpenOrders = React.useMemo(() => {
    const rows = [...openOrders];
    rows.sort((left, right) => {
      let comparison = 0;
      switch (openOrdersSort.key) {
        case "order":
          comparison = compareText(left.order_id, right.order_id);
          break;
        case "market":
          comparison = compareText(left.market_id, right.market_id);
          break;
        case "status":
          comparison = compareText(left.status, right.status);
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
      return openOrdersSort.direction === "desc" ? -comparison : comparison;
    });
    return rows;
  }, [openOrders, openOrdersSort]);
  const sortedActivityOrders = React.useMemo(() => {
    const rows = [...orders];
    rows.sort((left, right) => {
      let comparison = 0;
      switch (ordersSort.key) {
        case "order":
          comparison = compareText(left.order_id, right.order_id);
          break;
        case "status":
          comparison = compareText(left.status, right.status);
          break;
        case "market":
          comparison = compareText(left.market_id, right.market_id);
          break;
        case "side":
          comparison = compareText(left.side, right.side);
          break;
        case "outcome":
          comparison = compareText(left.outcome, right.outcome);
          break;
        case "qty":
          comparison = compareNullableNumber(left.quantity_minor, right.quantity_minor);
          break;
        case "updated":
          comparison = compareTime(left.updated_at, right.updated_at);
          break;
      }
      if (comparison === 0) {
        comparison = compareText(left.order_id, right.order_id);
      }
      return ordersSort.direction === "desc" ? -comparison : comparison;
    });
    return rows;
  }, [orders, ordersSort]);
  const sortedActivityFills = React.useMemo(() => {
    const rows = [...fills];
    rows.sort((left, right) => {
      let comparison = 0;
      switch (fillsSort.key) {
        case "fill":
          comparison = compareText(left.fill_id, right.fill_id);
          break;
        case "role":
          comparison = compareText(left.perspective_role, right.perspective_role);
          break;
        case "market":
          comparison = compareText(left.market_id, right.market_id);
          break;
        case "outcome":
          comparison = compareText(left.outcome, right.outcome);
          break;
        case "price":
          comparison = compareNullableNumber(left.price_ticks, right.price_ticks);
          break;
        case "qty":
          comparison = compareNullableNumber(left.quantity_minor, right.quantity_minor);
          break;
        case "matched":
          comparison = compareTime(left.matched_at, right.matched_at);
          break;
      }
      if (comparison === 0) {
        comparison = compareText(left.fill_id, right.fill_id);
      }
      return fillsSort.direction === "desc" ? -comparison : comparison;
    });
    return rows;
  }, [fills, fillsSort]);
  const sortedActivityDeposits = React.useMemo(() => {
    const rows = [...deposits];
    rows.sort((left, right) => {
      let comparison = 0;
      switch (depositsSort.key) {
        case "deposit":
          comparison = compareText(left.deposit_id, right.deposit_id);
          break;
        case "expected":
          comparison = compareNullableNumber(left.expected_amount_minor, right.expected_amount_minor);
          break;
        case "status":
          comparison = compareText(left.latest_status_class ?? "", right.latest_status_class ?? "");
          break;
        case "created":
          comparison = compareTime(left.created_at, right.created_at);
          break;
      }
      if (comparison === 0) {
        comparison = compareText(left.deposit_id, right.deposit_id);
      }
      return depositsSort.direction === "desc" ? -comparison : comparison;
    });
    return rows;
  }, [deposits, depositsSort]);
  const sortedActivityWithdrawals = React.useMemo(() => {
    const rows = [...withdrawals];
    rows.sort((left, right) => {
      let comparison = 0;
      switch (withdrawalsSort.key) {
        case "withdrawal":
          comparison = compareText(left.withdrawal_id, right.withdrawal_id);
          break;
        case "amount":
          comparison = compareNullableNumber(left.amount_minor, right.amount_minor);
          break;
        case "state":
          comparison = compareText(left.pending_state, right.pending_state);
          break;
        case "status":
          comparison = compareText(left.latest_status_class ?? "", right.latest_status_class ?? "");
          break;
        case "created":
          comparison = compareTime(left.created_at, right.created_at);
          break;
      }
      if (comparison === 0) {
        comparison = compareText(left.withdrawal_id, right.withdrawal_id);
      }
      return withdrawalsSort.direction === "desc" ? -comparison : comparison;
    });
    return rows;
  }, [withdrawals, withdrawalsSort]);
  const sortedActivityReceipts = React.useMemo(() => {
    const rows = [...receipts];
    rows.sort((left, right) => {
      let comparison = 0;
      switch (receiptsSort.key) {
        case "receipt":
          comparison = compareText(left.receipt_id, right.receipt_id);
          break;
        case "kind":
          comparison = compareText(left.kind, right.kind);
          break;
        case "status":
          comparison = compareText(left.status, right.status);
          break;
        case "amount":
          comparison = compareNullableNumber(left.amount_minor, right.amount_minor);
          break;
        case "reference":
          comparison = compareText(left.reference_id, right.reference_id);
          break;
        case "created":
          comparison = compareTime(left.created_at, right.created_at);
          break;
      }
      if (comparison === 0) {
        comparison = compareText(left.receipt_id, right.receipt_id);
      }
      return receiptsSort.direction === "desc" ? -comparison : comparison;
    });
    return rows;
  }, [receipts, receiptsSort]);
  const openOrdersPageCount = React.useMemo(
    () => Math.max(1, Math.ceil(sortedActivityOpenOrders.length / activityPageSize)),
    [activityPageSize, sortedActivityOpenOrders.length],
  );
  const ordersPageCount = React.useMemo(
    () => Math.max(1, Math.ceil(sortedActivityOrders.length / activityPageSize)),
    [activityPageSize, sortedActivityOrders.length],
  );
  const fillsPageCount = React.useMemo(
    () => Math.max(1, Math.ceil(sortedActivityFills.length / activityPageSize)),
    [activityPageSize, sortedActivityFills.length],
  );
  const depositsPageCount = React.useMemo(
    () => Math.max(1, Math.ceil(sortedActivityDeposits.length / activityPageSize)),
    [activityPageSize, sortedActivityDeposits.length],
  );
  const withdrawalsPageCount = React.useMemo(
    () => Math.max(1, Math.ceil(sortedActivityWithdrawals.length / activityPageSize)),
    [activityPageSize, sortedActivityWithdrawals.length],
  );
  const receiptsPageCount = React.useMemo(
    () => Math.max(1, Math.ceil(sortedActivityReceipts.length / activityPageSize)),
    [activityPageSize, sortedActivityReceipts.length],
  );
  const pagedOpenOrders = React.useMemo(() => {
    const start = (openOrdersPage - 1) * activityPageSize;
    return sortedActivityOpenOrders.slice(start, start + activityPageSize);
  }, [activityPageSize, openOrdersPage, sortedActivityOpenOrders]);
  const pagedOrders = React.useMemo(() => {
    const start = (ordersPage - 1) * activityPageSize;
    return sortedActivityOrders.slice(start, start + activityPageSize);
  }, [activityPageSize, ordersPage, sortedActivityOrders]);
  const pagedFills = React.useMemo(() => {
    const start = (fillsPage - 1) * activityPageSize;
    return sortedActivityFills.slice(start, start + activityPageSize);
  }, [activityPageSize, fillsPage, sortedActivityFills]);
  const pagedDeposits = React.useMemo(() => {
    const start = (depositsPage - 1) * activityPageSize;
    return sortedActivityDeposits.slice(start, start + activityPageSize);
  }, [activityPageSize, depositsPage, sortedActivityDeposits]);
  const pagedWithdrawals = React.useMemo(() => {
    const start = (withdrawalsPage - 1) * activityPageSize;
    return sortedActivityWithdrawals.slice(start, start + activityPageSize);
  }, [activityPageSize, sortedActivityWithdrawals, withdrawalsPage]);
  const pagedReceipts = React.useMemo(() => {
    const start = (receiptsPage - 1) * activityPageSize;
    return sortedActivityReceipts.slice(start, start + activityPageSize);
  }, [activityPageSize, receiptsPage, sortedActivityReceipts]);
  const sortedPositions = React.useMemo(() => {
    const rows = [...positions];
    rows.sort((left, right) => {
      let comparison = 0;
      switch (positionSort.key) {
        case "market": {
          const leftLabel = left.question ?? left.market_id;
          const rightLabel = right.question ?? right.market_id;
          comparison = compareText(leftLabel, rightLabel);
          if (comparison === 0) {
            comparison = compareText(left.market_id, right.market_id);
          }
          break;
        }
        case "outcome":
          comparison = compareText(left.outcome, right.outcome);
          break;
        case "netQuantity":
          comparison = compareNullableNumber(left.net_quantity_minor, right.net_quantity_minor);
          break;
        case "avgEntry":
          comparison = compareNullableNumber(left.avg_entry_price_ticks, right.avg_entry_price_ticks);
          break;
        case "mark":
          comparison = compareNullableNumber(left.mark_price_ticks, right.mark_price_ticks);
          break;
        case "markValue":
          comparison = compareNullableNumber(left.mark_value_minor, right.mark_value_minor);
          break;
        case "realizedPnl":
          comparison = compareNullableNumber(left.realized_pnl_minor, right.realized_pnl_minor);
          break;
      }

      if (comparison === 0) {
        comparison = compareText(`${left.market_id}-${left.outcome}`, `${right.market_id}-${right.outcome}`);
      }

      if (positionSort.direction === "desc") {
        return -comparison;
      }
      return comparison;
    });
    return rows;
  }, [positionSort, positions]);
  const positionsPageCount = React.useMemo(() => {
    return Math.max(1, Math.ceil(sortedPositions.length / positionsPageSize));
  }, [positionsPageSize, sortedPositions.length]);
  const pagedPositions = React.useMemo(() => {
    const startIndex = (positionsPage - 1) * positionsPageSize;
    return sortedPositions.slice(startIndex, startIndex + positionsPageSize);
  }, [positionsPage, positionsPageSize, sortedPositions]);

  const openPositionMarkValueMinor = React.useMemo(() => {
    return positions.reduce((sum, position) => sum + (position.mark_value_minor ?? 0), 0);
  }, [positions]);
  const accountBalanceMinor = React.useMemo(() => {
    if (!summary) {
      return null;
    }
    return summary.available_minor + summary.locked_open_orders_minor + openPositionMarkValueMinor;
  }, [openPositionMarkValueMinor, summary]);
  const historyPoints = React.useMemo(() => {
    return [...history]
      .sort((left, right) => Date.parse(left.created_at) - Date.parse(right.created_at))
      .map((entry) => entry.total_equity_minor);
  }, [history]);
  const historyPolyline = buildPolyline(historyPoints, 640, 220);
  const isLoading = status === "loading...";
  const isError = !isLoading && status !== "ok";
  const onPositionSortClick = React.useCallback((key: PositionSortKey) => {
    setPositionSort((previous) => {
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
    setPositionsPage(1);
  }, []);
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
  const onOrdersSortClick = React.useCallback((key: OrdersSortKey) => {
    setOrdersSort((previous) => {
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
    setOrdersPage(1);
  }, []);
  const onFillsSortClick = React.useCallback((key: FillsSortKey) => {
    setFillsSort((previous) => {
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
    setFillsPage(1);
  }, []);
  const onDepositsSortClick = React.useCallback((key: DepositsSortKey) => {
    setDepositsSort((previous) => {
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
    setDepositsPage(1);
  }, []);
  const onWithdrawalsSortClick = React.useCallback((key: WithdrawalsSortKey) => {
    setWithdrawalsSort((previous) => {
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
    setWithdrawalsPage(1);
  }, []);
  const onReceiptsSortClick = React.useCallback((key: ReceiptsSortKey) => {
    setReceiptsSort((previous) => {
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
    setReceiptsPage(1);
  }, []);
  const resetActivityPages = React.useCallback(() => {
    setOpenOrdersPage(1);
    setOrdersPage(1);
    setFillsPage(1);
    setDepositsPage(1);
    setWithdrawalsPage(1);
    setReceiptsPage(1);
  }, []);
  const onActivityPageSizeChange = React.useCallback(
    (size: (typeof POSITION_PAGE_SIZES)[number]) => {
      setActivityPageSize(size);
      resetActivityPages();
    },
    [resetActivityPages],
  );
  const renderPositionSortIcon = React.useCallback(
    (key: PositionSortKey) => {
      if (positionSort.key !== key) {
        return <RiExpandUpDownLine className="size-3 text-muted-foreground" />;
      }
      if (positionSort.direction === "asc") {
        return <RiArrowUpSLine className="size-3" />;
      }
      return <RiArrowDownSLine className="size-3" />;
    },
    [positionSort.direction, positionSort.key],
  );
  const renderActivityPagination = React.useCallback(
    (
      scope: string,
      page: number,
      pageCount: number,
      setPage: React.Dispatch<React.SetStateAction<number>>,
    ) => (
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div className="flex items-center gap-1">
          <span className="text-xs text-muted-foreground">Rows</span>
          {POSITION_PAGE_SIZES.map((size) => (
            <Button
              key={`${scope}-${size}`}
              type="button"
              variant={activityPageSize === size ? "default" : "outline"}
              size="xs"
              onClick={() => onActivityPageSizeChange(size)}
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
            disabled={page <= 1}
            onClick={() => setPage((current) => Math.max(1, current - 1))}
          >
            Previous
          </Button>
          <span className="text-xs text-muted-foreground">
            Page {page} of {pageCount}
          </span>
          <Button
            type="button"
            variant="outline"
            size="xs"
            disabled={page >= pageCount}
            onClick={() => setPage((current) => Math.min(pageCount, current + 1))}
          >
            Next
          </Button>
        </div>
      </div>
    ),
    [activityPageSize, onActivityPageSizeChange],
  );

  React.useEffect(() => {
    setPositionsPage((current) => Math.min(current, positionsPageCount));
  }, [positionsPageCount]);
  React.useEffect(() => {
    setOpenOrdersPage((current) => Math.min(current, openOrdersPageCount));
  }, [openOrdersPageCount]);
  React.useEffect(() => {
    setOrdersPage((current) => Math.min(current, ordersPageCount));
  }, [ordersPageCount]);
  React.useEffect(() => {
    setFillsPage((current) => Math.min(current, fillsPageCount));
  }, [fillsPageCount]);
  React.useEffect(() => {
    setDepositsPage((current) => Math.min(current, depositsPageCount));
  }, [depositsPageCount]);
  React.useEffect(() => {
    setWithdrawalsPage((current) => Math.min(current, withdrawalsPageCount));
  }, [withdrawalsPageCount]);
  React.useEffect(() => {
    setReceiptsPage((current) => Math.min(current, receiptsPageCount));
  }, [receiptsPageCount]);

  return (
    <TooltipProvider>
      <div className="space-y-4">
      <Card>
        <CardContent className="space-y-2 text-center">
          <div className="text-xs text-muted-foreground">Total Equity</div>
          <div className="text-5xl font-semibold tracking-tight">{formatUsdFromCents(accountBalanceMinor)}</div>
          <div className="text-sm text-muted-foreground">
            Withdrawable: <span className="text-foreground">{formatUsdFromCents(summary?.withdrawable_minor)}</span>
          </div>
        </CardContent>
      </Card>

      {isError ? (
        <Alert variant="destructive">
          <AlertTitle>Portfolio data failed</AlertTitle>
          <AlertDescription>{status}</AlertDescription>
        </Alert>
      ) : null}

      {isLoading && !summary ? (
        <Card>
          <CardHeader>
            <Skeleton className="mx-auto h-4 w-28" />
          </CardHeader>
          <CardContent className="space-y-2">
            <Skeleton className="mx-auto h-12 w-56" />
            <Skeleton className="mx-auto h-4 w-44" />
          </CardContent>
        </Card>
      ) : null}

      <div className="grid gap-4 xl:grid-cols-[1.6fr_1fr]">
        <Card>
          <CardHeader>
            <CardTitleWithTooltip
              title="Open Positions"
              subtitle="Powered by /me/positions with market status and latest mark price."
            />
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
                      onClick={() => onPositionSortClick("market")}
                    >
                      Market
                      {renderPositionSortIcon("market")}
                    </Button>
                  </TableHead>
                  <TableHead>
                    <Button
                      type="button"
                      variant="ghost"
                      size="xs"
                      className="-ml-2"
                      onClick={() => onPositionSortClick("outcome")}
                    >
                      Outcome
                      {renderPositionSortIcon("outcome")}
                    </Button>
                  </TableHead>
                  <TableHead>
                    <Button
                      type="button"
                      variant="ghost"
                      size="xs"
                      className="-ml-2"
                      onClick={() => onPositionSortClick("netQuantity")}
                    >
                      Net Qty
                      {renderPositionSortIcon("netQuantity")}
                    </Button>
                  </TableHead>
                  <TableHead>
                    <Button
                      type="button"
                      variant="ghost"
                      size="xs"
                      className="-ml-2"
                      onClick={() => onPositionSortClick("avgEntry")}
                    >
                      Avg Entry (c)
                      {renderPositionSortIcon("avgEntry")}
                    </Button>
                  </TableHead>
                  <TableHead>
                    <Button
                      type="button"
                      variant="ghost"
                      size="xs"
                      className="-ml-2"
                      onClick={() => onPositionSortClick("mark")}
                    >
                      Mark (c)
                      {renderPositionSortIcon("mark")}
                    </Button>
                  </TableHead>
                  <TableHead>
                    <Button
                      type="button"
                      variant="ghost"
                      size="xs"
                      className="-ml-2"
                      onClick={() => onPositionSortClick("markValue")}
                    >
                      Mark Value
                      {renderPositionSortIcon("markValue")}
                    </Button>
                  </TableHead>
                  <TableHead>
                    <Button
                      type="button"
                      variant="ghost"
                      size="xs"
                      className="-ml-2"
                      onClick={() => onPositionSortClick("realizedPnl")}
                    >
                      Realized PnL
                      {renderPositionSortIcon("realizedPnl")}
                    </Button>
                  </TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {pagedPositions.map((position) => (
                  <TableRow key={`${position.market_id}-${position.outcome}`}>
                    <TableCell>
                      <Button
                        variant="link"
                        size="xs"
                        nativeButton={false}
                        className="h-auto justify-start p-0 text-left no-underline hover:no-underline"
                        render={<Link to="/markets/$marketId" params={{ marketId: position.market_id }} />}
                      >
                        <span className="space-y-1">
                          <span className="block font-medium">{position.question ?? position.market_id}</span>
                          <span className="block text-xs text-muted-foreground">{position.market_id}</span>
                        </span>
                      </Button>
                    </TableCell>
                    <TableCell>{position.outcome}</TableCell>
                    <TableCell>{formatShares(position.net_quantity_minor)}</TableCell>
                    <TableCell>{formatTicks(position.avg_entry_price_ticks)}</TableCell>
                    <TableCell>{formatTicks(position.mark_price_ticks)}</TableCell>
                    <TableCell>{formatUsdFromCents(position.mark_value_minor)}</TableCell>
                    <TableCell>{formatUsdFromCents(position.realized_pnl_minor)}</TableCell>
                  </TableRow>
                ))}
                {sortedPositions.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={7} className="text-muted-foreground">
                      No open positions.
                    </TableCell>
                  </TableRow>
                ) : null}
              </TableBody>
            </Table>
            <div className="flex flex-wrap items-center justify-between gap-2">
              <div className="flex items-center gap-1">
                <span className="text-xs text-muted-foreground">Rows</span>
                {POSITION_PAGE_SIZES.map((size) => (
                  <Button
                    key={size}
                    type="button"
                    variant={positionsPageSize === size ? "default" : "outline"}
                    size="xs"
                    onClick={() => {
                      setPositionsPageSize(size);
                      setPositionsPage(1);
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
                  disabled={positionsPage <= 1}
                  onClick={() => setPositionsPage((current) => Math.max(1, current - 1))}
                >
                  Previous
                </Button>
                <span className="text-xs text-muted-foreground">
                  Page {positionsPage} of {positionsPageCount}
                </span>
                <Button
                  type="button"
                  variant="outline"
                  size="xs"
                  disabled={positionsPage >= positionsPageCount}
                  onClick={() =>
                    setPositionsPage((current) => Math.min(positionsPageCount, current + 1))
                  }
                >
                  Next
                </Button>
              </div>
            </div>
          </CardContent>
        </Card>

        <div className="space-y-4">
          <Card>
            <CardHeader>
              <CardTitleWithTooltip
                title="Portfolio History"
                subtitle="Total equity snapshots from /me/portfolio-history (includes unrealized position value)."
              />
            </CardHeader>
            <CardContent className="space-y-3">
              {historyPoints.length < 2 ? (
                <div className="text-sm text-muted-foreground">Not enough history points yet.</div>
              ) : (
                <svg
                  viewBox="0 0 640 220"
                  className="h-52 w-full border"
                  role="img"
                  aria-label="Portfolio history chart"
                >
                  <polyline
                    fill="none"
                    stroke="currentColor"
                    strokeWidth="2"
                    points={historyPolyline}
                  />
                </svg>
              )}

              <div className="space-y-1 text-xs text-muted-foreground">
                <div>Latest snapshot: {formatTime(history.at(0)?.created_at)}</div>
                <div>Latest total equity: {formatUsdFromCents(history.at(0)?.total_equity_minor)}</div>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitleWithTooltip
                title="Funding Actions"
                subtitle="Create withdrawal requests and track pending states."
              />
              <CardAction>
                <Button
                  type="button"
                  size="icon-sm"
                  variant="outline"
                  aria-label="Refresh funding actions"
                  disabled={isRefreshing}
                  onClick={() => void refresh()}
                >
                  <RiRefreshLine className={isRefreshing ? "size-4 animate-spin" : "size-4"} />
                </Button>
              </CardAction>
            </CardHeader>
            <CardContent className="space-y-3">
              <div className="flex flex-wrap items-end gap-2">
                <div className="space-y-1">
                  <div className="text-xs text-muted-foreground">Deposit</div>
                  <Input
                    value={depositAmount}
                    onChange={(event) => setDepositAmount(event.target.value)}
                    className="w-48"
                  />
                </div>
                <Button
                  disabled={depositing}
                  onClick={async () => {
                    const parsed = Number(depositAmount);
                    if (!Number.isInteger(parsed) || parsed <= 0) {
                      toast.error("Deposit amount must be an integer greater than 0.");
                      return;
                    }

                    setDepositing(true);
                    const toastId = toast.loading("Submitting deposit...");
                    try {
                      const result = await api.createDepositRequest(apiBase, auth, {
                        amount_minor: parsed,
                      });
                      toast.success(`Deposit offer created: ${result.deposit_id}`, {
                        id: toastId,
                      });
                      await refresh();
                    } catch (err: unknown) {
                      toast.error("Deposit failed.", {
                        id: toastId,
                        description: describeError(err),
                      });
                    } finally {
                      setDepositing(false);
                    }
                  }}
                >
                  {depositing ? "Submitting..." : "Create Deposit"}
                </Button>
              </div>
              <div className="flex flex-wrap items-end gap-2">
                <div className="space-y-1">
                  <div className="text-xs text-muted-foreground">Withdraw</div>
                  <Input
                    value={withdrawAmount}
                    onChange={(event) => setWithdrawAmount(event.target.value)}
                    className="w-48"
                  />
                </div>
                <Button
                  disabled={withdrawing}
                  onClick={async () => {
                    const parsed = Number(withdrawAmount);
                    if (!Number.isInteger(parsed) || parsed <= 0) {
                      toast.error("Withdrawal amount must be an integer greater than 0.");
                      return;
                    }

                    setWithdrawing(true);
                    const toastId = toast.loading("Submitting withdrawal...");
                    try {
                      const result = await api.createWithdrawalRequest(apiBase, auth, {
                        amount_minor: parsed,
                      });
                      toast.success(`Withdrawal requested: ${result.withdrawal_id}`, {
                        id: toastId,
                        description: result.create_intent_state,
                      });
                      await refresh();
                    } catch (err: unknown) {
                      toast.error("Withdrawal failed.", {
                        id: toastId,
                        description: describeError(err),
                      });
                    } finally {
                      setWithdrawing(false);
                    }
                  }}
                >
                  {withdrawing ? "Submitting..." : "Request withdrawal"}
                </Button>
              </div>
            </CardContent>
          </Card>
        </div>
      </div>

      <Card>
        <CardHeader>
          <CardTitleWithTooltip
            title="Activity"
            subtitle="Open orders, order history, fills, pending deposits/withdrawals, and receipts."
          />
        </CardHeader>
        <CardContent>
          <Tabs
            value={activityTab}
            onValueChange={(value) => setActivityTab((value as ActivityTab | null) ?? "open-orders")}
          >
            <TabsList variant="line" className="w-full justify-start overflow-x-auto overflow-y-hidden">
              <TabsTrigger value="open-orders">Open orders ({openOrders.length})</TabsTrigger>
              <TabsTrigger value="orders">Orders ({orders.length})</TabsTrigger>
              <TabsTrigger value="fills">Fills ({fills.length})</TabsTrigger>
              <TabsTrigger value="deposits">Deposits ({deposits.length})</TabsTrigger>
              <TabsTrigger value="withdrawals">Withdrawals ({withdrawals.length})</TabsTrigger>
              <TabsTrigger value="receipts">Receipts ({receipts.length})</TabsTrigger>
            </TabsList>

            <TabsContent value="open-orders" className="space-y-3">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>
                      <Button
                        type="button"
                        variant="ghost"
                        size="xs"
                        className="-ml-2"
                        onClick={() => onOpenOrdersSortClick("order")}
                      >
                        Order
                        {renderSortIcon(openOrdersSort, "order")}
                      </Button>
                    </TableHead>
                    <TableHead>
                      <Button
                        type="button"
                        variant="ghost"
                        size="xs"
                        className="-ml-2"
                        onClick={() => onOpenOrdersSortClick("market")}
                      >
                        Market
                        {renderSortIcon(openOrdersSort, "market")}
                      </Button>
                    </TableHead>
                    <TableHead>
                      <Button
                        type="button"
                        variant="ghost"
                        size="xs"
                        className="-ml-2"
                        onClick={() => onOpenOrdersSortClick("status")}
                      >
                        Status
                        {renderSortIcon(openOrdersSort, "status")}
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
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {pagedOpenOrders.map((order) => (
                    <TableRow key={order.order_id}>
                      <TableCell>{order.order_id}</TableCell>
                      <TableCell>
                        <Button
                          variant="link"
                          size="xs"
                          nativeButton={false}
                          render={<Link to="/markets/$marketId" params={{ marketId: order.market_id }} />}
                        >
                          {order.market_id}
                        </Button>
                      </TableCell>
                      <TableCell>{order.status}</TableCell>
                      <TableCell>{order.outcome}</TableCell>
                      <TableCell>{formatTicks(order.price_ticks)}</TableCell>
                      <TableCell>{formatShares(order.remaining_minor)}</TableCell>
                    </TableRow>
                  ))}
                  {sortedActivityOpenOrders.length === 0 ? (
                    <TableRow>
                      <TableCell colSpan={6} className="text-muted-foreground">
                        No open orders.
                      </TableCell>
                    </TableRow>
                  ) : null}
                </TableBody>
              </Table>
              {renderActivityPagination(
                "open-orders",
                openOrdersPage,
                openOrdersPageCount,
                setOpenOrdersPage,
              )}
            </TabsContent>

            <TabsContent value="orders" className="space-y-3">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>
                      <Button
                        type="button"
                        variant="ghost"
                        size="xs"
                        className="-ml-2"
                        onClick={() => onOrdersSortClick("order")}
                      >
                        Order
                        {renderSortIcon(ordersSort, "order")}
                      </Button>
                    </TableHead>
                    <TableHead>
                      <Button
                        type="button"
                        variant="ghost"
                        size="xs"
                        className="-ml-2"
                        onClick={() => onOrdersSortClick("status")}
                      >
                        Status
                        {renderSortIcon(ordersSort, "status")}
                      </Button>
                    </TableHead>
                    <TableHead>
                      <Button
                        type="button"
                        variant="ghost"
                        size="xs"
                        className="-ml-2"
                        onClick={() => onOrdersSortClick("market")}
                      >
                        Market
                        {renderSortIcon(ordersSort, "market")}
                      </Button>
                    </TableHead>
                    <TableHead>
                      <Button
                        type="button"
                        variant="ghost"
                        size="xs"
                        className="-ml-2"
                        onClick={() => onOrdersSortClick("side")}
                      >
                        Side
                        {renderSortIcon(ordersSort, "side")}
                      </Button>
                    </TableHead>
                    <TableHead>
                      <Button
                        type="button"
                        variant="ghost"
                        size="xs"
                        className="-ml-2"
                        onClick={() => onOrdersSortClick("outcome")}
                      >
                        Outcome
                        {renderSortIcon(ordersSort, "outcome")}
                      </Button>
                    </TableHead>
                    <TableHead>
                      <Button
                        type="button"
                        variant="ghost"
                        size="xs"
                        className="-ml-2"
                        onClick={() => onOrdersSortClick("qty")}
                      >
                        Qty
                        {renderSortIcon(ordersSort, "qty")}
                      </Button>
                    </TableHead>
                    <TableHead>
                      <Button
                        type="button"
                        variant="ghost"
                        size="xs"
                        className="-ml-2"
                        onClick={() => onOrdersSortClick("updated")}
                      >
                        Updated
                        {renderSortIcon(ordersSort, "updated")}
                      </Button>
                    </TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {pagedOrders.map((order) => (
                    <TableRow key={order.order_id}>
                      <TableCell>{order.order_id}</TableCell>
                      <TableCell>{order.status}</TableCell>
                      <TableCell>
                        <Button
                          variant="link"
                          size="xs"
                          nativeButton={false}
                          render={<Link to="/markets/$marketId" params={{ marketId: order.market_id }} />}
                        >
                          {order.market_id}
                        </Button>
                      </TableCell>
                      <TableCell>{order.side}</TableCell>
                      <TableCell>{order.outcome}</TableCell>
                      <TableCell>{formatShares(order.quantity_minor)}</TableCell>
                      <TableCell>{formatTime(order.updated_at)}</TableCell>
                    </TableRow>
                  ))}
                  {sortedActivityOrders.length === 0 ? (
                    <TableRow>
                      <TableCell colSpan={7} className="text-muted-foreground">
                        No orders.
                      </TableCell>
                    </TableRow>
                  ) : null}
                </TableBody>
              </Table>
              {renderActivityPagination("orders", ordersPage, ordersPageCount, setOrdersPage)}
            </TabsContent>

            <TabsContent value="fills" className="space-y-3">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>
                      <Button
                        type="button"
                        variant="ghost"
                        size="xs"
                        className="-ml-2"
                        onClick={() => onFillsSortClick("fill")}
                      >
                        Fill
                        {renderSortIcon(fillsSort, "fill")}
                      </Button>
                    </TableHead>
                    <TableHead>
                      <Button
                        type="button"
                        variant="ghost"
                        size="xs"
                        className="-ml-2"
                        onClick={() => onFillsSortClick("role")}
                      >
                        Role
                        {renderSortIcon(fillsSort, "role")}
                      </Button>
                    </TableHead>
                    <TableHead>
                      <Button
                        type="button"
                        variant="ghost"
                        size="xs"
                        className="-ml-2"
                        onClick={() => onFillsSortClick("market")}
                      >
                        Market
                        {renderSortIcon(fillsSort, "market")}
                      </Button>
                    </TableHead>
                    <TableHead>
                      <Button
                        type="button"
                        variant="ghost"
                        size="xs"
                        className="-ml-2"
                        onClick={() => onFillsSortClick("outcome")}
                      >
                        Outcome
                        {renderSortIcon(fillsSort, "outcome")}
                      </Button>
                    </TableHead>
                    <TableHead>
                      <Button
                        type="button"
                        variant="ghost"
                        size="xs"
                        className="-ml-2"
                        onClick={() => onFillsSortClick("price")}
                      >
                        Price (c)
                        {renderSortIcon(fillsSort, "price")}
                      </Button>
                    </TableHead>
                    <TableHead>
                      <Button
                        type="button"
                        variant="ghost"
                        size="xs"
                        className="-ml-2"
                        onClick={() => onFillsSortClick("qty")}
                      >
                        Qty
                        {renderSortIcon(fillsSort, "qty")}
                      </Button>
                    </TableHead>
                    <TableHead>
                      <Button
                        type="button"
                        variant="ghost"
                        size="xs"
                        className="-ml-2"
                        onClick={() => onFillsSortClick("matched")}
                      >
                        Matched
                        {renderSortIcon(fillsSort, "matched")}
                      </Button>
                    </TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {pagedFills.map((fill) => (
                    <TableRow key={fill.fill_id}>
                      <TableCell>{fill.fill_id}</TableCell>
                      <TableCell>{fill.perspective_role}</TableCell>
                      <TableCell>
                        <Button
                          variant="link"
                          size="xs"
                          nativeButton={false}
                          render={<Link to="/markets/$marketId" params={{ marketId: fill.market_id }} />}
                        >
                          {fill.market_id}
                        </Button>
                      </TableCell>
                      <TableCell>{fill.outcome}</TableCell>
                      <TableCell>{formatTicks(fill.price_ticks)}</TableCell>
                      <TableCell>{formatShares(fill.quantity_minor)}</TableCell>
                      <TableCell>{formatTime(fill.matched_at)}</TableCell>
                    </TableRow>
                  ))}
                  {sortedActivityFills.length === 0 ? (
                    <TableRow>
                      <TableCell colSpan={7} className="text-muted-foreground">
                        No fills.
                      </TableCell>
                    </TableRow>
                  ) : null}
                </TableBody>
              </Table>
              {renderActivityPagination("fills", fillsPage, fillsPageCount, setFillsPage)}
            </TabsContent>

            <TabsContent value="deposits" className="space-y-3">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>
                      <Button
                        type="button"
                        variant="ghost"
                        size="xs"
                        className="-ml-2"
                        onClick={() => onDepositsSortClick("deposit")}
                      >
                        Deposit ID
                        {renderSortIcon(depositsSort, "deposit")}
                      </Button>
                    </TableHead>
                    <TableHead>
                      <Button
                        type="button"
                        variant="ghost"
                        size="xs"
                        className="-ml-2"
                        onClick={() => onDepositsSortClick("expected")}
                      >
                        Expected
                        {renderSortIcon(depositsSort, "expected")}
                      </Button>
                    </TableHead>
                    <TableHead>
                      <Button
                        type="button"
                        variant="ghost"
                        size="xs"
                        className="-ml-2"
                        onClick={() => onDepositsSortClick("status")}
                      >
                        Status
                        {renderSortIcon(depositsSort, "status")}
                      </Button>
                    </TableHead>
                    <TableHead>
                      <Button
                        type="button"
                        variant="ghost"
                        size="xs"
                        className="-ml-2"
                        onClick={() => onDepositsSortClick("created")}
                      >
                        Created
                        {renderSortIcon(depositsSort, "created")}
                      </Button>
                    </TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {pagedDeposits.map((deposit) => (
                    <TableRow key={deposit.contract_id}>
                      <TableCell>{deposit.deposit_id}</TableCell>
                      <TableCell>{formatUsdFromCents(deposit.expected_amount_minor)}</TableCell>
                      <TableCell>{deposit.latest_status_class ?? "Pending"}</TableCell>
                      <TableCell>{formatTime(deposit.created_at)}</TableCell>
                    </TableRow>
                  ))}
                  {sortedActivityDeposits.length === 0 ? (
                    <TableRow>
                      <TableCell colSpan={4} className="text-muted-foreground">
                        No pending deposits.
                      </TableCell>
                    </TableRow>
                  ) : null}
                </TableBody>
              </Table>
              {renderActivityPagination("deposits", depositsPage, depositsPageCount, setDepositsPage)}
            </TabsContent>

            <TabsContent value="withdrawals" className="space-y-3">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>
                      <Button
                        type="button"
                        variant="ghost"
                        size="xs"
                        className="-ml-2"
                        onClick={() => onWithdrawalsSortClick("withdrawal")}
                      >
                        Withdrawal ID
                        {renderSortIcon(withdrawalsSort, "withdrawal")}
                      </Button>
                    </TableHead>
                    <TableHead>
                      <Button
                        type="button"
                        variant="ghost"
                        size="xs"
                        className="-ml-2"
                        onClick={() => onWithdrawalsSortClick("amount")}
                      >
                        Amount
                        {renderSortIcon(withdrawalsSort, "amount")}
                      </Button>
                    </TableHead>
                    <TableHead>
                      <Button
                        type="button"
                        variant="ghost"
                        size="xs"
                        className="-ml-2"
                        onClick={() => onWithdrawalsSortClick("state")}
                      >
                        State
                        {renderSortIcon(withdrawalsSort, "state")}
                      </Button>
                    </TableHead>
                    <TableHead>
                      <Button
                        type="button"
                        variant="ghost"
                        size="xs"
                        className="-ml-2"
                        onClick={() => onWithdrawalsSortClick("status")}
                      >
                        Status
                        {renderSortIcon(withdrawalsSort, "status")}
                      </Button>
                    </TableHead>
                    <TableHead>
                      <Button
                        type="button"
                        variant="ghost"
                        size="xs"
                        className="-ml-2"
                        onClick={() => onWithdrawalsSortClick("created")}
                      >
                        Created
                        {renderSortIcon(withdrawalsSort, "created")}
                      </Button>
                    </TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {pagedWithdrawals.map((withdrawal) => (
                    <TableRow key={withdrawal.contract_id}>
                      <TableCell>{withdrawal.withdrawal_id}</TableCell>
                      <TableCell>{formatUsdFromCents(withdrawal.amount_minor)}</TableCell>
                      <TableCell>{withdrawal.pending_state}</TableCell>
                      <TableCell>{withdrawal.latest_status_class ?? "Pending"}</TableCell>
                      <TableCell>{formatTime(withdrawal.created_at)}</TableCell>
                    </TableRow>
                  ))}
                  {sortedActivityWithdrawals.length === 0 ? (
                    <TableRow>
                      <TableCell colSpan={5} className="text-muted-foreground">
                        No pending withdrawals.
                      </TableCell>
                    </TableRow>
                  ) : null}
                </TableBody>
              </Table>
              {renderActivityPagination(
                "withdrawals",
                withdrawalsPage,
                withdrawalsPageCount,
                setWithdrawalsPage,
              )}
            </TabsContent>

            <TabsContent value="receipts" className="space-y-3">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>
                      <Button
                        type="button"
                        variant="ghost"
                        size="xs"
                        className="-ml-2"
                        onClick={() => onReceiptsSortClick("receipt")}
                      >
                        Receipt
                        {renderSortIcon(receiptsSort, "receipt")}
                      </Button>
                    </TableHead>
                    <TableHead>
                      <Button
                        type="button"
                        variant="ghost"
                        size="xs"
                        className="-ml-2"
                        onClick={() => onReceiptsSortClick("kind")}
                      >
                        Kind
                        {renderSortIcon(receiptsSort, "kind")}
                      </Button>
                    </TableHead>
                    <TableHead>
                      <Button
                        type="button"
                        variant="ghost"
                        size="xs"
                        className="-ml-2"
                        onClick={() => onReceiptsSortClick("status")}
                      >
                        Status
                        {renderSortIcon(receiptsSort, "status")}
                      </Button>
                    </TableHead>
                    <TableHead>
                      <Button
                        type="button"
                        variant="ghost"
                        size="xs"
                        className="-ml-2"
                        onClick={() => onReceiptsSortClick("amount")}
                      >
                        Amount
                        {renderSortIcon(receiptsSort, "amount")}
                      </Button>
                    </TableHead>
                    <TableHead>
                      <Button
                        type="button"
                        variant="ghost"
                        size="xs"
                        className="-ml-2"
                        onClick={() => onReceiptsSortClick("reference")}
                      >
                        Reference
                        {renderSortIcon(receiptsSort, "reference")}
                      </Button>
                    </TableHead>
                    <TableHead>
                      <Button
                        type="button"
                        variant="ghost"
                        size="xs"
                        className="-ml-2"
                        onClick={() => onReceiptsSortClick("created")}
                      >
                        Created
                        {renderSortIcon(receiptsSort, "created")}
                      </Button>
                    </TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {pagedReceipts.map((receipt) => (
                    <TableRow key={receipt.receipt_id}>
                      <TableCell>{receipt.receipt_id}</TableCell>
                      <TableCell>
                        <Badge variant="outline">{receipt.kind}</Badge>
                      </TableCell>
                      <TableCell>{receipt.status}</TableCell>
                      <TableCell>{formatUsdFromCents(receipt.amount_minor)}</TableCell>
                      <TableCell>{receipt.reference_id}</TableCell>
                      <TableCell>{formatTime(receipt.created_at)}</TableCell>
                    </TableRow>
                  ))}
                  {sortedActivityReceipts.length === 0 ? (
                    <TableRow>
                      <TableCell colSpan={6} className="text-muted-foreground">
                        No receipts yet.
                      </TableCell>
                    </TableRow>
                  ) : null}
                </TableBody>
              </Table>
              {renderActivityPagination("receipts", receiptsPage, receiptsPageCount, setReceiptsPage)}
            </TabsContent>
          </Tabs>
        </CardContent>
      </Card>
      </div>
    </TooltipProvider>
  );
}
