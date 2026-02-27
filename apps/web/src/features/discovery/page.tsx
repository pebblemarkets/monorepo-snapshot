import React from "react";

import { useSessionContext } from "@/app/session";
import { describeError } from "@/app/session";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { ButtonGroup } from "@/components/ui/button-group";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
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
  type DiscoveryMarketCardRow,
  DiscoveryMarketCard,
} from "@/features/discovery/components/market-card";
import * as api from "@/lib/api";
import { withRetry } from "@/lib/retry";

type DiscoveryFilterTab = "Live" | "Resolved" | "All";
type DiscoverySort = "featured" | "newest" | "most-active" | "volume";
const DISCOVERY_STATUS_FILTERS: ReadonlyArray<DiscoveryFilterTab> = ["Live", "Resolved", "All"];

type DiscoveryMarketRow = DiscoveryMarketCardRow & {
  fills_24h: number;
};

function matchesStatusTab(status: string, tab: DiscoveryFilterTab): boolean {
  if (tab === "All") {
    return true;
  }

  if (tab === "Live") {
    return status === "Open";
  }

  return status === "Resolved";
}

function sortMarkets(rows: DiscoveryMarketRow[], sort: DiscoverySort): DiscoveryMarketRow[] {
  const next = [...rows];

  next.sort((left, right) => {
    const createdCmp = Date.parse(right.created_at) - Date.parse(left.created_at);

    if (sort === "newest") {
      return createdCmp;
    }

    if (sort === "most-active") {
      return (
        right.fills_24h - left.fills_24h ||
        right.volume_24h_minor - left.volume_24h_minor ||
        createdCmp
      );
    }

    if (sort === "volume") {
      return right.volume_24h_minor - left.volume_24h_minor || createdCmp;
    }

    return (
      Number(right.featured) - Number(left.featured) ||
      right.volume_24h_minor - left.volume_24h_minor ||
      createdCmp
    );
  });

  return next;
}

export function DiscoveryPage() {
  const { apiBase } = useSessionContext();

  const [rows, setRows] = React.useState<DiscoveryMarketRow[]>([]);
  const [status, setStatus] = React.useState<string>("loading...");

  const [search, setSearch] = React.useState<string>("");
  const [sort, setSort] = React.useState<DiscoverySort>("featured");
  const [tab, setTab] = React.useState<DiscoveryFilterTab>("Live");
  const [categoryTab, setCategoryTab] = React.useState<string>("All categories");

  const refresh = React.useCallback(async () => {
    setStatus("loading...");

    try {
      const [marketRows, marketStats] = await withRetry(
        async () =>
          Promise.all([api.listMarkets(apiBase), api.listPublicMarketStats(apiBase, 500)]),
        {
          attempts: 3,
          initialDelayMs: 250,
        },
      );

      let marketMetadata: api.MarketMetadataView[] = [];
      try {
        marketMetadata = await withRetry(
          async () => api.listMarketMetadata(apiBase, { limit: 500 }),
          {
            attempts: 2,
            initialDelayMs: 200,
          },
        );
      } catch {
        // Metadata is optional for discovery defaults.
      }

      const statsByMarketId = new Map(marketStats.map((row) => [row.market_id, row]));
      const metadataByMarketId = new Map(marketMetadata.map((row) => [row.market_id, row]));

      const nextRows: DiscoveryMarketRow[] = marketRows.map((market) => {
        const metadata = metadataByMarketId.get(market.market_id);
        const stats = statsByMarketId.get(market.market_id);

        return {
          market_id: market.market_id,
          question: market.question,
          outcomes: market.outcomes,
          status: market.status,
          resolved_outcome: market.resolved_outcome,
          created_at: market.created_at,
          category: metadata?.category ?? "General",
          tags: metadata?.tags ?? [],
          featured: metadata?.featured ?? false,
          fills_24h: stats?.fills_24h ?? 0,
          volume_24h_minor: stats?.volume_24h_minor ?? 0,
          open_interest_minor: stats?.open_interest_minor ?? 0,
          last_traded_price_ticks: stats?.last_traded_price_ticks ?? null,
          last_traded_at: stats?.last_traded_at ?? null,
          card_background_image_url: metadata?.card_background_image_url ?? null,
          hero_background_image_url: metadata?.hero_background_image_url ?? null,
          thumbnail_image_url: metadata?.thumbnail_image_url ?? null,
        };
      });

      setRows(nextRows);
      setStatus(`ok (${nextRows.length} markets)`);
    } catch (err: unknown) {
      setRows([]);
      setStatus(describeError(err));
    }
  }, [apiBase]);

  React.useEffect(() => {
    void refresh();
  }, [refresh]);

  const categories = React.useMemo(() => {
    const set = new Set<string>();
    for (const row of rows) {
      set.add(row.category);
    }

    return ["All categories", ...Array.from(set).sort((left, right) => left.localeCompare(right))];
  }, [rows]);

  React.useEffect(() => {
    if (!categories.includes(categoryTab)) {
      setCategoryTab("All categories");
    }
  }, [categories, categoryTab]);

  const filtered = React.useMemo(() => {
    const needle = search.trim().toLowerCase();

    return sortMarkets(
      rows.filter((row) => {
        if (!matchesStatusTab(row.status, tab)) {
          return false;
        }

        if (categoryTab !== "All categories" && row.category !== categoryTab) {
          return false;
        }

        if (!needle) {
          return true;
        }

        const haystack = [
          row.market_id,
          row.question,
          row.outcomes.join(" "),
          row.tags.join(" "),
          row.category,
        ]
          .join(" ")
          .toLowerCase();
        return haystack.includes(needle);
      }),
      sort,
    );
  }, [rows, search, tab, categoryTab, sort]);

  const counts = React.useMemo(() => {
    const live = rows.filter((row) => row.status === "Open").length;
    const resolved = rows.filter((row) => row.status === "Resolved").length;
    const featured = rows.filter((row) => row.featured).length;

    return {
      total: rows.length,
      live,
      resolved,
      featured,
    };
  }, [rows]);

  const isLoading = status === "loading...";
  const isError = !isLoading && !status.startsWith("ok");

  return (
    <div className="space-y-4">
      <Card>
        <CardHeader>
          <CardTitle>Market Discovery</CardTitle>
          <CardDescription>
            Search active and resolved markets, filter by category, and jump directly into the trading page.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-3 pt-0">
          <div className="grid gap-2 md:grid-cols-[1fr_auto_auto]">
            <Input
              value={search}
              onChange={(event) => setSearch(event.target.value)}
              placeholder="Search by question, market id, outcomes, or tags"
            />
            <Select
              value={sort}
              onValueChange={(value) => setSort((value as DiscoverySort | null) ?? "featured")}
            >
              <SelectTrigger>
                <SelectValue placeholder="Sort" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="featured">Featured first</SelectItem>
                <SelectItem value="newest">Newest</SelectItem>
                <SelectItem value="most-active">Most active (24h fills)</SelectItem>
                <SelectItem value="volume">Highest 24h volume</SelectItem>
              </SelectContent>
            </Select>
            <Button variant="outline" onClick={refresh}>
              Refresh
            </Button>
          </div>

          <div className="space-y-2">
            <div className="text-[11px] font-semibold uppercase tracking-[0.16em] text-muted-foreground">Status</div>
            <ButtonGroup aria-label="Market status filter">
              {DISCOVERY_STATUS_FILTERS.map((statusFilter) => (
                <Button
                  key={statusFilter}
                  size="xs"
                  variant={tab === statusFilter ? "default" : "outline"}
                  className="h-7 min-w-20"
                  onClick={() => setTab(statusFilter)}
                >
                  {statusFilter}
                </Button>
              ))}
            </ButtonGroup>
          </div>

          <div className="space-y-2">
            <div className="text-[11px] font-semibold uppercase tracking-[0.16em] text-muted-foreground">Category</div>
            <ButtonGroup aria-label="Market category filter" className="flex-wrap">
              {categories.map((category) => (
                <Button
                  key={category}
                  size="xs"
                  variant={categoryTab === category ? "default" : "outline"}
                  className="h-7"
                  onClick={() => setCategoryTab(category)}
                >
                  {category}
                </Button>
              ))}
            </ButtonGroup>
          </div>
        </CardContent>
      </Card>

      {isError ? (
        <Alert variant="destructive">
          <AlertTitle>Discovery data failed</AlertTitle>
          <AlertDescription>{status}</AlertDescription>
        </Alert>
      ) : null}

      <div className="grid gap-3 md:grid-cols-2 lg:grid-cols-3">
        {isLoading && rows.length === 0
          ? Array.from({ length: 6 }, (_, index) => (
              <Card key={`skeleton-${index}`}>
                <CardHeader>
                  <Skeleton className="h-4 w-20" />
                  <Skeleton className="h-5 w-2/3" />
                  <Skeleton className="h-4 w-1/3" />
                </CardHeader>
                <CardContent className="space-y-2">
                  <Skeleton className="h-4 w-full" />
                  <Skeleton className="h-4 w-5/6" />
                  <Skeleton className="h-7 w-28" />
                </CardContent>
              </Card>
            ))
          : null}
        {!isLoading && filtered.length === 0 ? (
          <Card className="md:col-span-2 lg:col-span-3">
            <CardContent className="py-8 text-center text-sm text-muted-foreground">
              No markets matched the current filters.
            </CardContent>
          </Card>
        ) : (
          filtered.map((market) => (
            <DiscoveryMarketCard
              key={market.market_id}
              market={market}
            />
          ))
        )}
      </div>

      <Card>
        <CardContent className="py-3 text-xs text-muted-foreground">
          Showing {filtered.length} of {counts.total} markets.
        </CardContent>
      </Card>
    </div>
  );
}
