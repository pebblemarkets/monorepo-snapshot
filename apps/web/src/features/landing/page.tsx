import { Link } from "@tanstack/react-router";
import React from "react";

import { useSessionContext } from "@/app/session";
import { describeError } from "@/app/session";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import {
  type DiscoveryMarketCardRow,
  DiscoveryMarketCard,
} from "@/features/discovery/components/market-card";
import * as api from "@/lib/api";
import { withRetry } from "@/lib/retry";

const USD_FORMATTER = new Intl.NumberFormat(undefined, {
  style: "currency",
  currency: "USD",
  minimumFractionDigits: 0,
  maximumFractionDigits: 2,
});

function formatCount(value: number | null | undefined): string {
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

function StatLabelWithTooltip({ label, tooltip }: { label: string; tooltip: string }) {
  return (
    <Tooltip>
      <TooltipTrigger render={<CardDescription className="w-fit cursor-help" />}>
        {label}
      </TooltipTrigger>
      <TooltipContent align="start">{tooltip}</TooltipContent>
    </Tooltip>
  );
}

export function LandingPage() {
  const { apiBase, session } = useSessionContext();
  const [overview, setOverview] = React.useState<api.PublicStatsOverviewView | null>(null);
  const [featuredMarkets, setFeaturedMarkets] = React.useState<DiscoveryMarketCardRow[]>([]);
  const [status, setStatus] = React.useState<string>("loading...");

  const refresh = React.useCallback(async () => {
    setStatus("loading...");
    try {
      const [nextOverview, marketRows, marketStats] = await withRetry(
        async () =>
          Promise.all([
            api.getPublicStatsOverview(apiBase),
            api.listMarkets(apiBase),
            api.listPublicMarketStats(apiBase, 200),
          ]),
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
        // Metadata is optional for landing defaults.
      }

      const statsByMarketId = new Map(marketStats.map((row) => [row.market_id, row]));
      const metadataByMarketId = new Map(marketMetadata.map((row) => [row.market_id, row]));

      const nextFeaturedMarkets = marketRows
        .map((market) => {
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
            volume_24h_minor: stats?.volume_24h_minor ?? 0,
            open_interest_minor: stats?.open_interest_minor ?? 0,
            last_traded_price_ticks: stats?.last_traded_price_ticks ?? null,
            last_traded_at: stats?.last_traded_at ?? null,
            card_background_image_url: metadata?.card_background_image_url ?? null,
            hero_background_image_url: metadata?.hero_background_image_url ?? null,
            thumbnail_image_url: metadata?.thumbnail_image_url ?? null,
          } satisfies DiscoveryMarketCardRow;
        })
        .sort(
          (left, right) =>
            Number(right.featured) - Number(left.featured) ||
            right.volume_24h_minor - left.volume_24h_minor ||
            Date.parse(right.created_at) - Date.parse(left.created_at),
        )
        .slice(0, 6);

      setOverview(nextOverview);
      setFeaturedMarkets(nextFeaturedMarkets);
      setStatus("ok");
    } catch (err: unknown) {
      setOverview(null);
      setFeaturedMarkets([]);
      setStatus(describeError(err));
    }
  }, [apiBase]);

  React.useEffect(() => {
    void refresh();
  }, [refresh]);

  const isLoading = status === "loading...";
  const isError = !isLoading && status !== "ok";
  const hasUserSession = Boolean(session?.account_id);

  return (
    <TooltipProvider>
      <div className="space-y-6">
      <Card className="hero-gradient-motion relative overflow-hidden border-none bg-gradient-to-br from-emerald-100 via-cyan-50 to-background py-0 ring-1 ring-border dark:from-emerald-950/50 dark:via-cyan-950/20 dark:to-background">
        <div className="hero-gradient-focus-a pointer-events-none absolute -right-20 -top-20 h-72 w-72 rounded-full bg-emerald-300/35 blur-3xl dark:bg-emerald-500/15" />
        <div className="hero-gradient-focus-b pointer-events-none absolute -bottom-24 left-1/3 h-72 w-72 rounded-full bg-cyan-200/40 blur-3xl dark:bg-cyan-500/10" />

        <CardContent className="relative space-y-4 p-6 md:p-10">
          <h1 className="max-w-4xl text-3xl font-bold tracking-[-0.07em] md:text-5xl">
            pebble
          </h1>
          <p className="max-w-3xl text-sm text-muted-foreground md:text-base">
            The privacy focused institutional grade outcome trading platform on Canton.
          </p>

          <div className="flex flex-wrap items-center gap-2">
            <Button nativeButton={false} render={<Link to="/markets" />}>
              Browse markets
            </Button>
            <Button variant="outline" nativeButton={false} render={<Link to="/portfolio" />}>
              {hasUserSession ? "Open portfolio" : "Login or Register"}
            </Button>
          </div>
        </CardContent>
      </Card>

      {isError ? (
        <Alert variant="destructive">
          <AlertTitle>Landing metrics unavailable</AlertTitle>
          <AlertDescription>{status}</AlertDescription>
        </Alert>
      ) : null}

      <section className="grid gap-3 md:grid-cols-2 lg:grid-cols-4">
        <Card>
          <CardHeader className="pb-2">
            <StatLabelWithTooltip
              label="Total Value Locked"
              tooltip="Derived from latest cleared liabilities."
            />
            <CardTitle className="text-3xl font-semibold tracking-tight">
              {formatUsdFromCents(overview?.tvl_minor)}
            </CardTitle>
          </CardHeader>
        </Card>

        <Card>
          <CardHeader className="pb-2">
            <StatLabelWithTooltip
              label="24h Volume"
              tooltip="Notional from executed fills in the last 24 hours."
            />
            <CardTitle className="text-3xl font-semibold tracking-tight">
              {formatUsdFromCents(overview?.volume_24h_minor)}
            </CardTitle>
          </CardHeader>
        </Card>

        <Card>
          <CardHeader className="pb-2">
            <StatLabelWithTooltip
              label="Live Markets"
              tooltip="Currently open markets."
            />
            <CardTitle className="text-3xl font-semibold tracking-tight">
              {formatCount(overview?.markets_open)}
            </CardTitle>
          </CardHeader>
        </Card>

        <Card>
          <CardHeader className="pb-2">
            <StatLabelWithTooltip
              label="24h Fill Count"
              tooltip="Executed fills in the last 24 hours."
            />
            <CardTitle className="text-3xl font-semibold tracking-tight">
              {formatCount(overview?.fills_24h)}
            </CardTitle>
          </CardHeader>
        </Card>
      </section>

      <section>
        <Card>
          <CardHeader>
            <CardTitle>Featured Markets</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            {featuredMarkets.length === 0 ? (
              <p className="text-sm text-muted-foreground">
                {isLoading ? "Loading featured markets..." : "No market rows available."}
              </p>
            ) : (
              <div className="grid gap-3 md:grid-cols-2 lg:grid-cols-3">
                {featuredMarkets.map((market) => (
                  <DiscoveryMarketCard
                    key={market.market_id}
                    market={market}
                  />
                ))}
              </div>
            )}
            <div className="flex justify-center pt-1">
              <Button variant="outline" nativeButton={false} render={<Link to="/markets" />}>
                Explore more markets
              </Button>
            </div>
          </CardContent>
        </Card>
      </section>
      </div>
    </TooltipProvider>
  );
}
