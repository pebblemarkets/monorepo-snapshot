import { Link } from "@tanstack/react-router";

import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { withAssetVariant } from "@/lib/assets";
import { formatMarketStatusLabel } from "@/lib/market-status";
import { cn } from "@/lib/utils";

export type DiscoveryMarketCardRow = {
  market_id: string;
  question: string;
  status: string;
  outcomes: string[];
  resolved_outcome: string | null;
  created_at: string;
  category: string;
  tags: string[];
  featured: boolean;
  volume_24h_minor: number;
  open_interest_minor: number;
  last_traded_price_ticks: number | null;
  last_traded_at: string | null;
  card_background_image_url: string | null;
  hero_background_image_url: string | null;
  thumbnail_image_url: string | null;
};

const USD_FORMATTER = new Intl.NumberFormat(undefined, {
  style: "currency",
  currency: "USD",
  minimumFractionDigits: 0,
  maximumFractionDigits: 2,
});

function formatUsdFromCents(value: number | null | undefined): string {
  if (value === null || value === undefined) {
    return "-";
  }
  return USD_FORMATTER.format(value / 100);
}

type DiscoveryMarketCardProps = {
  market: DiscoveryMarketCardRow;
};

export function DiscoveryMarketCard(props: DiscoveryMarketCardProps) {
  const { market } = props;
  const backgroundImageUrl = withAssetVariant(market.card_background_image_url, 800, 70);
  const chance =
    market.last_traded_price_ticks === null
      ? null
      : Math.max(0, Math.min(100, market.last_traded_price_ticks));
  const cardStyle = backgroundImageUrl
    ? { backgroundImage: `url("${backgroundImageUrl}")` }
    : undefined;

  return (
    <Link
      to="/markets/$marketId"
      params={{ marketId: market.market_id }}
      className="block h-full rounded-none focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2"
    >
      <Card
        className={cn(
          "relative h-full overflow-hidden border-border/80 transition-colors hover:border-primary/40 hover:bg-muted/10",
          backgroundImageUrl ? "bg-cover bg-center bg-no-repeat" : "",
        )}
        style={cardStyle}
      >
        {backgroundImageUrl ? (
          <div className="pointer-events-none absolute inset-0 bg-gradient-to-t from-background via-background/94 to-background/82" />
        ) : null}
        <div className="relative flex h-full flex-col">
          <CardHeader className="space-y-2 pb-2">
            <div className="flex flex-wrap items-center justify-between gap-2">
              <Badge variant={market.status === "Open" ? "default" : "outline"}>
                {formatMarketStatusLabel(market.status)}
              </Badge>
              <div className="flex flex-wrap items-center gap-1">
                <Badge variant="secondary">{market.category}</Badge>
                {market.featured ? <Badge>Featured</Badge> : null}
              </div>
            </div>
            <CardTitle className="text-base leading-snug">{market.question}</CardTitle>
            {market.tags.length > 0 ? (
              <div className="flex flex-wrap gap-1 pt-1">
                {market.tags.map((tag) => (
                  <Badge key={`${market.market_id}-${tag}`} variant="outline">
                    {tag}
                  </Badge>
                ))}
              </div>
            ) : null}
          </CardHeader>
          <CardContent className="mt-auto space-y-2 pt-4">
            <div className="flex items-end justify-between gap-4">
              <div className="space-y-0">
                <div className="text-[10px] uppercase tracking-[0.1em] text-muted-foreground">
                  24h Volume
                </div>
                <div className="text-xs leading-tight font-medium">{formatUsdFromCents(market.volume_24h_minor)}</div>
                <div className="pt-0.5 text-[10px] uppercase tracking-[0.1em] text-muted-foreground">
                  Open Interest
                </div>
                <div className="text-xs leading-tight font-medium">
                  {formatUsdFromCents(market.open_interest_minor)}
                </div>
              </div>
              <div className="text-right">
                <div className="text-4xl leading-none font-semibold">{chance === null ? "-" : `${chance}%`}</div>
                <div className="pt-0.5 text-[10px] uppercase tracking-[0.1em] text-muted-foreground">
                  Chance
                </div>
              </div>
            </div>
            {market.status === "Resolved" && market.resolved_outcome ? (
              <Badge variant="outline">Resolved outcome: {market.resolved_outcome}</Badge>
            ) : null}
          </CardContent>
        </div>
      </Card>
    </Link>
  );
}
