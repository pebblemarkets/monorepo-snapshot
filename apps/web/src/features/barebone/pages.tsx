import { Link, useParams, useRouterState } from "@tanstack/react-router";
import React from "react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { NativeSelect, NativeSelectOption } from "@/components/ui/native-select";
import { Textarea } from "@/components/ui/textarea";
import { MarketImageCropper } from "@/features/admin/market-image-cropper";
import * as api from "@/lib/api";
import { formatMarketStatusLabel } from "@/lib/market-status";

const USER_API_KEY_STORAGE = "pebble.user_api_key";
const ADMIN_API_KEY_STORAGE = "pebble.admin_api_key";

type AuthContextValue = {
  apiBase: string;
  auth: api.AuthKeys;
  setUserApiKey: (value: string) => void;
  setAdminApiKey: (value: string) => void;
  session: api.SessionView | null;
  sessionStatus: string;
  refreshSession: () => Promise<void>;
};

const AuthContext = React.createContext<AuthContextValue | null>(null);

function useAuthContext(): AuthContextValue {
  const value = React.useContext(AuthContext);
  if (!value) {
    throw new Error("auth context is unavailable");
  }
  return value;
}

function getApiBase(): string {
  return (
    import.meta.env.VITE_API_URL?.replace(/\/$/, "") ?? "http://127.0.0.1:3030"
  );
}

function loadPersistedValue(key: string): string {
  if (typeof window === "undefined") {
    return "";
  }
  return window.localStorage.getItem(key) ?? "";
}

function savePersistedValue(key: string, value: string): void {
  if (typeof window === "undefined") {
    return;
  }
  if (!value) {
    window.localStorage.removeItem(key);
    return;
  }
  window.localStorage.setItem(key, value);
}

function describeError(err: unknown): string {
  if (err instanceof api.ApiClientError) {
    return `${err.message} (${err.status})`;
  }
  if (err instanceof Error) {
    return err.message;
  }
  return "unknown error";
}

function formatTime(value: string): string {
  const millis = Date.parse(value);
  if (Number.isNaN(millis)) {
    return value;
  }
  return new Date(millis).toLocaleString();
}

function formatMinor(value: number | null | undefined): string {
  if (value === null || value === undefined) {
    return "-";
  }
  return value.toLocaleString();
}

function formatTicks(value: number | null | undefined): string {
  if (value === null || value === undefined) {
    return "-";
  }
  return `${value}c`;
}

function formatRatio(value: number): string {
  return `${(value * 100).toFixed(1)}%`;
}

type BareboneRoutePrefix = "/barebone" | "/control-panel";

type BareboneRoutes = {
  home: BareboneRoutePrefix;
  markets: `${BareboneRoutePrefix}/markets`;
  funds: `${BareboneRoutePrefix}/funds`;
  ops: `${BareboneRoutePrefix}/ops`;
  marketDetail: `${BareboneRoutePrefix}/markets/$marketId`;
};

function buildBareboneRoutes(routePrefix: BareboneRoutePrefix): BareboneRoutes {
  return {
    home: routePrefix,
    markets: `${routePrefix}/markets`,
    funds: `${routePrefix}/funds`,
    ops: `${routePrefix}/ops`,
    marketDetail: `${routePrefix}/markets/$marketId`,
  };
}

type BareboneRootLayoutProps = {
  children: React.ReactNode;
  routePrefix: BareboneRoutePrefix;
  title: string;
  subtitle: string;
};

function BareboneRootLayout(props: BareboneRootLayoutProps) {
  const apiBase = React.useMemo(() => getApiBase(), []);
  const routes = React.useMemo(
    () => buildBareboneRoutes(props.routePrefix),
    [props.routePrefix],
  );
  const pathname = useRouterState({
    select: (state) => state.location.pathname,
  });
  const [userApiKey, setUserApiKey] = React.useState<string>(() =>
    loadPersistedValue(USER_API_KEY_STORAGE),
  );
  const [adminApiKey, setAdminApiKey] = React.useState<string>(() =>
    loadPersistedValue(ADMIN_API_KEY_STORAGE),
  );
  const [session, setSession] = React.useState<api.SessionView | null>(null);
  const [sessionStatus, setSessionStatus] = React.useState<string>("not checked");
  const [healthStatus, setHealthStatus] = React.useState<string>("checking...");

  React.useEffect(() => {
    savePersistedValue(USER_API_KEY_STORAGE, userApiKey.trim());
  }, [userApiKey]);

  React.useEffect(() => {
    savePersistedValue(ADMIN_API_KEY_STORAGE, adminApiKey.trim());
  }, [adminApiKey]);

  const auth = React.useMemo<api.AuthKeys>(
    () => ({
      userApiKey: userApiKey.trim(),
      adminApiKey: adminApiKey.trim(),
    }),
    [adminApiKey, userApiKey],
  );

  const refreshSession = React.useCallback(async () => {
    if (!auth.userApiKey && !auth.adminApiKey) {
      setSession(null);
      setSessionStatus("no keys configured");
      return;
    }

    setSessionStatus("checking...");
    try {
      const next = await api.getSession(apiBase, auth);
      setSession(next);
      setSessionStatus("ok");
    } catch (err: unknown) {
      setSession(null);
      setSessionStatus(describeError(err));
    }
  }, [apiBase, auth]);

  React.useEffect(() => {
    let alive = true;

    void api
      .getHealthz(apiBase)
      .then((body) => {
        if (!alive) {
          return;
        }
        setHealthStatus(
          `ok (userKeys=${body.m3_api_keys ?? 0}, adminKeys=${body.m4_admin_keys ?? 0})`,
        );
      })
      .catch((err: unknown) => {
        if (!alive) {
          return;
        }
        setHealthStatus(describeError(err));
      });

    return () => {
      alive = false;
    };
  }, [apiBase]);

  const contextValue = React.useMemo<AuthContextValue>(
    () => ({
      apiBase,
      auth,
      setUserApiKey,
      setAdminApiKey,
      session,
      sessionStatus,
      refreshSession,
    }),
    [apiBase, auth, session, sessionStatus, refreshSession],
  );

  const navLinks = [
    { label: "Home", to: routes.home },
    { label: "Markets", to: routes.markets },
    { label: "Funds", to: routes.funds },
    { label: "Ops", to: routes.ops },
  ];

  const isActive = (to: string): boolean => {
    if (to === routes.home) {
      return pathname === routes.home;
    }

    return pathname === to || pathname.startsWith(`${to}/`);
  };

  return (
    <AuthContext.Provider value={contextValue}>
      <div className="min-h-screen bg-background text-foreground">
        <header className="border-b">
          <div className="mx-auto flex w-full max-w-6xl flex-col gap-4 px-4 py-4">
            <div className="flex flex-wrap items-end justify-between gap-3">
              <div>
                <div className="text-xl font-semibold tracking-tight">{props.title}</div>
                <div className="text-xs text-muted-foreground">{props.subtitle}</div>
              </div>
              <Card size="sm" className="py-0">
                <CardContent className="px-3 py-2 text-xs">
                  API status: <span className="font-medium">{healthStatus}</span>
                </CardContent>
              </Card>
            </div>

            <nav className="flex flex-wrap items-center gap-2">
              {navLinks.map((entry) => (
                <Button
                  key={entry.to}
                  size="sm"
                  variant={isActive(entry.to) ? "default" : "outline"}
                  nativeButton={false}
                  render={<Link to={entry.to} />}
                >
                  {entry.label}
                </Button>
              ))}
            </nav>

            <AuthPanel />
          </div>
        </header>

        <main className="mx-auto w-full max-w-6xl px-4 py-6">
          {props.children}
        </main>
      </div>
    </AuthContext.Provider>
  );
}

function AuthPanel() {
  const {
    auth,
    session,
    sessionStatus,
    setAdminApiKey,
    setUserApiKey,
    refreshSession,
  } = useAuthContext();
  const [draftUserKey, setDraftUserKey] = React.useState<string>(auth.userApiKey);
  const [draftAdminKey, setDraftAdminKey] = React.useState<string>(auth.adminApiKey);

  React.useEffect(() => {
    setDraftUserKey(auth.userApiKey);
  }, [auth.userApiKey]);

  React.useEffect(() => {
    setDraftAdminKey(auth.adminApiKey);
  }, [auth.adminApiKey]);

  return (
    <Card className="gap-0 p-4">
      <div className="mb-3 flex items-center justify-between gap-2">
        <div className="text-sm font-medium">Session keys</div>
        <div className="text-xs text-muted-foreground">
          status: <span className="font-medium">{sessionStatus}</span>
        </div>
      </div>
      <div className="grid gap-3 md:grid-cols-2">
        <label className="space-y-1">
          <div className="text-xs text-muted-foreground">User API key</div>
          <Input
            className="w-full"
            value={draftUserKey}
            onChange={(event) => setDraftUserKey(event.target.value)}
            placeholder="x-api-key for /orders and /me/*"
          />
        </label>
        <label className="space-y-1">
          <div className="text-xs text-muted-foreground">Admin API key</div>
          <Input
            className="w-full"
            value={draftAdminKey}
            onChange={(event) => setDraftAdminKey(event.target.value)}
            placeholder="x-admin-key for /admin/*"
          />
        </label>
      </div>
      <div className="mt-3 flex flex-wrap items-center gap-2">
        <Button
          type="button"
          
          onClick={async () => {
            setUserApiKey(draftUserKey.trim());
            setAdminApiKey(draftAdminKey.trim());
            await refreshSession();
          }}
        >
          Save + Validate
        </Button>
        <Button
          type="button"
          variant="outline"
          onClick={async () => {
            setDraftUserKey("");
            setDraftAdminKey("");
            setUserApiKey("");
            setAdminApiKey("");
            await refreshSession();
          }}
        >
          Clear
        </Button>
        <div className="text-xs text-muted-foreground">
          account:{" "}
          <span className="font-medium">{session?.account_id ?? "-"}</span> | admin:{" "}
          <span className="font-medium">{session?.is_admin ? "yes" : "no"}</span>
        </div>
      </div>
    </Card>
  );
}

type BarebonePageProps = {
  routes: BareboneRoutes;
};

function HomePage(props: BarebonePageProps) {
  const { apiBase, auth, session, refreshSession, sessionStatus } = useAuthContext();
  const [markets, setMarkets] = React.useState<api.MarketRow[]>([]);
  const [marketsStatus, setMarketsStatus] = React.useState<string>("loading...");
  const [summary, setSummary] = React.useState<api.MySummaryView | null>(null);
  const [summaryStatus, setSummaryStatus] = React.useState<string>("idle");

  const refresh = React.useCallback(async () => {
    setMarketsStatus("loading...");
    try {
      const nextMarkets = await api.listMarkets(apiBase);
      setMarkets(nextMarkets.slice(0, 6));
      setMarketsStatus(`ok (${nextMarkets.length} total)`);
    } catch (err: unknown) {
      setMarketsStatus(describeError(err));
    }

    if (!auth.userApiKey) {
      setSummary(null);
      setSummaryStatus("set user API key to load");
      return;
    }

    setSummaryStatus("loading...");
    try {
      const nextSummary = await api.getMySummary(apiBase, auth);
      setSummary(nextSummary);
      setSummaryStatus("ok");
    } catch (err: unknown) {
      setSummary(null);
      setSummaryStatus(describeError(err));
    }
  }, [apiBase, auth]);

  React.useEffect(() => {
    void refresh();
  }, [refresh]);

  return (
    <div className="space-y-4">
      <Card className="gap-0 p-4">
        <div className="mb-2 text-sm font-medium">Session overview</div>
        <div className="text-sm">
          Session status: <span className="font-medium">{sessionStatus}</span>
        </div>
        <div className="text-sm">
          User account: <span className="font-medium">{session?.account_id ?? "-"}</span>
        </div>
        <div className="text-sm">
          Admin enabled:{" "}
          <span className="font-medium">{session?.is_admin ? "yes" : "no"}</span>
        </div>
        <div className="mt-3">
          <Button
            type="button"
            variant="outline"
            onClick={async () => {
              await refreshSession();
              await refresh();
            }}
          >
            Refresh session + data
          </Button>
        </div>
      </Card>

      <div className="grid gap-4 md:grid-cols-2">
        <Card className="gap-0 p-4">
          <div className="mb-2 flex items-center justify-between gap-2">
            <div className="text-sm font-medium">Markets</div>
            <div className="text-xs text-muted-foreground">{marketsStatus}</div>
          </div>
          {markets.length === 0 ? (
            <div className="text-sm text-muted-foreground">No markets found.</div>
          ) : (
            <ul className="space-y-2">
              {markets.map((market) => (
                <li key={market.contract_id} className="rounded-none border p-3">
                  <div className="text-sm font-medium">{market.question}</div>
                  <div className="text-xs text-muted-foreground">{formatMarketStatusLabel(market.status)}</div>
                  <div className="mt-2">
                    <Button
                      variant="link"
                      size="xs"
                      nativeButton={false}
                      render={
                        <Link
                          to={props.routes.marketDetail}
                          params={{ marketId: market.market_id }}
                        />
                      }
                    >
                      Open market details
                    </Button>
                  </div>
                </li>
              ))}
            </ul>
          )}
        </Card>

        <Card className="gap-0 p-4">
          <div className="mb-2 flex items-center justify-between gap-2">
            <div className="text-sm font-medium">Account summary</div>
            <div className="text-xs text-muted-foreground">{summaryStatus}</div>
          </div>
          {summary ? (
            <div className="space-y-1 text-sm">
              <div>Account: {summary.account_id}</div>
              <div>Status: {summary.account_status}</div>
              <div>Cleared: {formatMinor(summary.cleared_cash_minor)}</div>
              <div>Pending trades: {formatMinor(summary.delta_pending_trades_minor)}</div>
              <div>Locked orders: {formatMinor(summary.locked_open_orders_minor)}</div>
              <div className="font-medium">
                Available: {formatMinor(summary.available_minor)}
              </div>
              {summary.fee_schedule ? (
                <div className="text-xs text-muted-foreground">
                  Withdrawal fee policy: {summary.fee_schedule.withdrawal_policy}
                </div>
              ) : null}
            </div>
          ) : (
            <div className="text-sm text-muted-foreground">
              Configure a user API key to see account state.
            </div>
          )}
        </Card>
      </div>
    </div>
  );
}

function MarketsPage(props: BarebonePageProps) {
  const { apiBase, auth, session } = useAuthContext();
  const [markets, setMarkets] = React.useState<api.MarketRow[]>([]);
  const [status, setStatus] = React.useState<string>("loading...");
  const [question, setQuestion] = React.useState<string>("");
  const [outcomesText, setOutcomesText] = React.useState<string>("YES\nNO");
  const [createStatus, setCreateStatus] = React.useState<string>("idle");
  const [cardBackgroundFile, setCardBackgroundFile] = React.useState<File | null>(null);
  const [heroBackgroundFile, setHeroBackgroundFile] = React.useState<File | null>(null);
  const [thumbnailFile, setThumbnailFile] = React.useState<File | null>(null);

  const refresh = React.useCallback(async () => {
    setStatus("loading...");
    try {
      const nextMarkets = await api.listMarkets(apiBase);
      setMarkets(nextMarkets);
      setStatus("ok");
    } catch (err: unknown) {
      setStatus(describeError(err));
    }
  }, [apiBase]);

  React.useEffect(() => {
    void refresh();
  }, [refresh]);

  const createMarket = React.useCallback(
    async (event: React.FormEvent) => {
      event.preventDefault();

      if (!auth.adminApiKey) {
        setCreateStatus("missing admin API key");
        return;
      }
      const outcomes = outcomesText
        .split("\n")
        .map((line) => line.trim())
        .filter((line) => line.length > 0);

      if (!question.trim()) {
        setCreateStatus("question is required");
        return;
      }
      if (outcomes.length < 2) {
        setCreateStatus("at least two outcomes are required");
        return;
      }

      setCreateStatus("creating...");
      try {
        const created = await api.createMarket(apiBase, auth, { question: question.trim(), outcomes });
        const metadataPatch: api.UpsertMarketMetadataRequest = {};

        if (cardBackgroundFile) {
          setCreateStatus("uploading card background...");
          const upload = await api.uploadAdminMarketAsset(apiBase, auth, {
            market_id: created.market_id,
            slot: "card_background",
            file: cardBackgroundFile,
          });
          metadataPatch.card_background_image_url = upload.asset_url;
        }

        if (heroBackgroundFile) {
          setCreateStatus("uploading hero background...");
          const upload = await api.uploadAdminMarketAsset(apiBase, auth, {
            market_id: created.market_id,
            slot: "hero_background",
            file: heroBackgroundFile,
          });
          metadataPatch.hero_background_image_url = upload.asset_url;
        }

        if (thumbnailFile) {
          setCreateStatus("uploading thumbnail...");
          const upload = await api.uploadAdminMarketAsset(apiBase, auth, {
            market_id: created.market_id,
            slot: "thumbnail",
            file: thumbnailFile,
          });
          metadataPatch.thumbnail_image_url = upload.asset_url;
        }

        if (
          metadataPatch.card_background_image_url ||
          metadataPatch.hero_background_image_url ||
          metadataPatch.thumbnail_image_url
        ) {
          setCreateStatus("saving metadata...");
          await api.upsertAdminMarketMetadata(apiBase, auth, created.market_id, metadataPatch);
        }

        setQuestion("");
        setCardBackgroundFile(null);
        setHeroBackgroundFile(null);
        setThumbnailFile(null);
        setCreateStatus("created");
        await refresh();
      } catch (err: unknown) {
        setCreateStatus(describeError(err));
      }
    },
    [
      apiBase,
      auth,
      cardBackgroundFile,
      heroBackgroundFile,
      outcomesText,
      question,
      refresh,
      thumbnailFile,
    ],
  );

  return (
    <div className="space-y-4">
      <Card className="gap-0 p-4">
        <div className="mb-2 flex items-center justify-between gap-2">
          <div className="text-sm font-medium">Create market (admin)</div>
          <div className="text-xs text-muted-foreground">
            admin session: {session?.is_admin ? "yes" : "no"}
          </div>
        </div>
        <form className="space-y-3" onSubmit={createMarket}>
          <label className="block space-y-1">
            <div className="text-xs text-muted-foreground">Question</div>
            <Input
              className="w-full"
              value={question}
              onChange={(event) => setQuestion(event.target.value)}
              placeholder="Will BTC close above $100k on 2026-12-31?"
            />
          </label>
          <label className="block space-y-1">
            <div className="text-xs text-muted-foreground">
              Outcomes (one per line)
            </div>
            <Textarea
              className="h-24 w-full"
              value={outcomesText}
              onChange={(event) => setOutcomesText(event.target.value)}
            />
          </label>
          <div className="grid gap-3 md:grid-cols-3">
            <MarketImageCropper
              label="Card background"
              hint="2:1 cover image for market cards."
              file={cardBackgroundFile}
              outputWidth={800}
              outputHeight={400}
              aspectRatio={2 / 1}
              onChange={setCardBackgroundFile}
            />
            <MarketImageCropper
              label="Hero background"
              hint="6:1 banner image for trading hero."
              file={heroBackgroundFile}
              outputWidth={1260}
              outputHeight={210}
              aspectRatio={6 / 1}
              onChange={setHeroBackgroundFile}
            />
            <MarketImageCropper
              label="Thumbnail"
              hint="1:1 square image shown beside market titles."
              file={thumbnailFile}
              outputWidth={128}
              outputHeight={128}
              aspectRatio={1}
              onChange={setThumbnailFile}
            />
          </div>
          <div className="flex items-center justify-between gap-2">
            <Button
              type="submit"
            >
              Create market
            </Button>
            <div className="text-xs text-muted-foreground">{createStatus}</div>
          </div>
        </form>
      </Card>

      <Card className="gap-0 p-4">
        <div className="mb-3 flex items-center justify-between gap-2">
          <div className="text-sm font-medium">Markets</div>
          <div className="flex items-center gap-2">
            <Button
              type="button"
              variant="outline" size="xs"
              onClick={() => void refresh()}
            >
              Refresh
            </Button>
            <div className="text-xs text-muted-foreground">{status}</div>
          </div>
        </div>
        {markets.length === 0 ? (
          <div className="text-sm text-muted-foreground">No markets yet.</div>
        ) : (
          <ul className="space-y-3">
            {markets.map((market) => (
              <li key={market.contract_id} className="rounded-none border p-3">
                <div className="text-sm font-medium">{market.question}</div>
                <div className="mt-1 text-xs text-muted-foreground">
                  {formatMarketStatusLabel(market.status)}
                  {market.resolved_outcome
                    ? ` (${market.resolved_outcome})`
                    : ""}
                </div>
                <div className="mt-2 flex flex-wrap gap-1">
                  {market.outcomes.map((outcome) => (
                    <Badge
                      key={outcome}
                      variant="outline"
                    >
                      {outcome}
                    </Badge>
                  ))}
                </div>
                <div className="mt-3">
                  <Button
                    variant="link"
                    size="sm"
                    nativeButton={false}
                    render={
                      <Link
                        to={props.routes.marketDetail}
                        params={{ marketId: market.market_id }}
                      />
                    }
                  >
                    Open order book and trading panel
                  </Button>
                </div>
              </li>
            ))}
          </ul>
        )}
      </Card>
    </div>
  );
}

type MarketDetailPageProps = {
  marketId: string;
};

function MarketDetailPage(props: MarketDetailPageProps) {
  const { marketId } = props;
  const { apiBase, auth } = useAuthContext();
  const [market, setMarket] = React.useState<api.MarketRow | null>(null);
  const [book, setBook] = React.useState<api.MarketOrderBookView | null>(null);
  const [recentFills, setRecentFills] = React.useState<api.MarketFillView[]>([]);
  const [myOrders, setMyOrders] = React.useState<api.OrderRowView[]>([]);
  const [myFills, setMyFills] = React.useState<api.FillRowView[]>([]);
  const [status, setStatus] = React.useState<string>("loading...");
  const [tradingStatus, setTradingStatus] = React.useState<string>("idle");
  const [adminStatus, setAdminStatus] = React.useState<string>("idle");

  const [orderOutcome, setOrderOutcome] = React.useState<string>("");
  const [orderSide, setOrderSide] = React.useState<api.OrderSide>("Buy");
  const [nextNonce, setNextNonce] = React.useState<number>(0);
  const [orderPriceTicks, setOrderPriceTicks] = React.useState<string>("10");
  const [orderQuantityMinor, setOrderQuantityMinor] = React.useState<string>("1");
  const [resolveOutcome, setResolveOutcome] = React.useState<string>("");

  const refreshPublic = React.useCallback(async () => {
    setStatus("loading...");
    try {
      const [nextMarket, nextBook, nextFills] = await Promise.all([
        api.getMarket(apiBase, marketId),
        api.getMarketOrderBook(apiBase, marketId),
        api.listMarketFills(apiBase, marketId, 100),
      ]);
      setMarket(nextMarket);
      setBook(nextBook);
      setRecentFills(nextFills);
      setStatus("ok");

      if (!orderOutcome && nextMarket.outcomes.length > 0) {
        setOrderOutcome(nextMarket.outcomes[0] ?? "");
      }
      if (!resolveOutcome && nextMarket.outcomes.length > 0) {
        setResolveOutcome(nextMarket.outcomes[0] ?? "");
      }
    } catch (err: unknown) {
      setStatus(describeError(err));
    }
  }, [apiBase, marketId, orderOutcome, resolveOutcome]);

  const refreshTrading = React.useCallback(async () => {
    if (!auth.userApiKey) {
      setMyOrders([]);
      setMyFills([]);
      setNextNonce(0);
      setTradingStatus("configure user API key");
      return;
    }
    setTradingStatus("loading...");
    try {
      const [orders, fills, allOrders] = await Promise.all([
        api.listMyOrders(apiBase, auth, marketId),
        api.listMyFills(apiBase, auth, marketId),
        api.listMyOrders(apiBase, auth),
      ]);
      setMyOrders(orders);
      setMyFills(fills);
      setNextNonce(api.nextOrderNonce(allOrders));
      setTradingStatus("ok");
    } catch (err: unknown) {
      setNextNonce(0);
      setTradingStatus(describeError(err));
    }
  }, [apiBase, auth, marketId]);

  const refreshAll = React.useCallback(async () => {
    await Promise.all([refreshPublic(), refreshTrading()]);
  }, [refreshPublic, refreshTrading]);

  React.useEffect(() => {
    void refreshAll();
  }, [refreshAll]);

  React.useEffect(() => {
    const timer = window.setInterval(() => {
      void refreshAll();
    }, 1_000);
    return () => window.clearInterval(timer);
  }, [refreshAll]);

  const submitOrder = React.useCallback(
    async (event: React.FormEvent) => {
      event.preventDefault();
      if (!auth.userApiKey) {
        setTradingStatus("missing user API key");
        return;
      }
      const priceTicks = Number(orderPriceTicks);
      const quantityMinor = Number(orderQuantityMinor);
      if (!Number.isInteger(priceTicks) || priceTicks <= 0) {
        setTradingStatus("price must be an integer cents value > 0");
        return;
      }
      if (!Number.isInteger(quantityMinor) || quantityMinor <= 0) {
        setTradingStatus("quantity must be a whole number > 0");
        return;
      }
      if (!orderOutcome) {
        setTradingStatus("choose an outcome");
        return;
      }

      setTradingStatus("submitting...");
      try {
        const placeWithNonce = (nonce: number) =>
          api.placeOrder(apiBase, auth, {
            market_id: marketId,
            outcome: orderOutcome,
            side: orderSide,
            nonce,
            price_ticks: priceTicks,
            quantity_minor: quantityMinor,
          });

        let nonce = api.nextOrderNonce(await api.listMyOrders(apiBase, auth));
        try {
          await placeWithNonce(nonce);
        } catch (err: unknown) {
          const expectedNonce = api.expectedNonceFromError(err);
          if (expectedNonce === null) {
            throw err;
          }
          nonce = expectedNonce;
          await placeWithNonce(expectedNonce);
        }

        setTradingStatus(`order placed (nonce=${nonce})`);
        setNextNonce(nonce + 1);
        await refreshAll();
      } catch (err: unknown) {
        setTradingStatus(describeError(err));
      }
    },
    [
      apiBase,
      auth,
      marketId,
      orderPriceTicks,
      orderQuantityMinor,
      orderOutcome,
      orderSide,
      refreshAll,
    ],
  );

  return (
    <div className="space-y-4">
      <Card className="gap-0 p-4">
        <div className="mb-2 flex items-center justify-between gap-2">
          <div className="text-sm font-medium">Market detail</div>
          <div className="flex items-center gap-2">
            <Button
              type="button"
              variant="outline" size="xs"
              onClick={() => void refreshAll()}
            >
              Refresh
            </Button>
            <div className="text-xs text-muted-foreground">{status}</div>
          </div>
        </div>
        {market ? (
          <div className="space-y-1 text-sm">
            <div className="font-medium">{market.question}</div>
            <div>
              Status: <span className="font-medium">{formatMarketStatusLabel(market.status)}</span>
            </div>
            <div>Market ID: {market.market_id}</div>
            {market.resolved_outcome ? (
              <div>Resolved outcome: {market.resolved_outcome}</div>
            ) : null}
            <div className="pt-2">
              Outcomes:{" "}
              {market.outcomes.map((outcome) => (
                <Badge
                  key={outcome}
                  className="mr-2"
                  variant="outline"
                >
                  {outcome}
                </Badge>
              ))}
            </div>
          </div>
        ) : (
          <div className="text-sm text-muted-foreground">Market not loaded.</div>
        )}
      </Card>

      <div className="grid gap-4 lg:grid-cols-2">
        <Card className="gap-0 p-4">
          <div className="mb-2 text-sm font-medium">Order book</div>
          {book ? (
            <div className="grid gap-3 md:grid-cols-2">
              <div>
                <div className="mb-1 text-xs font-medium text-muted-foreground">
                  Bids
                </div>
                {book.bids.length === 0 ? (
                  <div className="text-xs text-muted-foreground">No bids.</div>
                ) : (
                  <ul className="space-y-1 text-xs">
                    {book.bids.map((level) => (
                      <li
                        key={`bid-${level.outcome}-${level.price_ticks}`}
                        className="rounded-none border p-2"
                      >
                        <div>{level.outcome}</div>
                        <div>
                          px={formatTicks(level.price_ticks)} qty=
                          {formatMinor(level.quantity_minor)} ({level.order_count} orders)
                        </div>
                      </li>
                    ))}
                  </ul>
                )}
              </div>
              <div>
                <div className="mb-1 text-xs font-medium text-muted-foreground">
                  Asks
                </div>
                {book.asks.length === 0 ? (
                  <div className="text-xs text-muted-foreground">No asks.</div>
                ) : (
                  <ul className="space-y-1 text-xs">
                    {book.asks.map((level) => (
                      <li
                        key={`ask-${level.outcome}-${level.price_ticks}`}
                        className="rounded-none border p-2"
                      >
                        <div>{level.outcome}</div>
                        <div>
                          px={formatTicks(level.price_ticks)} qty=
                          {formatMinor(level.quantity_minor)} ({level.order_count} orders)
                        </div>
                      </li>
                    ))}
                  </ul>
                )}
              </div>
            </div>
          ) : (
            <div className="text-sm text-muted-foreground">
              Order book is unavailable.
            </div>
          )}
        </Card>

        <Card className="gap-0 p-4">
          <div className="mb-2 text-sm font-medium">Recent fills</div>
          {recentFills.length === 0 ? (
            <div className="text-sm text-muted-foreground">No fills yet.</div>
          ) : (
            <ul className="space-y-2">
              {recentFills.slice(0, 12).map((fill) => (
                <li key={fill.fill_id} className="rounded-none border p-2 text-xs">
                  <div>
                    {fill.outcome} px={formatTicks(fill.price_ticks)} qty=
                    {formatMinor(fill.quantity_minor)}
                  </div>
                  <div className="text-muted-foreground">
                    {formatTime(fill.matched_at)} | epoch:{" "}
                    {fill.clearing_epoch ?? "-"}
                  </div>
                </li>
              ))}
            </ul>
          )}
        </Card>
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <Card className="gap-0 p-4">
          <div className="mb-2 flex items-center justify-between gap-2">
            <div className="text-sm font-medium">Place order</div>
            <div className="text-xs text-muted-foreground">{tradingStatus}</div>
          </div>
          <div className="mb-2 text-xs text-muted-foreground">Nonce is automatic (next: {nextNonce})</div>
          <form className="space-y-2" onSubmit={submitOrder}>
            <label className="block space-y-1">
              <div className="text-xs text-muted-foreground">Outcome</div>
              <NativeSelect
                className="w-full"
                value={orderOutcome}
                onChange={(event) => setOrderOutcome(event.target.value)}
              >
                {(market?.outcomes ?? []).map((outcome) => (
                  <NativeSelectOption key={outcome} value={outcome}>
                    {outcome}
                  </NativeSelectOption>
                ))}
              </NativeSelect>
            </label>
            <div className="grid grid-cols-1 gap-2">
              <label className="space-y-1">
                <div className="text-xs text-muted-foreground">Side</div>
                <NativeSelect
                  className="w-full"
                  value={orderSide}
                  onChange={(event) =>
                    setOrderSide(event.target.value as api.OrderSide)
                  }
                >
                  <NativeSelectOption value="Buy">Buy</NativeSelectOption>
                  <NativeSelectOption value="Sell">Sell</NativeSelectOption>
                </NativeSelect>
              </label>
            </div>
            <div className="grid grid-cols-2 gap-2">
              <label className="space-y-1">
                <div className="text-xs text-muted-foreground">Price (cents)</div>
                <Input
                  className="w-full"
                  value={orderPriceTicks}
                  onChange={(event) => setOrderPriceTicks(event.target.value)}
                />
              </label>
              <label className="space-y-1">
                <div className="text-xs text-muted-foreground">Quantity (shares)</div>
                <Input
                  className="w-full"
                  value={orderQuantityMinor}
                  onChange={(event) => setOrderQuantityMinor(event.target.value)}
                />
              </label>
            </div>
            <Button
              type="submit"
            >
              Submit order
            </Button>
          </form>
        </Card>

        <Card className="gap-0 p-4">
          <div className="mb-2 text-sm font-medium">My orders</div>
          {myOrders.length === 0 ? (
            <div className="text-sm text-muted-foreground">No orders yet.</div>
          ) : (
            <ul className="space-y-2">
              {myOrders.slice(0, 12).map((order) => (
                <li key={order.order_id} className="rounded-none border p-2 text-xs">
                  <div>
                    {order.side} {order.outcome} px={formatTicks(order.price_ticks)} qty=
                    {formatMinor(order.quantity_minor)} rem=
                    {formatMinor(order.remaining_minor)}
                  </div>
                  <div className="text-muted-foreground">
                    nonce={order.nonce} status={order.status}
                  </div>
                  {(order.status === "Open" ||
                    order.status === "PartiallyFilled") &&
                  auth.userApiKey ? (
                    <Button
                      type="button"
                      className="mt-1" variant="outline" size="xs"
                      onClick={async () => {
                        setTradingStatus("cancelling...");
                        try {
                          await api.cancelOrder(apiBase, auth, order.order_id);
                          setTradingStatus("order cancelled");
                          await refreshAll();
                        } catch (err: unknown) {
                          setTradingStatus(describeError(err));
                        }
                      }}
                    >
                      Cancel
                    </Button>
                  ) : null}
                </li>
              ))}
            </ul>
          )}
        </Card>
      </div>

      <Card className="gap-0 p-4">
        <div className="mb-2 text-sm font-medium">My fills</div>
        {myFills.length === 0 ? (
          <div className="text-sm text-muted-foreground">No personal fills yet.</div>
        ) : (
          <ul className="space-y-2">
            {myFills.slice(0, 12).map((fill) => (
              <li key={fill.fill_id} className="rounded-none border p-2 text-xs">
                <div>
                  {fill.perspective_role} {fill.outcome} px=
                  {formatTicks(fill.price_ticks)} qty={formatMinor(fill.quantity_minor)}
                </div>
                <div className="text-muted-foreground">
                  {formatTime(fill.matched_at)} | epoch={fill.clearing_epoch ?? "-"}
                </div>
              </li>
            ))}
          </ul>
        )}
      </Card>

      <Card className="gap-0 p-4">
        <div className="mb-2 flex items-center justify-between gap-2">
          <div className="text-sm font-medium">Admin controls</div>
          <div className="text-xs text-muted-foreground">{adminStatus}</div>
        </div>
        <div className="flex flex-wrap items-end gap-2">
          <Button
            type="button"
            variant="outline"
            disabled={!auth.adminApiKey || !market || market.status !== "Open"}
            onClick={async () => {
              if (!auth.adminApiKey || !market) {
                setAdminStatus("missing admin key");
                return;
              }
              setAdminStatus("closing...");
              try {
                await api.closeMarket(apiBase, auth, market.market_id);
                setAdminStatus("closed");
                await refreshPublic();
              } catch (err: unknown) {
                setAdminStatus(describeError(err));
              }
            }}
          >
            Close market
          </Button>
          <label className="space-y-1">
            <div className="text-xs text-muted-foreground">Resolve outcome</div>
            <NativeSelect
              className="w-56"
              value={resolveOutcome}
              onChange={(event) => setResolveOutcome(event.target.value)}
            >
              {(market?.outcomes ?? []).map((outcome) => (
                <NativeSelectOption key={outcome} value={outcome}>
                  {outcome}
                </NativeSelectOption>
              ))}
            </NativeSelect>
          </label>
          <Button
            type="button"
            variant="outline"
            disabled={!auth.adminApiKey || !market || market.status === "Resolved"}
            onClick={async () => {
              if (!auth.adminApiKey || !market) {
                setAdminStatus("missing admin key");
                return;
              }
              if (!resolveOutcome) {
                setAdminStatus("choose outcome");
                return;
              }
              setAdminStatus("resolving...");
              try {
                await api.resolveMarket(apiBase, auth, market.market_id, resolveOutcome);
                setAdminStatus("resolved");
                await refreshPublic();
              } catch (err: unknown) {
                setAdminStatus(describeError(err));
              }
            }}
          >
            Resolve market
          </Button>
        </div>
      </Card>
    </div>
  );
}

function FundsPage() {
  const { apiBase, auth } = useAuthContext();
  const [summary, setSummary] = React.useState<api.MySummaryView | null>(null);
  const [instructions, setInstructions] =
    React.useState<api.DepositInstructionsView | null>(null);
  const [depositPendings, setDepositPendings] = React.useState<
    api.DepositPendingView[]
  >([]);
  const [withdrawalPendings, setWithdrawalPendings] = React.useState<
    api.WithdrawalPendingView[]
  >([]);
  const [receipts, setReceipts] = React.useState<api.ReceiptView[]>([]);
  const [status, setStatus] = React.useState<string>("idle");
  const [withdrawalAmountMinor, setWithdrawalAmountMinor] =
    React.useState<string>("1");
  const [withdrawalRequestId, setWithdrawalRequestId] =
    React.useState<string>("");
  const [withdrawalSubmitStatus, setWithdrawalSubmitStatus] =
    React.useState<string>("idle");

  const refresh = React.useCallback(async () => {
    if (!auth.userApiKey) {
      setSummary(null);
      setInstructions(null);
      setDepositPendings([]);
      setWithdrawalPendings([]);
      setReceipts([]);
      setStatus("set user API key");
      return;
    }

    setStatus("loading...");
    try {
      const [nextSummary, nextInstructions, nextDepositPendings, nextWithdrawalPendings, nextReceipts] =
        await Promise.all([
          api.getMySummary(apiBase, auth),
          api.getMyDepositInstructions(apiBase, auth),
          api.listMyDepositPendings(apiBase, auth),
          api.listMyWithdrawalPendings(apiBase, auth),
          api.listMyReceipts(apiBase, auth),
        ]);
      setSummary(nextSummary);
      setInstructions(nextInstructions);
      setDepositPendings(nextDepositPendings);
      setWithdrawalPendings(nextWithdrawalPendings);
      setReceipts(nextReceipts);
      setStatus("ok");
    } catch (err: unknown) {
      setStatus(describeError(err));
    }
  }, [apiBase, auth]);

  React.useEffect(() => {
    void refresh();
  }, [refresh]);

  React.useEffect(() => {
    if (!auth.userApiKey) {
      return;
    }
    const timer = window.setInterval(() => {
      void refresh();
    }, 1_000);
    return () => window.clearInterval(timer);
  }, [auth.userApiKey, refresh]);

  return (
    <div className="space-y-4">
      <Card className="gap-0 p-4">
        <div className="mb-2 flex items-center justify-between gap-2">
          <div className="text-sm font-medium">Funds overview</div>
          <div className="flex items-center gap-2">
            <Button
              type="button"
              variant="outline" size="xs"
              onClick={() => void refresh()}
            >
              Refresh
            </Button>
            <div className="text-xs text-muted-foreground">{status}</div>
          </div>
        </div>
        {summary ? (
          <div className="grid gap-2 md:grid-cols-2 lg:grid-cols-3">
            <div className="rounded-none border p-3 text-sm">
              <div className="text-xs text-muted-foreground">Account</div>
              <div className="font-medium">{summary.account_id}</div>
            </div>
            <div className="rounded-none border p-3 text-sm">
              <div className="text-xs text-muted-foreground">Cleared cash</div>
              <div className="font-medium">{formatMinor(summary.cleared_cash_minor)}</div>
            </div>
            <div className="rounded-none border p-3 text-sm">
              <div className="text-xs text-muted-foreground">Available</div>
              <div className="font-medium">{formatMinor(summary.available_minor)}</div>
            </div>
          </div>
        ) : (
          <div className="text-sm text-muted-foreground">
            Configure a user API key to load funds data.
          </div>
        )}
        <div className="mt-4 rounded-none border p-3">
          <div className="mb-2 text-sm font-medium">Request withdrawal</div>
          <form
            className="grid gap-2 md:grid-cols-[1fr_1fr_auto]"
            onSubmit={async (event) => {
              event.preventDefault();
              if (!auth.userApiKey) {
                setWithdrawalSubmitStatus("missing user API key");
                return;
              }
              const amountMinor = Number(withdrawalAmountMinor);
              if (!Number.isInteger(amountMinor) || amountMinor <= 0) {
                setWithdrawalSubmitStatus("amount must be whole-number cents > 0");
                return;
              }
              setWithdrawalSubmitStatus("submitting...");
              try {
                const body: api.CreateWithdrawalRequestBody = {
                  amount_minor: amountMinor,
                };
                const candidateId = withdrawalRequestId.trim();
                if (candidateId) {
                  body.withdrawal_id = candidateId;
                }
                const response = await api.createWithdrawalRequest(
                  apiBase,
                  auth,
                  body,
                );
                setWithdrawalSubmitStatus(
                  `submitted: ${response.withdrawal_id}`,
                );
                setWithdrawalRequestId("");
                await refresh();
              } catch (err: unknown) {
                setWithdrawalSubmitStatus(describeError(err));
              }
            }}
          >
            <label className="space-y-1">
              <div className="text-xs text-muted-foreground">Amount (cents)</div>
              <Input
                className="w-full"
                value={withdrawalAmountMinor}
                onChange={(event) => setWithdrawalAmountMinor(event.target.value)}
              />
            </label>
            <label className="space-y-1">
              <div className="text-xs text-muted-foreground">
                Withdrawal ID (optional)
              </div>
              <Input
                className="w-full"
                value={withdrawalRequestId}
                onChange={(event) => setWithdrawalRequestId(event.target.value)}
                placeholder="auto-generated when empty"
              />
            </label>
            <Button
              type="submit"
              className="self-end"
            >
              Submit
            </Button>
          </form>
          <div className="mt-2 text-xs text-muted-foreground">
            {withdrawalSubmitStatus}
          </div>
        </div>
      </Card>

      <Card className="gap-0 p-4">
        <div className="mb-2 text-sm font-medium">Deposit instructions</div>
        {instructions ? (
          <div className="space-y-2 text-sm">
            <div>Recipient party: {instructions.recipient_party}</div>
            <div>Reason hint: {instructions.reason_hint}</div>
            <div>Instrument: {instructions.instrument_id}</div>
            {instructions.fee_schedule ? (
              <div className="text-xs text-muted-foreground">
                Fee policy: {instructions.fee_schedule.withdrawal_policy}
              </div>
            ) : null}
            <div>
              Required metadata:
              <ul className="mt-1 space-y-1">
                {instructions.required_metadata.map((field) => (
                  <li key={field.key} className="rounded-none border p-2 text-xs">
                    <div>
                      <span className="font-medium">{field.key}</span> (
                      {field.required ? "required" : "optional"})
                    </div>
                    <div className="text-muted-foreground">
                      value hint: {field.value_hint}
                    </div>
                  </li>
                ))}
              </ul>
            </div>
          </div>
        ) : (
          <div className="text-sm text-muted-foreground">Not available.</div>
        )}
      </Card>

      <div className="grid gap-4 lg:grid-cols-2">
        <Card className="gap-0 p-4">
          <div className="mb-2 text-sm font-medium">Pending deposits</div>
          {depositPendings.length === 0 ? (
            <div className="text-sm text-muted-foreground">
              No pending deposits.
            </div>
          ) : (
            <ul className="space-y-2">
              {depositPendings.map((pending) => (
                <li key={pending.contract_id} className="rounded-none border p-2 text-xs">
                  <div>
                    deposit={pending.deposit_id} amount=
                    {formatMinor(pending.expected_amount_minor)}
                  </div>
                  <div className="text-muted-foreground">
                    output={pending.latest_output ?? "-"} statusClass=
                    {pending.latest_status_class ?? "-"}
                  </div>
                  <div className="text-muted-foreground">
                    {formatTime(pending.created_at)}
                  </div>
                </li>
              ))}
            </ul>
          )}
        </Card>

        <Card className="gap-0 p-4">
          <div className="mb-2 text-sm font-medium">Pending withdrawals</div>
          {withdrawalPendings.length === 0 ? (
            <div className="text-sm text-muted-foreground">
              No pending withdrawals.
            </div>
          ) : (
            <ul className="space-y-2">
              {withdrawalPendings.map((pending) => (
                <li key={pending.contract_id} className="rounded-none border p-2 text-xs">
                  <div>
                    withdrawal={pending.withdrawal_id} amount=
                    {formatMinor(pending.amount_minor)}
                  </div>
                  <div className="text-muted-foreground">
                    state={pending.pending_state} output=
                    {pending.latest_output ?? "-"} statusClass=
                    {pending.latest_status_class ?? "-"}
                  </div>
                  <div className="text-muted-foreground">
                    {formatTime(pending.created_at)}
                  </div>
                </li>
              ))}
            </ul>
          )}
        </Card>
      </div>

      <Card className="gap-0 p-4">
        <div className="mb-2 text-sm font-medium">Receipt history</div>
        {receipts.length === 0 ? (
          <div className="text-sm text-muted-foreground">No receipts.</div>
        ) : (
          <ul className="space-y-2">
            {receipts.map((receipt) => (
              <li key={receipt.receipt_id} className="rounded-none border p-2 text-xs">
                <div>
                  {receipt.kind} {receipt.reference_id} {receipt.status} amount=
                  {formatMinor(receipt.amount_minor)}
                </div>
                <div className="text-muted-foreground">
                  {formatTime(receipt.created_at)}
                </div>
              </li>
            ))}
          </ul>
        )}
      </Card>
    </div>
  );
}

function OpsPage() {
  const { apiBase, auth } = useAuthContext();
  const [quarantine, setQuarantine] = React.useState<api.AdminQuarantineView[]>([]);
  const [escalated, setEscalated] = React.useState<
    api.AdminEscalatedWithdrawalView[]
  >([]);
  const [inventory, setInventory] = React.useState<api.AdminInventoryView[]>([]);
  const [drift, setDrift] = React.useState<api.AdminDriftView[]>([]);
  const [ops, setOps] = React.useState<api.AdminTreasuryOpView[]>([]);
  const [status, setStatus] = React.useState<string>("idle");
  const [quarantineActionStatus, setQuarantineActionStatus] =
    React.useState<string>("idle");
  const [closeoutContractId, setCloseoutContractId] = React.useState<string>("");
  const [escalationActionStatus, setEscalationActionStatus] =
    React.useState<string>("idle");
  const [reconcilingContractId, setReconcilingContractId] =
    React.useState<string>("");

  const refresh = React.useCallback(async () => {
    if (!auth.adminApiKey) {
      setQuarantine([]);
      setEscalated([]);
      setInventory([]);
      setDrift([]);
      setOps([]);
      setStatus("set admin API key");
      return;
    }

    setStatus("loading...");
    try {
      const [nextQuarantine, nextEscalated, nextInventory, nextDrift, nextOps] =
        await Promise.all([
          api.listAdminQuarantine(apiBase, auth),
          api.listAdminEscalatedWithdrawals(apiBase, auth),
          api.getAdminTreasuryInventory(apiBase, auth),
          api.getAdminDrift(apiBase, auth),
          api.listAdminTreasuryOps(apiBase, auth),
        ]);
      setQuarantine(nextQuarantine);
      setEscalated(nextEscalated);
      setInventory(nextInventory);
      setDrift(nextDrift);
      setOps(nextOps);
      setStatus("ok");
    } catch (err: unknown) {
      setStatus(describeError(err));
    }
  }, [apiBase, auth]);

  React.useEffect(() => {
    void refresh();
  }, [refresh]);

  React.useEffect(() => {
    if (!auth.adminApiKey) {
      return;
    }
    const timer = window.setInterval(() => {
      void refresh();
    }, 1_000);
    return () => window.clearInterval(timer);
  }, [auth.adminApiKey, refresh]);

  return (
    <div className="space-y-4">
      <Card className="gap-0 p-4">
        <div className="mb-2 flex items-center justify-between gap-2">
          <div className="text-sm font-medium">Admin operations dashboard</div>
          <div className="flex items-center gap-2">
            <Button
              type="button"
              variant="outline" size="xs"
              onClick={() => void refresh()}
            >
              Refresh
            </Button>
            <div className="text-xs text-muted-foreground">{status}</div>
          </div>
        </div>
        <div className="text-xs text-muted-foreground">
          Available actions: quarantine administrative closeout, and escalated
          withdrawal reconciliation.
        </div>
      </Card>

      <div className="grid gap-4 lg:grid-cols-2">
        <Card className="gap-0 p-4">
          <div className="mb-2 text-sm font-medium">Quarantine queue</div>
          {quarantine.length === 0 ? (
            <div className="text-sm text-muted-foreground">No quarantined holdings.</div>
          ) : (
            <ul className="space-y-2">
              {quarantine.map((entry) => (
                <li key={entry.contract_id} className="rounded-none border p-2 text-xs">
                  <div>
                    holding={entry.holding_cid} amount=
                    {formatMinor(entry.holding_amount_minor)}
                  </div>
                  <div>reason={entry.reason}</div>
                  <div className="text-muted-foreground">
                    account={entry.related_account_id ?? "-"} deposit=
                    {entry.related_deposit_id ?? "-"}
                  </div>
                  <div className="mt-2">
                    <Button
                      type="button"
                      variant="outline" size="xs"
                      disabled={closeoutContractId === entry.contract_id}
                      onClick={async () => {
                        setCloseoutContractId(entry.contract_id);
                        setQuarantineActionStatus(`closing out ${entry.holding_cid}...`);
                        try {
                          const result = await api.closeoutAdminQuarantineHolding(
                            apiBase,
                            auth,
                            entry.contract_id,
                          );
                          setQuarantineActionStatus(
                            `${entry.holding_cid}: ${result.action}`,
                          );
                          await refresh();
                        } catch (err: unknown) {
                          setQuarantineActionStatus(describeError(err));
                        } finally {
                          setCloseoutContractId("");
                        }
                      }}
                    >
                      Administrative closeout
                    </Button>
                  </div>
                </li>
              ))}
            </ul>
          )}
          <div className="mt-2 text-xs text-muted-foreground">
            {quarantineActionStatus}
          </div>
        </Card>

        <Card className="gap-0 p-4">
          <div className="mb-2 text-sm font-medium">
            Withdrawals in CancelEscalated
          </div>
          {escalated.length === 0 ? (
            <div className="text-sm text-muted-foreground">No escalations.</div>
          ) : (
            <ul className="space-y-2">
              {escalated.map((entry) => (
                <li key={entry.contract_id} className="rounded-none border p-2 text-xs">
                  <div>
                    withdrawal={entry.withdrawal_id} account={entry.account_id} amount=
                    {formatMinor(entry.amount_minor)}
                  </div>
                  <div className="text-muted-foreground">
                    statusClass={entry.latest_status_class ?? "-"} output=
                    {entry.latest_output ?? "-"}
                  </div>
                  <div className="mt-2">
                    <Button
                      type="button"
                      variant="outline" size="xs"
                      disabled={reconcilingContractId === entry.contract_id}
                      onClick={async () => {
                        setReconcilingContractId(entry.contract_id);
                        setEscalationActionStatus(
                          `reconciling ${entry.withdrawal_id}...`,
                        );
                        try {
                          const result = await api.reconcileAdminEscalatedWithdrawal(
                            apiBase,
                            auth,
                            entry.contract_id,
                          );
                          setEscalationActionStatus(
                            `${entry.withdrawal_id}: ${result.action} (${result.latest_output})`,
                          );
                          await refresh();
                        } catch (err: unknown) {
                          setEscalationActionStatus(describeError(err));
                        } finally {
                          setReconcilingContractId("");
                        }
                      }}
                    >
                      Reconcile now
                    </Button>
                  </div>
                </li>
              ))}
            </ul>
          )}
          <div className="mt-2 text-xs text-muted-foreground">
            {escalationActionStatus}
          </div>
        </Card>
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <Card className="gap-0 p-4">
          <div className="mb-2 text-sm font-medium">Treasury inventory + UTXO health</div>
          {inventory.length === 0 ? (
            <div className="text-sm text-muted-foreground">No inventory rows.</div>
          ) : (
            <ul className="space-y-2">
              {inventory.map((entry) => (
                <li
                  key={`${entry.instrument_admin}:${entry.instrument_id}`}
                  className="rounded-none border p-2 text-xs"
                >
                  <div>
                    {entry.instrument_id} total={formatMinor(entry.total_holdings)} unlocked=
                    {formatMinor(entry.unlocked_holdings)} reserved=
                    {formatMinor(entry.reserved_holdings)}
                  </div>
                  <div className="text-muted-foreground">
                    lockExpired={formatMinor(entry.lock_expired_holdings)} dust=
                    {formatMinor(entry.dust_holdings)} ({formatRatio(entry.dust_ratio)})
                  </div>
                  <div className="text-muted-foreground">
                    targetRange={formatMinor(entry.target_utxo_count_min)}-
                    {formatMinor(entry.target_utxo_count_max)} dustThreshold=
                    {formatMinor(entry.dust_threshold_minor)}
                  </div>
                </li>
              ))}
            </ul>
          )}
        </Card>

        <Card className="gap-0 p-4">
          <div className="mb-2 text-sm font-medium">Drift dashboard</div>
          {drift.length === 0 ? (
            <div className="text-sm text-muted-foreground">No drift rows.</div>
          ) : (
            <ul className="space-y-2">
              {drift.map((entry) => (
                <li
                  key={`${entry.instrument_admin}:${entry.instrument_id}`}
                  className="rounded-none border p-2 text-xs"
                >
                  <div>
                    {entry.instrument_id} holdings=
                    {formatMinor(entry.total_holdings_minor)} liabilities=
                    {formatMinor(entry.cleared_liabilities_minor)}
                  </div>
                  <div className="text-muted-foreground">
                    pendingW={formatMinor(entry.pending_withdrawals_minor)} pendingD=
                    {formatMinor(entry.pending_deposits_minor)} quarantined=
                    {formatMinor(entry.quarantined_minor)}
                  </div>
                  <div className="text-muted-foreground">
                    implied={formatMinor(entry.implied_obligations_minor)} coverage=
                    {formatMinor(entry.coverage_minor)} liquidity=
                    {formatMinor(entry.available_liquidity_minor)}
                  </div>
                </li>
              ))}
            </ul>
          )}
        </Card>
      </div>

      <Card className="gap-0 p-4">
        <div className="mb-2 text-sm font-medium">Treasury operations</div>
        {ops.length === 0 ? (
          <div className="text-sm text-muted-foreground">No operations found.</div>
        ) : (
          <ul className="space-y-2">
            {ops.slice(0, 30).map((entry) => (
              <li key={entry.op_id} className="rounded-none border p-2 text-xs">
                <div>
                  {entry.op_type} {entry.op_id} state={entry.state} step=
                  {entry.step_seq}
                </div>
                <div className="text-muted-foreground">
                  account={entry.account_id ?? "-"} amount=
                  {formatMinor(entry.amount_minor)}
                </div>
                <div className="text-muted-foreground">
                  updated={formatTime(entry.updated_at)}
                  {entry.last_error ? ` | error=${entry.last_error}` : ""}
                </div>
              </li>
            ))}
          </ul>
        )}
      </Card>
    </div>
  );
}

const bareboneRoutes = buildBareboneRoutes("/barebone");
const controlPanelRoutes = buildBareboneRoutes("/control-panel");

type ControlPanelLayoutProps = {
  children: React.ReactNode;
};

export function ControlPanelLayout(props: ControlPanelLayoutProps) {
  return (
    <BareboneRootLayout
      routePrefix="/control-panel"
      title="pebble control panel"
      subtitle="Admin tools for markets, funds, and operations"
    >
      {props.children}
    </BareboneRootLayout>
  );
}

export function BareboneHomeRoutePage() {
  return (
    <BareboneRootLayout
      routePrefix="/barebone"
      title="pebble barebone"
      subtitle="Prediction markets on Canton"
    >
      <HomePage routes={bareboneRoutes} />
    </BareboneRootLayout>
  );
}

export function BareboneMarketsRoutePage() {
  return (
    <BareboneRootLayout
      routePrefix="/barebone"
      title="pebble barebone"
      subtitle="Prediction markets on Canton"
    >
      <MarketsPage routes={bareboneRoutes} />
    </BareboneRootLayout>
  );
}

export function BareboneMarketDetailRoutePage() {
  const { marketId } = useParams({ from: "/barebone/markets/$marketId" });

  return (
    <BareboneRootLayout
      routePrefix="/barebone"
      title="pebble barebone"
      subtitle="Prediction markets on Canton"
    >
      <MarketDetailPage marketId={marketId} />
    </BareboneRootLayout>
  );
}

export function BareboneFundsRoutePage() {
  return (
    <BareboneRootLayout
      routePrefix="/barebone"
      title="pebble barebone"
      subtitle="Prediction markets on Canton"
    >
      <FundsPage />
    </BareboneRootLayout>
  );
}

export function BareboneOpsRoutePage() {
  return (
    <BareboneRootLayout
      routePrefix="/barebone"
      title="pebble barebone"
      subtitle="Prediction markets on Canton"
    >
      <OpsPage />
    </BareboneRootLayout>
  );
}

export function ControlPanelHomeRoutePage() {
  return <HomePage routes={controlPanelRoutes} />;
}

export function ControlPanelMarketsRoutePage() {
  return <MarketsPage routes={controlPanelRoutes} />;
}

export function ControlPanelMarketDetailRoutePage() {
  const { marketId } = useParams({ from: "/control-panel/markets/$marketId" });

  return <MarketDetailPage marketId={marketId} />;
}

export function ControlPanelFundsRoutePage() {
  return <FundsPage />;
}

export function ControlPanelOpsRoutePage() {
  return <OpsPage />;
}
