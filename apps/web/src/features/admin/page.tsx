import React from "react";

import { useSessionContext } from "@/app/session";
import { describeError } from "@/app/session";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Skeleton } from "@/components/ui/skeleton";
import { Switch } from "@/components/ui/switch";
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
import { Textarea } from "@/components/ui/textarea";
import { MarketImageCropper } from "@/features/admin/market-image-cropper";
import * as api from "@/lib/api";
import { withAssetVariant } from "@/lib/assets";
import { formatMarketStatusLabel } from "@/lib/market-status";
import { withRetry } from "@/lib/retry";

type AdminMarketView = api.MarketRow & {
  category: string;
  tags: string[];
  featured: boolean;
  card_background_image_url: string | null;
  hero_background_image_url: string | null;
  thumbnail_image_url: string | null;
};

function formatMinor(value: number | null | undefined): string {
  if (value === null || value === undefined) {
    return "-";
  }

  return value.toLocaleString();
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

function parseListInput(raw: string): string[] {
  const seen = new Set<string>();
  const values: string[] = [];

  for (const part of raw.split(",")) {
    const value = part.trim();
    if (!value) {
      continue;
    }
    if (!seen.has(value)) {
      seen.add(value);
      values.push(value);
    }
  }

  return values;
}

export function AdminPage() {
  const { apiBase, auth } = useSessionContext();

  const [markets, setMarkets] = React.useState<AdminMarketView[]>([]);
  const [inventory, setInventory] = React.useState<api.AdminInventoryView[]>([]);
  const [drift, setDrift] = React.useState<api.AdminDriftView[]>([]);
  const [ops, setOps] = React.useState<api.AdminTreasuryOpView[]>([]);
  const [quarantine, setQuarantine] = React.useState<api.AdminQuarantineView[]>([]);
  const [escalatedWithdrawals, setEscalatedWithdrawals] = React.useState<api.AdminEscalatedWithdrawalView[]>([]);

  const [status, setStatus] = React.useState<string>("loading...");
  const [actionStatus, setActionStatus] = React.useState<string>("idle");

  const [selectedMarketId, setSelectedMarketId] = React.useState<string>("");
  const [resolveOutcome, setResolveOutcome] = React.useState<string>("");
  const [metadataCategory, setMetadataCategory] = React.useState<string>("General");
  const [metadataTags, setMetadataTags] = React.useState<string>("");
  const [metadataFeatured, setMetadataFeatured] = React.useState<boolean>(false);
  const [metadataCardBackgroundFile, setMetadataCardBackgroundFile] = React.useState<File | null>(
    null,
  );
  const [metadataHeroBackgroundFile, setMetadataHeroBackgroundFile] = React.useState<File | null>(
    null,
  );
  const [metadataThumbnailFile, setMetadataThumbnailFile] = React.useState<File | null>(null);

  const [createQuestion, setCreateQuestion] = React.useState<string>("");
  const [createOutcomes, setCreateOutcomes] = React.useState<string>("Yes,No");
  const [createCategory, setCreateCategory] = React.useState<string>("General");
  const [createTags, setCreateTags] = React.useState<string>("");
  const [createFeatured, setCreateFeatured] = React.useState<boolean>(false);
  const [createCardBackgroundFile, setCreateCardBackgroundFile] = React.useState<File | null>(null);
  const [createHeroBackgroundFile, setCreateHeroBackgroundFile] = React.useState<File | null>(null);
  const [createThumbnailFile, setCreateThumbnailFile] = React.useState<File | null>(null);

  const [pendingActionId, setPendingActionId] = React.useState<string | null>(null);
  const metadataDraftMarketIdRef = React.useRef<string>("");

  const refresh = React.useCallback(async () => {
    setStatus("loading...");

    try {
      const [
        rawMarkets,
        metadataRows,
        nextInventory,
        nextDrift,
        nextOps,
        nextQuarantine,
        nextEscalated,
      ] = await withRetry(
        async () =>
          Promise.all([
            api.listMarkets(apiBase),
            api.listMarketMetadata(apiBase, { limit: 500 }),
            api.getAdminTreasuryInventory(apiBase, auth),
            api.getAdminDrift(apiBase, auth),
            api.listAdminTreasuryOps(apiBase, auth),
            api.listAdminQuarantine(apiBase, auth),
            api.listAdminEscalatedWithdrawals(apiBase, auth),
          ]),
        {
          attempts: 3,
          initialDelayMs: 250,
        },
      );

      const metadataByMarketId = new Map(metadataRows.map((row) => [row.market_id, row]));
      const nextMarkets: AdminMarketView[] = rawMarkets.map((market) => {
        const metadata = metadataByMarketId.get(market.market_id);

        return {
          ...market,
          category: metadata?.category ?? "General",
          tags: metadata?.tags ?? [],
          featured: metadata?.featured ?? false,
          card_background_image_url: metadata?.card_background_image_url ?? null,
          hero_background_image_url: metadata?.hero_background_image_url ?? null,
          thumbnail_image_url: metadata?.thumbnail_image_url ?? null,
        };
      });

      setMarkets(nextMarkets);
      setInventory(nextInventory);
      setDrift(nextDrift);
      setOps(nextOps);
      setQuarantine(nextQuarantine);
      setEscalatedWithdrawals(nextEscalated);
      setStatus("ok");
    } catch (err: unknown) {
      setMarkets([]);
      setInventory([]);
      setDrift([]);
      setOps([]);
      setQuarantine([]);
      setEscalatedWithdrawals([]);
      setStatus(describeError(err));
    }
  }, [apiBase, auth]);

  React.useEffect(() => {
    void refresh();

    const id = window.setInterval(() => {
      void refresh();
    }, 1_000);

    return () => {
      window.clearInterval(id);
    };
  }, [refresh]);

  React.useEffect(() => {
    if (!selectedMarketId && markets.length > 0) {
      setSelectedMarketId(markets[0].market_id);
    }

    if (selectedMarketId && markets.every((market) => market.market_id !== selectedMarketId)) {
      setSelectedMarketId(markets[0]?.market_id ?? "");
    }
  }, [selectedMarketId, markets]);

  const selectedMarket = React.useMemo(() => {
    return markets.find((market) => market.market_id === selectedMarketId) ?? null;
  }, [markets, selectedMarketId]);

  React.useEffect(() => {
    if (!selectedMarket) {
      metadataDraftMarketIdRef.current = "";
      return;
    }

    if (metadataDraftMarketIdRef.current === selectedMarket.market_id) {
      return;
    }
    metadataDraftMarketIdRef.current = selectedMarket.market_id;

    setResolveOutcome(selectedMarket.outcomes[0] ?? "");
    setMetadataCategory(selectedMarket.category);
    setMetadataTags(selectedMarket.tags.join(", "));
    setMetadataFeatured(selectedMarket.featured);
    setMetadataCardBackgroundFile(null);
    setMetadataHeroBackgroundFile(null);
    setMetadataThumbnailFile(null);
  }, [selectedMarket]);

  const openMarkets = markets.filter((market) => market.status === "Open").length;
  const isLoading = status === "loading...";
  const isError = !isLoading && status !== "ok";

  return (
    <div className="space-y-4">
      <Card>
        <CardHeader>
          <CardTitle>Admin Operations</CardTitle>
          <CardDescription>
            Operator-grade controls for market lifecycle, treasury monitoring, and incident response.
          </CardDescription>
        </CardHeader>
        <CardContent className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
          <Card size="sm" className="py-0">
            <CardContent className="space-y-1 p-3">
              <div className="text-xs text-muted-foreground">Live markets</div>
              <div className="text-sm font-medium">{openMarkets}</div>
            </CardContent>
          </Card>
          <Card size="sm" className="py-0">
            <CardContent className="space-y-1 p-3">
              <div className="text-xs text-muted-foreground">Quarantine queue</div>
              <div className="text-sm font-medium">{quarantine.length}</div>
            </CardContent>
          </Card>
          <Card size="sm" className="py-0">
            <CardContent className="space-y-1 p-3">
              <div className="text-xs text-muted-foreground">Escalated withdrawals</div>
              <div className="text-sm font-medium">{escalatedWithdrawals.length}</div>
            </CardContent>
          </Card>
          <Card size="sm" className="py-0">
            <CardContent className="space-y-1 p-3">
              <div className="text-xs text-muted-foreground">Data status</div>
              <div className="text-sm font-medium">{status}</div>
            </CardContent>
          </Card>
        </CardContent>
      </Card>

      <Alert>
        <AlertTitle>Action status</AlertTitle>
        <AlertDescription>{actionStatus}</AlertDescription>
      </Alert>

      {isError ? (
        <Alert variant="destructive">
          <AlertTitle>Admin data failed</AlertTitle>
          <AlertDescription>{status}</AlertDescription>
        </Alert>
      ) : null}

      {isLoading && markets.length === 0 ? (
        <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
          {Array.from({ length: 4 }, (_, index) => (
            <Card key={`admin-skeleton-${index}`}>
              <CardHeader>
                <Skeleton className="h-4 w-24" />
              </CardHeader>
              <CardContent>
                <Skeleton className="h-6 w-16" />
              </CardContent>
            </Card>
          ))}
        </div>
      ) : null}

      <Tabs defaultValue="markets">
        <TabsList variant="line" className="w-full justify-start overflow-x-auto">
          <TabsTrigger value="markets">Market admin</TabsTrigger>
          <TabsTrigger value="treasury">Treasury monitoring</TabsTrigger>
          <TabsTrigger value="incidents">Incident response</TabsTrigger>
        </TabsList>

        <TabsContent value="markets" className="space-y-4">
          <div className="grid gap-4 xl:grid-cols-[1.2fr_1fr]">
            <Card>
              <CardHeader>
                <CardTitle>Create Market</CardTitle>
                <CardDescription>
                  Creates the market on-ledger and optionally assigns category/tags/featured metadata.
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-3">
                <Textarea
                  value={createQuestion}
                  onChange={(event) => setCreateQuestion(event.target.value)}
                  placeholder="Question"
                  rows={3}
                />
                <Input
                  value={createOutcomes}
                  onChange={(event) => setCreateOutcomes(event.target.value)}
                  placeholder="Outcomes (comma separated)"
                />
                <div className="grid gap-2 sm:grid-cols-2">
                  <Input
                    value={createCategory}
                    onChange={(event) => setCreateCategory(event.target.value)}
                    placeholder="Category"
                  />
                  <Input
                    value={createTags}
                    onChange={(event) => setCreateTags(event.target.value)}
                    placeholder="Tags (comma separated)"
                  />
                </div>
                <label className="flex items-center gap-2 text-xs text-muted-foreground">
                  <Switch checked={createFeatured} onCheckedChange={setCreateFeatured} />
                  Featured market
                </label>
                <div className="grid gap-3 md:grid-cols-3">
                  <MarketImageCropper
                    label="Card background"
                    hint="2:1 cover image for market cards."
                    file={createCardBackgroundFile}
                    outputWidth={800}
                    outputHeight={400}
                    aspectRatio={2 / 1}
                    onChange={setCreateCardBackgroundFile}
                    disabled={pendingActionId !== null}
                  />
                  <MarketImageCropper
                    label="Hero background"
                    hint="6:1 banner image for trading hero."
                    file={createHeroBackgroundFile}
                    outputWidth={1260}
                    outputHeight={210}
                    aspectRatio={6 / 1}
                    onChange={setCreateHeroBackgroundFile}
                    disabled={pendingActionId !== null}
                  />
                  <MarketImageCropper
                    label="Thumbnail"
                    hint="1:1 square image shown beside market titles."
                    file={createThumbnailFile}
                    outputWidth={128}
                    outputHeight={128}
                    aspectRatio={1}
                    onChange={setCreateThumbnailFile}
                    disabled={pendingActionId !== null}
                  />
                </div>

                <Button
                  onClick={async () => {
                    const question = createQuestion.trim();
                    const outcomes = parseListInput(createOutcomes);
                    if (!question) {
                      setActionStatus("question is required");
                      return;
                    }
                    if (outcomes.length < 2) {
                      setActionStatus("at least two outcomes are required");
                      return;
                    }

                    setPendingActionId("create-market");
                    setActionStatus("creating market...");
                    try {
                      const created = await api.createMarket(apiBase, auth, {
                        question,
                        outcomes,
                      });

                      const metadataPatch: api.UpsertMarketMetadataRequest = {
                        category: createCategory.trim() || "General",
                        tags: parseListInput(createTags),
                        featured: createFeatured,
                      };

                      if (createCardBackgroundFile) {
                        setActionStatus("uploading card background...");
                        const upload = await api.uploadAdminMarketAsset(apiBase, auth, {
                          market_id: created.market_id,
                          slot: "card_background",
                          file: createCardBackgroundFile,
                        });
                        metadataPatch.card_background_image_url = upload.asset_url;
                      }

                      if (createHeroBackgroundFile) {
                        setActionStatus("uploading hero background...");
                        const upload = await api.uploadAdminMarketAsset(apiBase, auth, {
                          market_id: created.market_id,
                          slot: "hero_background",
                          file: createHeroBackgroundFile,
                        });
                        metadataPatch.hero_background_image_url = upload.asset_url;
                      }

                      if (createThumbnailFile) {
                        setActionStatus("uploading thumbnail...");
                        const upload = await api.uploadAdminMarketAsset(apiBase, auth, {
                          market_id: created.market_id,
                          slot: "thumbnail",
                          file: createThumbnailFile,
                        });
                        metadataPatch.thumbnail_image_url = upload.asset_url;
                      }

                      setActionStatus("saving metadata...");
                      await api.upsertAdminMarketMetadata(
                        apiBase,
                        auth,
                        created.market_id,
                        metadataPatch,
                      );

                      setActionStatus(`created market ${created.market_id}`);
                      setCreateQuestion("");
                      setCreateOutcomes("Yes,No");
                      setCreateTags("");
                      setCreateFeatured(false);
                      setCreateCardBackgroundFile(null);
                      setCreateHeroBackgroundFile(null);
                      setCreateThumbnailFile(null);
                      await refresh();
                    } catch (err: unknown) {
                      setActionStatus(describeError(err));
                    } finally {
                      setPendingActionId(null);
                    }
                  }}
                  disabled={pendingActionId !== null}
                >
                  Create market
                </Button>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle>Lifecycle + Metadata</CardTitle>
                <CardDescription>
                  Update market metadata and execute close/resolve actions.
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-3">
                <Select
                  value={selectedMarketId}
                  onValueChange={(value) => setSelectedMarketId(value ?? "")}
                >
                  <SelectTrigger>
                    <SelectValue placeholder="Select market" />
                  </SelectTrigger>
                  <SelectContent>
                    {markets.map((market) => (
                      <SelectItem key={market.market_id} value={market.market_id}>
                        {market.market_id}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>

                <div className="rounded-none border p-2 text-xs text-muted-foreground">
                  <div>Question: {selectedMarket?.question ?? "-"}</div>
                  <div>Status: {formatMarketStatusLabel(selectedMarket?.status)}</div>
                  <div>Outcomes: {selectedMarket?.outcomes.join(" / ") ?? "-"}</div>
                </div>

                <div className="grid gap-2 sm:grid-cols-2">
                  <Input
                    value={metadataCategory}
                    onChange={(event) => setMetadataCategory(event.target.value)}
                    placeholder="Category"
                  />
                  <Input
                    value={metadataTags}
                    onChange={(event) => setMetadataTags(event.target.value)}
                    placeholder="Tags (comma separated)"
                  />
                </div>
                <label className="flex items-center gap-2 text-xs text-muted-foreground">
                  <Switch checked={metadataFeatured} onCheckedChange={setMetadataFeatured} />
                  Featured market
                </label>
                <div className="grid gap-2 md:grid-cols-3">
                  <div className="space-y-1">
                    <div className="text-xs text-muted-foreground">Current card background</div>
                    <div className="aspect-[2/1] overflow-hidden border bg-muted">
                      {selectedMarket?.card_background_image_url ? (
                        <img
                          src={withAssetVariant(selectedMarket.card_background_image_url, 800, 70) ?? ""}
                          alt=""
                          className="h-full w-full object-cover"
                          loading="lazy"
                        />
                      ) : null}
                    </div>
                  </div>
                  <div className="space-y-1">
                    <div className="text-xs text-muted-foreground">Current hero background</div>
                    <div className="aspect-[6/1] overflow-hidden border bg-muted">
                      {selectedMarket?.hero_background_image_url ? (
                        <img
                          src={withAssetVariant(selectedMarket.hero_background_image_url, 1260, 70) ?? ""}
                          alt=""
                          className="h-full w-full object-cover"
                          loading="lazy"
                        />
                      ) : null}
                    </div>
                  </div>
                  <div className="space-y-1">
                    <div className="text-xs text-muted-foreground">Current thumbnail</div>
                    <div className="aspect-square overflow-hidden border bg-muted">
                      {selectedMarket?.thumbnail_image_url ? (
                        <img
                          src={withAssetVariant(selectedMarket.thumbnail_image_url, 128, 70) ?? ""}
                          alt=""
                          className="h-full w-full object-cover"
                          loading="lazy"
                        />
                      ) : null}
                    </div>
                  </div>
                </div>
                <div className="grid gap-3 md:grid-cols-3">
                  <MarketImageCropper
                    label="Replace card background"
                    hint="2:1 cover image for market cards."
                    file={metadataCardBackgroundFile}
                    outputWidth={800}
                    outputHeight={400}
                    aspectRatio={2 / 1}
                    onChange={setMetadataCardBackgroundFile}
                    disabled={!selectedMarket || pendingActionId !== null}
                  />
                  <MarketImageCropper
                    label="Replace hero background"
                    hint="6:1 banner image for trading hero."
                    file={metadataHeroBackgroundFile}
                    outputWidth={1260}
                    outputHeight={210}
                    aspectRatio={6 / 1}
                    onChange={setMetadataHeroBackgroundFile}
                    disabled={!selectedMarket || pendingActionId !== null}
                  />
                  <MarketImageCropper
                    label="Replace thumbnail"
                    hint="1:1 square image shown beside market titles."
                    file={metadataThumbnailFile}
                    outputWidth={128}
                    outputHeight={128}
                    aspectRatio={1}
                    onChange={setMetadataThumbnailFile}
                    disabled={!selectedMarket || pendingActionId !== null}
                  />
                </div>

                <Button
                  variant="outline"
                  disabled={!selectedMarket || pendingActionId !== null}
                  onClick={async () => {
                    if (!selectedMarket) {
                      return;
                    }

                    setPendingActionId(`metadata-${selectedMarket.market_id}`);
                    setActionStatus("updating market metadata...");
                    try {
                      const metadataPatch: api.UpsertMarketMetadataRequest = {
                        category: metadataCategory.trim() || "General",
                        tags: parseListInput(metadataTags),
                        featured: metadataFeatured,
                      };

                      if (metadataCardBackgroundFile) {
                        setActionStatus("uploading card background...");
                        const upload = await api.uploadAdminMarketAsset(apiBase, auth, {
                          market_id: selectedMarket.market_id,
                          slot: "card_background",
                          file: metadataCardBackgroundFile,
                        });
                        metadataPatch.card_background_image_url = upload.asset_url;
                      }

                      if (metadataHeroBackgroundFile) {
                        setActionStatus("uploading hero background...");
                        const upload = await api.uploadAdminMarketAsset(apiBase, auth, {
                          market_id: selectedMarket.market_id,
                          slot: "hero_background",
                          file: metadataHeroBackgroundFile,
                        });
                        metadataPatch.hero_background_image_url = upload.asset_url;
                      }

                      if (metadataThumbnailFile) {
                        setActionStatus("uploading thumbnail...");
                        const upload = await api.uploadAdminMarketAsset(apiBase, auth, {
                          market_id: selectedMarket.market_id,
                          slot: "thumbnail",
                          file: metadataThumbnailFile,
                        });
                        metadataPatch.thumbnail_image_url = upload.asset_url;
                      }

                      setActionStatus("saving metadata...");
                      await api.upsertAdminMarketMetadata(
                        apiBase,
                        auth,
                        selectedMarket.market_id,
                        metadataPatch,
                      );
                      setMetadataCardBackgroundFile(null);
                      setMetadataHeroBackgroundFile(null);
                      setMetadataThumbnailFile(null);
                      setActionStatus(`updated metadata for ${selectedMarket.market_id}`);
                      await refresh();
                    } catch (err: unknown) {
                      setActionStatus(describeError(err));
                    } finally {
                      setPendingActionId(null);
                    }
                  }}
                >
                  Save metadata and images
                </Button>

                <Select
                  value={resolveOutcome}
                  onValueChange={(value) => setResolveOutcome(value ?? "")}
                >
                  <SelectTrigger>
                    <SelectValue placeholder="Resolve outcome" />
                  </SelectTrigger>
                  <SelectContent>
                    {selectedMarket?.outcomes.map((outcome) => (
                      <SelectItem key={outcome} value={outcome}>
                        {outcome}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>

                <div className="flex flex-wrap gap-2">
                  <Button
                    variant="outline"
                    disabled={!selectedMarket || selectedMarket.status !== "Open" || pendingActionId !== null}
                    onClick={async () => {
                      if (!selectedMarket) {
                        return;
                      }

                      setPendingActionId(`close-${selectedMarket.market_id}`);
                      setActionStatus(`closing ${selectedMarket.market_id}...`);
                      try {
                        await api.closeMarket(apiBase, auth, selectedMarket.market_id);
                        setActionStatus(`closed ${selectedMarket.market_id}`);
                        await refresh();
                      } catch (err: unknown) {
                        setActionStatus(describeError(err));
                      } finally {
                        setPendingActionId(null);
                      }
                    }}
                  >
                    Close market
                  </Button>

                  <Button
                    disabled={!selectedMarket || !resolveOutcome || pendingActionId !== null}
                    onClick={async () => {
                      if (!selectedMarket) {
                        return;
                      }

                      setPendingActionId(`resolve-${selectedMarket.market_id}`);
                      setActionStatus(`resolving ${selectedMarket.market_id}...`);
                      try {
                        await api.resolveMarket(apiBase, auth, selectedMarket.market_id, resolveOutcome);
                        setActionStatus(`resolved ${selectedMarket.market_id} as ${resolveOutcome}`);
                        await refresh();
                      } catch (err: unknown) {
                        setActionStatus(describeError(err));
                      } finally {
                        setPendingActionId(null);
                      }
                    }}
                  >
                    Resolve market
                  </Button>
                </div>
              </CardContent>
            </Card>
          </div>

          <Card>
            <CardHeader>
              <CardTitle>Market List</CardTitle>
            </CardHeader>
            <CardContent>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Market</TableHead>
                    <TableHead>Status</TableHead>
                    <TableHead>Category</TableHead>
                    <TableHead>Tags</TableHead>
                    <TableHead>Featured</TableHead>
                    <TableHead>Created</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {markets.map((market) => (
                    <TableRow key={market.market_id}>
                      <TableCell>
                        <div className="space-y-1">
                          <div className="font-medium">{market.question}</div>
                          <div className="text-xs text-muted-foreground">{market.market_id}</div>
                        </div>
                      </TableCell>
                      <TableCell>{formatMarketStatusLabel(market.status)}</TableCell>
                      <TableCell>{market.category}</TableCell>
                      <TableCell>{market.tags.join(", ") || "-"}</TableCell>
                      <TableCell>{market.featured ? "yes" : "no"}</TableCell>
                      <TableCell>{formatTime(market.created_at)}</TableCell>
                    </TableRow>
                  ))}
                  {markets.length === 0 ? (
                    <TableRow>
                      <TableCell colSpan={6} className="text-muted-foreground">
                        No markets available.
                      </TableCell>
                    </TableRow>
                  ) : null}
                </TableBody>
              </Table>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="treasury" className="space-y-4">
          <Card>
            <CardHeader>
              <CardTitle>Drift Dashboard</CardTitle>
              <CardDescription>Coverage and obligations by instrument.</CardDescription>
            </CardHeader>
            <CardContent>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Instrument</TableHead>
                    <TableHead>Total Holdings</TableHead>
                    <TableHead>Liabilities</TableHead>
                    <TableHead>Pending W/D</TableHead>
                    <TableHead>Pending Deposits</TableHead>
                    <TableHead>Coverage</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {drift.map((row) => (
                    <TableRow key={`${row.instrument_admin}-${row.instrument_id}`}>
                      <TableCell>{row.instrument_id}</TableCell>
                      <TableCell>{formatMinor(row.total_holdings_minor)}</TableCell>
                      <TableCell>{formatMinor(row.cleared_liabilities_minor)}</TableCell>
                      <TableCell>{formatMinor(row.pending_withdrawals_minor)}</TableCell>
                      <TableCell>{formatMinor(row.pending_deposits_minor)}</TableCell>
                      <TableCell>{formatMinor(row.coverage_minor)}</TableCell>
                    </TableRow>
                  ))}
                  {drift.length === 0 ? (
                    <TableRow>
                      <TableCell colSpan={6} className="text-muted-foreground">
                        No drift rows.
                      </TableCell>
                    </TableRow>
                  ) : null}
                </TableBody>
              </Table>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>Treasury Inventory</CardTitle>
            </CardHeader>
            <CardContent>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Instrument</TableHead>
                    <TableHead>Total Holdings</TableHead>
                    <TableHead>Unlocked</TableHead>
                    <TableHead>Locked</TableHead>
                    <TableHead>Total Amount</TableHead>
                    <TableHead>Dust Ratio</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {inventory.map((row) => (
                    <TableRow key={`${row.instrument_admin}-${row.instrument_id}`}>
                      <TableCell>{row.instrument_id}</TableCell>
                      <TableCell>{row.total_holdings}</TableCell>
                      <TableCell>{row.unlocked_holdings}</TableCell>
                      <TableCell>{row.locked_holdings}</TableCell>
                      <TableCell>{formatMinor(row.total_amount_minor)}</TableCell>
                      <TableCell>{(row.dust_ratio * 100).toFixed(1)}%</TableCell>
                    </TableRow>
                  ))}
                  {inventory.length === 0 ? (
                    <TableRow>
                      <TableCell colSpan={6} className="text-muted-foreground">
                        No inventory rows.
                      </TableCell>
                    </TableRow>
                  ) : null}
                </TableBody>
              </Table>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>Treasury Ops</CardTitle>
            </CardHeader>
            <CardContent>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Op ID</TableHead>
                    <TableHead>Type</TableHead>
                    <TableHead>State</TableHead>
                    <TableHead>Account</TableHead>
                    <TableHead>Amount</TableHead>
                    <TableHead>Updated</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {ops.slice(0, 80).map((row) => (
                    <TableRow key={row.op_id}>
                      <TableCell>{row.op_id}</TableCell>
                      <TableCell>{row.op_type}</TableCell>
                      <TableCell>{row.state}</TableCell>
                      <TableCell>{row.account_id ?? "-"}</TableCell>
                      <TableCell>{formatMinor(row.amount_minor)}</TableCell>
                      <TableCell>{formatTime(row.updated_at)}</TableCell>
                    </TableRow>
                  ))}
                  {ops.length === 0 ? (
                    <TableRow>
                      <TableCell colSpan={6} className="text-muted-foreground">
                        No treasury ops.
                      </TableCell>
                    </TableRow>
                  ) : null}
                </TableBody>
              </Table>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="incidents" className="space-y-4">
          <Card>
            <CardHeader>
              <CardTitle>Quarantine Queue</CardTitle>
              <CardDescription>Close out quarantined holdings when intervention is required.</CardDescription>
            </CardHeader>
            <CardContent>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Contract</TableHead>
                    <TableHead>Reason</TableHead>
                    <TableHead>Account</TableHead>
                    <TableHead>Amount</TableHead>
                    <TableHead>Created</TableHead>
                    <TableHead>Action</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {quarantine.map((row) => (
                    <TableRow key={row.contract_id}>
                      <TableCell>{row.contract_id}</TableCell>
                      <TableCell>{row.reason}</TableCell>
                      <TableCell>{row.related_account_id ?? "-"}</TableCell>
                      <TableCell>{formatMinor(row.holding_amount_minor)}</TableCell>
                      <TableCell>{formatTime(row.created_at)}</TableCell>
                      <TableCell>
                        <Button
                          size="xs"
                          variant="outline"
                          disabled={pendingActionId === row.contract_id}
                          onClick={async () => {
                            setPendingActionId(row.contract_id);
                            setActionStatus(`closeout ${row.contract_id}...`);
                            try {
                              await api.closeoutAdminQuarantineHolding(apiBase, auth, row.contract_id);
                              setActionStatus(`closed out ${row.contract_id}`);
                              await refresh();
                            } catch (err: unknown) {
                              setActionStatus(describeError(err));
                            } finally {
                              setPendingActionId(null);
                            }
                          }}
                        >
                          Closeout
                        </Button>
                      </TableCell>
                    </TableRow>
                  ))}
                  {quarantine.length === 0 ? (
                    <TableRow>
                      <TableCell colSpan={6} className="text-muted-foreground">
                        No quarantined holdings.
                      </TableCell>
                    </TableRow>
                  ) : null}
                </TableBody>
              </Table>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>Escalated Withdrawal Cancels</CardTitle>
              <CardDescription>Reconcile cancel-escalated withdrawals.</CardDescription>
            </CardHeader>
            <CardContent>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Contract</TableHead>
                    <TableHead>Account</TableHead>
                    <TableHead>Withdrawal</TableHead>
                    <TableHead>Amount</TableHead>
                    <TableHead>Latest Status</TableHead>
                    <TableHead>Action</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {escalatedWithdrawals.map((row) => (
                    <TableRow key={row.contract_id}>
                      <TableCell>{row.contract_id}</TableCell>
                      <TableCell>{row.account_id}</TableCell>
                      <TableCell>{row.withdrawal_id}</TableCell>
                      <TableCell>{formatMinor(row.amount_minor)}</TableCell>
                      <TableCell>{row.latest_status_class ?? "-"}</TableCell>
                      <TableCell>
                        <Button
                          size="xs"
                          variant="outline"
                          disabled={pendingActionId === row.contract_id}
                          onClick={async () => {
                            setPendingActionId(row.contract_id);
                            setActionStatus(`reconciling ${row.contract_id}...`);
                            try {
                              await api.reconcileAdminEscalatedWithdrawal(apiBase, auth, row.contract_id);
                              setActionStatus(`reconciled ${row.contract_id}`);
                              await refresh();
                            } catch (err: unknown) {
                              setActionStatus(describeError(err));
                            } finally {
                              setPendingActionId(null);
                            }
                          }}
                        >
                          Reconcile
                        </Button>
                      </TableCell>
                    </TableRow>
                  ))}
                  {escalatedWithdrawals.length === 0 ? (
                    <TableRow>
                      <TableCell colSpan={6} className="text-muted-foreground">
                        No escalated withdrawals.
                      </TableCell>
                    </TableRow>
                  ) : null}
                </TableBody>
              </Table>
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>

      <Card>
        <CardContent className="py-3 text-xs text-muted-foreground">
          <div className="flex flex-wrap items-center gap-3">
            <span>session scope: admin</span>
            <span>markets: {markets.length}</span>
            <span>ops: {ops.length}</span>
            <span>quarantine: {quarantine.length}</span>
            <span>escalated: {escalatedWithdrawals.length}</span>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
