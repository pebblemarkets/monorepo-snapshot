export type OrderSide = "Buy" | "Sell";
export type OrderType = "Limit" | "Market";

export type AuthKeys = {
  userApiKey: string;
  adminApiKey: string;
};

type ApiRequestAuth = {
  useUserApiKey?: boolean;
  useAdminApiKey?: boolean;
};

export class ApiClientError extends Error {
  status: number;
  body: unknown;

  constructor(message: string, status: number, body: unknown) {
    super(message);
    this.name = "ApiClientError";
    this.status = status;
    this.body = body;
  }
}

function ensureApiBase(apiBase: string): string {
  return apiBase.replace(/\/$/, "");
}

function createRequestId(): string {
  if (typeof globalThis !== "undefined" && "crypto" in globalThis) {
    const cryptoRef = globalThis.crypto;
    if (cryptoRef?.randomUUID) {
      return cryptoRef.randomUUID();
    }
  }

  return `${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

function buildAuthHeaders(auth: AuthKeys, opts: ApiRequestAuth): Record<string, string> {
  const headers: Record<string, string> = {};

  if (opts.useUserApiKey) {
    const value = auth.userApiKey.trim();
    if (!value) {
      throw new Error("missing user API key");
    }
    headers["x-api-key"] = value;
  }

  if (opts.useAdminApiKey) {
    const value = auth.adminApiKey.trim();
    if (!value) {
      throw new Error("missing admin API key");
    }
    headers["x-admin-key"] = value;
  }

  return headers;
}

async function parseResponseBody(response: Response): Promise<unknown> {
  const text = await response.text();
  if (!text) {
    return null;
  }

  try {
    return JSON.parse(text) as unknown;
  } catch {
    return text;
  }
}

export async function apiRequest<T>(
  apiBase: string,
  path: string,
  init: RequestInit,
  auth: AuthKeys,
  authOpts: ApiRequestAuth = {},
): Promise<T> {
  const headers = new Headers(init.headers);
  if (!headers.has("x-request-id")) {
    headers.set("x-request-id", createRequestId());
  }
  const authHeaders = buildAuthHeaders(auth, authOpts);

  for (const [key, value] of Object.entries(authHeaders)) {
    headers.set(key, value);
  }

  const response = await fetch(`${ensureApiBase(apiBase)}${path}`, {
    ...init,
    headers,
  });

  const body = await parseResponseBody(response);
  if (!response.ok) {
    const message =
      typeof body === "object" &&
      body !== null &&
      "error" in body &&
      typeof body.error === "string"
        ? body.error
        : `request failed (${response.status})`;
    throw new ApiClientError(message, response.status, body);
  }

  return body as T;
}

export async function getHealthz(
  apiBase: string,
): Promise<{ status: string; m3_api_keys?: number; m4_admin_keys?: number }> {
  return apiRequest(
    apiBase,
    "/healthz",
    { method: "GET" },
    { userApiKey: "", adminApiKey: "" },
  );
}

export type SessionView = {
  account_id: string | null;
  is_admin: boolean;
};

export async function getSession(
  apiBase: string,
  auth: AuthKeys,
): Promise<SessionView> {
  return apiRequest(apiBase, "/auth/session", { method: "GET" }, auth, {
    useUserApiKey: auth.userApiKey.trim().length > 0,
    useAdminApiKey: auth.adminApiKey.trim().length > 0,
  });
}

export type RegisterUserRequest = {
  username: string;
  api_key: string;
};

export type RegisterUserResponse = {
  username: string;
  account_id: string;
  owner_party: string;
  instrument_admin: string;
  instrument_id: string;
  account_status: string;
  wallet_contract_id: string;
  account_created: boolean;
  key_created: boolean;
  account_ref_contract_id: string;
  account_state_contract_id: string;
  update_id: string | null;
  offset: number | null;
};

export async function registerUser(
  apiBase: string,
  body: RegisterUserRequest,
): Promise<RegisterUserResponse> {
  return apiRequest(
    apiBase,
    "/auth/register",
    {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    },
    { userApiKey: "", adminApiKey: "" },
  );
}

export type MeOnboardRequest = {
  owner_party?: string;
  instrument_admin?: string;
  instrument_id?: string;
};

export type MeOnboardResponse = {
  account_id: string;
  owner_party: string;
  instrument_admin: string;
  instrument_id: string;
  account_status: string;
  wallet_contract_id: string;
  account_created: boolean;
  account_ref_contract_id: string;
  account_state_contract_id: string;
  update_id: string | null;
  offset: number | null;
};

export async function onboardMeAccount(
  apiBase: string,
  auth: AuthKeys,
  body: MeOnboardRequest,
): Promise<MeOnboardResponse> {
  return apiRequest(
    apiBase,
    "/me/onboard",
    {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    },
    auth,
    { useUserApiKey: true },
  );
}

export type MeFaucetRequest = {
  amount_minor?: number;
};

export type MeFaucetResponse = {
  account_id: string;
  amount_minor: number;
  wallet_contract_id: string;
  holding_claim_contract_id: string;
  holding_contract_id: string;
  update_id: string;
  offset: number;
};

export async function faucetMe(
  apiBase: string,
  auth: AuthKeys,
  body: MeFaucetRequest,
): Promise<MeFaucetResponse> {
  return apiRequest(
    apiBase,
    "/me/faucet",
    {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    },
    auth,
    { useUserApiKey: true },
  );
}

export type CreateDepositRequest = {
  amount_minor: number;
  deposit_id?: string;
};

export type CreateDepositResponse = {
  account_id: string;
  deposit_id: string;
  wallet_contract_id: string;
  transfer_instruction_contract_id: string;
  input_holding_cids: string[];
  update_id: string;
  offset: number;
};

export async function createDepositRequest(
  apiBase: string,
  auth: AuthKeys,
  body: CreateDepositRequest,
): Promise<CreateDepositResponse> {
  return apiRequest(
    apiBase,
    "/me/deposits",
    {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    },
    auth,
    { useUserApiKey: true },
  );
}

export type MyFundingCapacityView = {
  account_id: string;
  instrument_admin: string;
  instrument_id: string;
  unlocked_holdings_minor: number;
  unlocked_holdings_count: number;
};

export async function getMyFundingCapacity(
  apiBase: string,
  auth: AuthKeys,
): Promise<MyFundingCapacityView> {
  return apiRequest(apiBase, "/me/funding-capacity", { method: "GET" }, auth, {
    useUserApiKey: true,
  });
}

export type WithdrawalClaimPendingView = {
  claim_contract_id: string;
  account_id: string;
  withdrawal_id: string;
  amount_minor: number;
  origin_instruction_cid: string;
  created_at: string;
};

export async function listMyWithdrawalClaimsPending(
  apiBase: string,
  auth: AuthKeys,
  limit = 50,
): Promise<WithdrawalClaimPendingView[]> {
  const qs = new URLSearchParams({ limit: String(limit) });
  return apiRequest(
    apiBase,
    `/me/withdrawal-claims/pending?${qs.toString()}`,
    { method: "GET" },
    auth,
    { useUserApiKey: true },
  );
}

export type ClaimWithdrawalClaimsRequest = {
  claim_contract_ids?: string[];
  withdrawal_id?: string;
  limit?: number;
};

export type ClaimWithdrawalClaimFailure = {
  claim_contract_id: string;
  code: string;
  message: string;
};

export type ClaimWithdrawalClaimsResponse = {
  account_id: string;
  attempted: number;
  claimed: number;
  already_claimed: number;
  failed: number;
  failures: ClaimWithdrawalClaimFailure[];
};

export async function claimMyWithdrawalClaims(
  apiBase: string,
  auth: AuthKeys,
  body: ClaimWithdrawalClaimsRequest = {},
): Promise<ClaimWithdrawalClaimsResponse> {
  return apiRequest(
    apiBase,
    "/me/withdrawal-claims/claim",
    {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    },
    auth,
    { useUserApiKey: true },
  );
}

export type MarketRow = {
  contract_id: string;
  market_id: string;
  question: string;
  outcomes: string[];
  status: string;
  resolved_outcome: string | null;
  created_at: string;
  active: boolean;
  last_offset: number;
};

export async function listMarkets(apiBase: string): Promise<MarketRow[]> {
  return apiRequest(
    apiBase,
    "/markets",
    { method: "GET" },
    { userApiKey: "", adminApiKey: "" },
  );
}

export type PublicStatsOverviewView = {
  tvl_minor: number;
  volume_24h_minor: number;
  fills_24h: number;
  markets_total: number;
  markets_open: number;
  markets_resolved: number;
  accounts_total: number;
  generated_at: string;
};

export async function getPublicStatsOverview(
  apiBase: string,
): Promise<PublicStatsOverviewView> {
  return apiRequest(
    apiBase,
    "/stats/overview",
    { method: "GET" },
    { userApiKey: "", adminApiKey: "" },
  );
}

export type PublicMarketStatsView = {
  market_id: string;
  question: string;
  status: string;
  resolved_outcome: string | null;
  created_at: string;
  fills_24h: number;
  volume_24h_minor: number;
  open_interest_minor: number;
  last_traded_price_ticks: number | null;
  last_traded_at: string | null;
};

export async function listPublicMarketStats(
  apiBase: string,
  limit = 20,
): Promise<PublicMarketStatsView[]> {
  const qs = new URLSearchParams({ limit: String(limit) });
  return apiRequest(
    apiBase,
    `/stats/markets?${qs.toString()}`,
    { method: "GET" },
    { userApiKey: "", adminApiKey: "" },
  );
}

export async function getPublicMarketStatsById(
  apiBase: string,
  marketId: string,
): Promise<PublicMarketStatsView | null> {
  const qs = new URLSearchParams({
    limit: "1",
    market_id: marketId,
  });
  const rows = await apiRequest<PublicMarketStatsView[]>(
    apiBase,
    `/stats/markets?${qs.toString()}`,
    { method: "GET" },
    { userApiKey: "", adminApiKey: "" },
  );
  return rows[0] ?? null;
}

export type MarketMetadataView = {
  market_id: string;
  question: string;
  outcomes: string[];
  status: string;
  created_at: string;
  category: string;
  tags: string[];
  featured: boolean;
  resolution_time: string | null;
  card_background_image_url: string | null;
  hero_background_image_url: string | null;
  thumbnail_image_url: string | null;
  updated_at: string | null;
};

export type ListMarketMetadataQuery = {
  limit?: number;
  market_id?: string;
  category?: string;
  tag?: string;
  featured?: boolean;
};

export async function listMarketMetadata(
  apiBase: string,
  query: ListMarketMetadataQuery = {},
): Promise<MarketMetadataView[]> {
  const qs = new URLSearchParams();
  if (query.limit !== undefined) {
    qs.set("limit", String(query.limit));
  }
  if (query.market_id) {
    qs.set("market_id", query.market_id);
  }
  if (query.category) {
    qs.set("category", query.category);
  }
  if (query.tag) {
    qs.set("tag", query.tag);
  }
  if (query.featured !== undefined) {
    qs.set("featured", String(query.featured));
  }

  return apiRequest(
    apiBase,
    `/markets/metadata${qs.toString() ? `?${qs.toString()}` : ""}`,
    { method: "GET" },
    { userApiKey: "", adminApiKey: "" },
  );
}

export async function getMarket(
  apiBase: string,
  marketId: string,
): Promise<MarketRow> {
  return apiRequest(
    apiBase,
    `/markets/${encodeURIComponent(marketId)}`,
    { method: "GET" },
    { userApiKey: "", adminApiKey: "" },
  );
}

export type OrderBookLevelView = {
  outcome: string;
  price_ticks: number;
  quantity_minor: number;
  order_count: number;
};

export type MarketOrderBookView = {
  market_id: string;
  bids: OrderBookLevelView[];
  asks: OrderBookLevelView[];
};

export async function getMarketOrderBook(
  apiBase: string,
  marketId: string,
): Promise<MarketOrderBookView> {
  return apiRequest(
    apiBase,
    `/markets/${encodeURIComponent(marketId)}/book`,
    { method: "GET" },
    { userApiKey: "", adminApiKey: "" },
  );
}

export type MarketFillView = {
  fill_id: string;
  fill_sequence: number;
  market_id: string;
  outcome: string;
  maker_order_id: string;
  taker_order_id: string;
  price_ticks: number;
  quantity_minor: number;
  engine_version: string;
  matched_at: string;
  clearing_epoch: number | null;
};

export type MarketChartRange = "1H" | "6H" | "1D" | "1W" | "1M" | "ALL";

export type MarketChartSamplePointView = {
  ts: string;
  price_ticks: number | null;
};

export type MarketChartUpdateFillView = {
  fill_sequence: number;
  price_ticks: number;
  matched_at: string;
};

export type MarketChartSnapshotView = {
  market_id: string;
  range: MarketChartRange;
  start_at: string;
  end_at: string;
  sample_points: number;
  samples: MarketChartSamplePointView[];
  last_fill_sequence: number | null;
  previous_fill: MarketFillView | null;
  fills: MarketFillView[];
};

export type MarketChartUpdatesEvent = {
  market_id: string;
  fills: MarketChartUpdateFillView[];
  last_fill_sequence: number;
};

export async function listMarketFills(
  apiBase: string,
  marketId: string,
  limit = 100,
): Promise<MarketFillView[]> {
  const qs = new URLSearchParams({ limit: String(limit) });
  return apiRequest(
    apiBase,
    `/markets/${encodeURIComponent(marketId)}/fills?${qs.toString()}`,
    { method: "GET" },
    { userApiKey: "", adminApiKey: "" },
  );
}

export async function getMarketChartSnapshot(
  apiBase: string,
  marketId: string,
  range: MarketChartRange,
  outcome?: string,
  samplePoints?: number,
): Promise<MarketChartSnapshotView> {
  const qs = new URLSearchParams({ range });
  if (outcome) {
    qs.set("outcome", outcome);
  }
  if (samplePoints !== undefined) {
    qs.set("sample_points", String(samplePoints));
  }
  return apiRequest(
    apiBase,
    `/markets/${encodeURIComponent(marketId)}/chart/snapshot?${qs.toString()}`,
    { method: "GET" },
    { userApiKey: "", adminApiKey: "" },
  );
}

export function createMarketChartUpdatesStream(
  apiBase: string,
  marketId: string,
  outcome: string,
  afterFillSequence: number | null,
): EventSource {
  const qs = new URLSearchParams({ outcome });
  if (afterFillSequence !== null) {
    qs.set("after_fill_sequence", String(afterFillSequence));
  }
  const query = qs.toString();
  const path = `/markets/${encodeURIComponent(marketId)}/chart/updates${query ? `?${query}` : ""}`;
  return new EventSource(`${ensureApiBase(apiBase)}${path}`);
}

export type CreateMarketRequest = {
  question: string;
  outcomes: string[];
};

export type CreateMarketResponse = {
  market_id: string;
  contract_id: string;
  update_id: string;
  offset: number;
};

export async function createMarket(
  apiBase: string,
  auth: AuthKeys,
  body: CreateMarketRequest,
): Promise<CreateMarketResponse> {
  return apiRequest(
    apiBase,
    "/markets",
    {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    },
    auth,
    { useAdminApiKey: true },
  );
}

export async function closeMarket(
  apiBase: string,
  auth: AuthKeys,
  marketId: string,
): Promise<{ contract_id: string; update_id: string; offset: number }> {
  return apiRequest(
    apiBase,
    `/markets/${encodeURIComponent(marketId)}/close`,
    { method: "POST" },
    auth,
    { useAdminApiKey: true },
  );
}

export async function resolveMarket(
  apiBase: string,
  auth: AuthKeys,
  marketId: string,
  outcome: string,
): Promise<{ contract_id: string; update_id: string; offset: number }> {
  return apiRequest(
    apiBase,
    `/markets/${encodeURIComponent(marketId)}/resolve`,
    {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ outcome }),
    },
    auth,
    { useAdminApiKey: true },
  );
}

export type OrderRowView = {
  order_id: string;
  market_id: string;
  account_id: string;
  owner_party: string;
  outcome: string;
  side: OrderSide;
  order_type: string;
  tif: string;
  nonce: number;
  price_ticks: number;
  quantity_minor: number;
  remaining_minor: number;
  locked_minor: number;
  status: string;
  engine_version: string;
  submitted_at: string;
  updated_at: string;
};

export async function listMyOrders(
  apiBase: string,
  auth: AuthKeys,
  marketId?: string,
): Promise<OrderRowView[]> {
  const qs = new URLSearchParams();
  if (marketId) {
    qs.set("market_id", marketId);
  }
  return apiRequest(
    apiBase,
    `/orders${qs.toString() ? `?${qs.toString()}` : ""}`,
    { method: "GET" },
    auth,
    { useUserApiKey: true },
  );
}

export function nextOrderNonce(orders: ReadonlyArray<Pick<OrderRowView, "nonce">>): number {
  return orders.reduce((max, row) => Math.max(max, row.nonce), -1) + 1;
}

export function expectedNonceFromError(err: unknown): number | null {
  if (!(err instanceof ApiClientError) || err.status !== 400) {
    return null;
  }

  const match = /^nonce mismatch: expected (\d+), got \d+$/.exec(err.message.trim());
  if (!match?.[1]) {
    return null;
  }

  return Number(match[1]);
}

export type FillRowView = {
  fill_id: string;
  fill_sequence: number;
  market_id: string;
  outcome: string;
  maker_order_id: string;
  taker_order_id: string;
  perspective_role: string;
  price_ticks: number;
  quantity_minor: number;
  engine_version: string;
  matched_at: string;
  clearing_epoch: number | null;
};

export async function listMyFills(
  apiBase: string,
  auth: AuthKeys,
  marketId?: string,
): Promise<FillRowView[]> {
  const qs = new URLSearchParams();
  if (marketId) {
    qs.set("market_id", marketId);
  }
  return apiRequest(
    apiBase,
    `/fills${qs.toString() ? `?${qs.toString()}` : ""}`,
    { method: "GET" },
    auth,
    { useUserApiKey: true },
  );
}

export type PlaceOrderRequest = {
  market_id: string;
  outcome: string;
  side: OrderSide;
  order_type?: OrderType;
  nonce: number;
  price_ticks?: number;
  quantity_minor: number;
};

export type OrderFillView = {
  fill_id: string;
  fill_sequence: number;
  maker_order_id: string;
  taker_order_id: string;
  outcome: string;
  price_ticks: number;
  quantity_minor: number;
  engine_version: string;
};

export type PlaceOrderResponse = {
  order_id: string;
  account_id: string;
  market_id: string;
  outcome: string;
  side: string;
  order_type: string;
  tif: string;
  status: string;
  quantity_minor: number;
  remaining_minor: number;
  locked_minor: number;
  nonce: number;
  available_minor_after: number;
  fills: OrderFillView[];
};

export async function placeOrder(
  apiBase: string,
  auth: AuthKeys,
  body: PlaceOrderRequest,
): Promise<PlaceOrderResponse> {
  return apiRequest(
    apiBase,
    "/orders",
    {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    },
    auth,
    { useUserApiKey: true },
  );
}

export async function cancelOrder(
  apiBase: string,
  auth: AuthKeys,
  orderId: string,
): Promise<{ order_id: string; status: string; released_locked_minor: number }> {
  return apiRequest(
    apiBase,
    `/orders/${encodeURIComponent(orderId)}/cancel`,
    { method: "POST" },
    auth,
    { useUserApiKey: true },
  );
}

export type TokenConfigView = {
  instrument_admin: string;
  instrument_id: string;
  symbol: string;
  decimals: number;
  deposit_mode: string;
  inbound_requires_acceptance: boolean;
  dust_threshold_minor: number;
  hard_max_inputs_per_transfer: number;
  operational_max_inputs_per_transfer: number;
  target_utxo_count_min: number | null;
  target_utxo_count_max: number | null;
  withdrawal_fee_headroom_minor: number | null;
  unexpected_fee_buffer_minor: number | null;
  requires_deposit_id_metadata: boolean | null;
  allowed_deposit_pending_status_classes: string[] | null;
  allowed_withdrawal_pending_status_classes: string[] | null;
  allowed_cancel_pending_status_classes: string[] | null;
};

export type FeeScheduleView = {
  instrument_admin: string;
  instrument_id: string;
  deposit_policy: string;
  withdrawal_policy: string;
};

export type MySummaryView = {
  account_id: string;
  owner_party: string;
  committee_party: string;
  instrument_admin: string;
  instrument_id: string;
  account_status: string;
  cleared_cash_minor: number;
  delta_pending_trades_minor: number;
  locked_open_orders_minor: number;
  available_minor: number;
  pending_withdrawals_reserved_minor: number;
  withdrawable_minor: number;
  effective_available_minor: number;
  token_config: TokenConfigView | null;
  fee_schedule: FeeScheduleView | null;
};

export async function getMySummary(
  apiBase: string,
  auth: AuthKeys,
): Promise<MySummaryView> {
  return apiRequest(apiBase, "/me/summary", { method: "GET" }, auth, {
    useUserApiKey: true,
  });
}

export type PositionView = {
  account_id: string;
  market_id: string;
  question: string | null;
  outcome: string;
  net_quantity_minor: number;
  avg_entry_price_ticks: number | null;
  realized_pnl_minor: number;
  mark_price_ticks: number | null;
  mark_value_minor: number | null;
  updated_at: string;
  market_status: string | null;
  market_resolved_outcome: string | null;
};

export async function listMyPositions(
  apiBase: string,
  auth: AuthKeys,
  limit = 200,
): Promise<PositionView[]> {
  const qs = new URLSearchParams({ limit: String(limit) });
  return apiRequest(
    apiBase,
    `/me/positions?${qs.toString()}`,
    { method: "GET" },
    auth,
    {
      useUserApiKey: true,
    },
  );
}

export type PortfolioHistoryView = {
  contract_id: string;
  account_id: string;
  cleared_cash_minor: number;
  position_mark_value_minor: number;
  total_equity_minor: number;
  last_applied_epoch: number;
  created_at: string;
  active: boolean;
};

export async function listMyPortfolioHistory(
  apiBase: string,
  auth: AuthKeys,
  limit = 200,
): Promise<PortfolioHistoryView[]> {
  const qs = new URLSearchParams({ limit: String(limit) });
  return apiRequest(
    apiBase,
    `/me/portfolio-history?${qs.toString()}`,
    { method: "GET" },
    auth,
    {
      useUserApiKey: true,
    },
  );
}

export type DepositInstructionMetadataField = {
  key: string;
  required: boolean;
  value_hint: string;
};

export type DepositInstructionsView = {
  account_id: string;
  instrument_admin: string;
  instrument_id: string;
  recipient_party: string;
  reason_hint: string;
  required_metadata: DepositInstructionMetadataField[];
  token_config: TokenConfigView | null;
  fee_schedule: FeeScheduleView | null;
};

export async function getMyDepositInstructions(
  apiBase: string,
  auth: AuthKeys,
): Promise<DepositInstructionsView> {
  return apiRequest(
    apiBase,
    "/me/deposit-instructions",
    { method: "GET" },
    auth,
    {
      useUserApiKey: true,
    },
  );
}

export type DepositPendingView = {
  contract_id: string;
  deposit_id: string;
  instrument_admin: string;
  instrument_id: string;
  lineage_root_instruction_cid: string;
  current_instruction_cid: string;
  step_seq: number;
  expected_amount_minor: number;
  metadata: Record<string, string>;
  reason: string;
  created_at: string;
  last_offset: number;
  latest_status_class: string | null;
  latest_output: string | null;
  latest_pending_actions: string[] | null;
};

export async function listMyDepositPendings(
  apiBase: string,
  auth: AuthKeys,
): Promise<DepositPendingView[]> {
  return apiRequest(apiBase, "/me/deposits/pending", { method: "GET" }, auth, {
    useUserApiKey: true,
  });
}

export type WithdrawalPendingView = {
  contract_id: string;
  withdrawal_id: string;
  instrument_admin: string;
  instrument_id: string;
  amount_minor: number;
  lineage_root_instruction_cid: string;
  current_instruction_cid: string;
  step_seq: number;
  pending_state: string;
  pending_actions: string[];
  metadata: Record<string, string>;
  reason: string;
  created_at: string;
  last_offset: number;
  latest_status_class: string | null;
  latest_output: string | null;
  latest_pending_actions: string[] | null;
};

export async function listMyWithdrawalPendings(
  apiBase: string,
  auth: AuthKeys,
): Promise<WithdrawalPendingView[]> {
  return apiRequest(
    apiBase,
    "/me/withdrawals/pending",
    { method: "GET" },
    auth,
    {
      useUserApiKey: true,
    },
  );
}

export type ReceiptView = {
  receipt_id: string;
  kind: string;
  status: string;
  amount_minor: number;
  instrument_admin: string;
  instrument_id: string;
  reference_id: string;
  lineage_root_instruction_cid: string;
  terminal_instruction_cid: string;
  created_at: string;
};

export async function listMyReceipts(
  apiBase: string,
  auth: AuthKeys,
): Promise<ReceiptView[]> {
  return apiRequest(apiBase, "/me/receipts", { method: "GET" }, auth, {
    useUserApiKey: true,
  });
}

export type CreateWithdrawalRequestBody = {
  amount_minor: number;
  withdrawal_id?: string;
  idempotency_key?: string;
};

export type CreateWithdrawalResponse = {
  account_id: string;
  withdrawal_id: string;
  contract_id: string | null;
  update_id: string | null;
  offset: number | null;
  request_state: string;
  create_intent_state: string;
  withdrawable_minor_before: number;
  pending_withdrawals_reserved_minor_after: number;
};

export async function createWithdrawalRequest(
  apiBase: string,
  auth: AuthKeys,
  body: CreateWithdrawalRequestBody,
): Promise<CreateWithdrawalResponse> {
  return apiRequest(
    apiBase,
    "/withdrawals",
    {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    },
    auth,
    { useUserApiKey: true },
  );
}

export type WithdrawalEligibilityView = {
  eligible: boolean;
  account_status: string;
  withdrawable_minor: number;
  pending_withdrawals_reserved_minor: number;
  blocking_reasons: string[];
};

export async function getMyWithdrawalEligibility(
  apiBase: string,
  auth: AuthKeys,
): Promise<WithdrawalEligibilityView> {
  return apiRequest(
    apiBase,
    "/me/withdrawal-eligibility",
    { method: "GET" },
    auth,
    { useUserApiKey: true },
  );
}

export type AdminQuarantineView = {
  contract_id: string;
  instrument_admin: string;
  instrument_id: string;
  holding_cid: string;
  reason: string;
  related_account_id: string | null;
  related_deposit_id: string | null;
  created_at: string;
  last_offset: number;
  holding_amount_minor: number | null;
};

export async function listAdminQuarantine(
  apiBase: string,
  auth: AuthKeys,
): Promise<AdminQuarantineView[]> {
  return apiRequest(apiBase, "/admin/quarantine", { method: "GET" }, auth, {
    useAdminApiKey: true,
  });
}

export type AdminQuarantineCloseoutResponse = {
  contract_id: string;
  action: string;
  update_id: string;
  offset: number;
};

export async function closeoutAdminQuarantineHolding(
  apiBase: string,
  auth: AuthKeys,
  contractId: string,
): Promise<AdminQuarantineCloseoutResponse> {
  return apiRequest(
    apiBase,
    `/admin/quarantine/${encodeURIComponent(contractId)}/closeout`,
    { method: "POST" },
    auth,
    { useAdminApiKey: true },
  );
}

export type AdminEscalatedWithdrawalView = {
  contract_id: string;
  owner_party: string;
  account_id: string;
  instrument_admin: string;
  instrument_id: string;
  withdrawal_id: string;
  amount_minor: number;
  current_instruction_cid: string;
  step_seq: number;
  pending_actions: string[];
  created_at: string;
  last_offset: number;
  latest_status_class: string | null;
  latest_output: string | null;
};

export async function listAdminEscalatedWithdrawals(
  apiBase: string,
  auth: AuthKeys,
): Promise<AdminEscalatedWithdrawalView[]> {
  return apiRequest(
    apiBase,
    "/admin/withdrawals/escalated",
    { method: "GET" },
    auth,
    { useAdminApiKey: true },
  );
}

export type AdminEscalatedWithdrawalReconcileResponse = {
  contract_id: string;
  action: string;
  latest_instruction_cid: string;
  latest_output: string;
  update_id: string | null;
  offset: number | null;
};

export async function reconcileAdminEscalatedWithdrawal(
  apiBase: string,
  auth: AuthKeys,
  contractId: string,
): Promise<AdminEscalatedWithdrawalReconcileResponse> {
  return apiRequest(
    apiBase,
    `/admin/withdrawals/escalated/${encodeURIComponent(contractId)}/reconcile`,
    { method: "POST" },
    auth,
    { useAdminApiKey: true },
  );
}

export type AdminInventoryView = {
  instrument_admin: string;
  instrument_id: string;
  total_holdings: number;
  unlocked_holdings: number;
  locked_holdings: number;
  lock_expired_holdings: number;
  reserved_holdings: number;
  dust_holdings: number;
  dust_ratio: number;
  total_amount_minor: number;
  target_utxo_count_min: number | null;
  target_utxo_count_max: number | null;
  dust_threshold_minor: number | null;
};

export async function getAdminTreasuryInventory(
  apiBase: string,
  auth: AuthKeys,
): Promise<AdminInventoryView[]> {
  return apiRequest(
    apiBase,
    "/admin/treasury/inventory",
    { method: "GET" },
    auth,
    {
      useAdminApiKey: true,
    },
  );
}

export type AdminTreasuryOpView = {
  op_id: string;
  op_type: string;
  instrument_admin: string;
  instrument_id: string;
  owner_party: string | null;
  account_id: string | null;
  amount_minor: number | null;
  state: string;
  step_seq: number;
  command_id: string;
  lineage_root_instruction_cid: string | null;
  current_instruction_cid: string | null;
  last_error: string | null;
  created_at: string;
  updated_at: string;
};

export async function listAdminTreasuryOps(
  apiBase: string,
  auth: AuthKeys,
): Promise<AdminTreasuryOpView[]> {
  return apiRequest(apiBase, "/admin/treasury/ops", { method: "GET" }, auth, {
    useAdminApiKey: true,
  });
}

export type AdminDriftView = {
  instrument_admin: string;
  instrument_id: string;
  total_holdings_minor: number;
  cleared_liabilities_minor: number;
  pending_withdrawals_minor: number;
  pending_deposits_minor: number;
  quarantined_minor: number;
  available_liquidity_minor: number;
  implied_obligations_minor: number;
  coverage_minor: number;
};

export async function getAdminDrift(
  apiBase: string,
  auth: AuthKeys,
): Promise<AdminDriftView[]> {
  return apiRequest(apiBase, "/admin/drift", { method: "GET" }, auth, {
    useAdminApiKey: true,
  });
}

export type UpsertMarketMetadataRequest = {
  category?: string;
  tags?: string[];
  featured?: boolean;
  resolution_time?: string;
  card_background_image_url?: string | null;
  hero_background_image_url?: string | null;
  thumbnail_image_url?: string | null;
};

export type AdminMarketAssetSlot = "card_background" | "hero_background" | "thumbnail";

export type UploadAdminMarketAssetRequest = {
  market_id: string;
  slot: AdminMarketAssetSlot;
  file: File;
};

export type UploadAdminMarketAssetResponse = {
  asset_url: string;
  filename: string;
  content_type: string;
  bytes: number;
};

export async function uploadAdminMarketAsset(
  apiBase: string,
  auth: AuthKeys,
  body: UploadAdminMarketAssetRequest,
): Promise<UploadAdminMarketAssetResponse> {
  const form = new FormData();
  form.append("market_id", body.market_id);
  form.append("slot", body.slot);
  form.append("file", body.file);

  return apiRequest(
    apiBase,
    "/admin/assets/upload",
    {
      method: "POST",
      body: form,
    },
    auth,
    { useAdminApiKey: true },
  );
}

export async function upsertAdminMarketMetadata(
  apiBase: string,
  auth: AuthKeys,
  marketId: string,
  body: UpsertMarketMetadataRequest,
): Promise<MarketMetadataView> {
  return apiRequest(
    apiBase,
    `/admin/markets/${encodeURIComponent(marketId)}/metadata`,
    {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    },
    auth,
    { useAdminApiKey: true },
  );
}
