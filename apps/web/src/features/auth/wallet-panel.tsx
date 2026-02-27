import React from "react";

import { useSessionContext } from "@/app/session";
import { describeError } from "@/app/session";
import * as api from "@/lib/api";
import { Button } from "@/components/ui/button";
import {
  CardAction,
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { RiRefreshLine } from "@remixicon/react";
import { toast } from "sonner";

const USD_FORMATTER = new Intl.NumberFormat(undefined, {
  style: "currency",
  currency: "USD",
  minimumFractionDigits: 0,
  maximumFractionDigits: 2,
});

function formatUsdFromCents(value: number | null | undefined): string {
  if (value == null) {
    return "-";
  }
  return USD_FORMATTER.format(value / 100);
}

export function WalletPanel() {
  const {
    apiBase,
    auth,
    session,
    sessionStatus,
    setAdminApiKey,
    setUserApiKey,
    refreshSession,
  } = useSessionContext();

  const [authTab, setAuthTab] = React.useState<"login" | "register">("login");
  const [walletOpen, setWalletOpen] = React.useState<boolean>(false);
  const [draftKey, setDraftKey] = React.useState<string>(auth.userApiKey || auth.adminApiKey);
  const [registerUsername, setRegisterUsername] = React.useState<string>("");
  const [registerKey, setRegisterKey] = React.useState<string>("");

  const [connectStatus, setConnectStatus] = React.useState<string>("idle");
  const [registerStatus, setRegisterStatus] = React.useState<string>("idle");

  const [faucetAmountMinor, setFaucetAmountMinor] = React.useState<string>("1000000");

  const [fundingCapacity, setFundingCapacity] = React.useState<api.MyFundingCapacityView | null>(
    null,
  );
  const [fundingStatus, setFundingStatus] = React.useState<string>("idle");
  const fundingRefreshing = fundingStatus === "loading...";

  const hasSession = sessionStatus === "ok" && session !== null;
  const hasUserSession = hasSession && Boolean(session?.account_id);
  const hasAdminSession = hasSession && Boolean(session?.is_admin);

  React.useEffect(() => {
    if (hasSession) {
      return;
    }
    setDraftKey(auth.userApiKey || auth.adminApiKey);
  }, [auth.adminApiKey, auth.userApiKey, hasSession]);

  const loadFundingCapacity = React.useCallback(async () => {
    if (!auth.userApiKey.trim()) {
      setFundingCapacity(null);
      setFundingStatus("no user key connected");
      return;
    }

    setFundingStatus("loading...");
    try {
      const result = await api.getMyFundingCapacity(apiBase, {
        userApiKey: auth.userApiKey.trim(),
        adminApiKey: "",
      });
      setFundingCapacity(result);
      setFundingStatus("ok");
    } catch (err: unknown) {
      setFundingCapacity(null);
      setFundingStatus(describeError(err));
    }
  }, [apiBase, auth.userApiKey]);

  React.useEffect(() => {
    if (!hasUserSession) {
      setFundingCapacity(null);
      setFundingStatus("idle");
      return;
    }
    if (!walletOpen) {
      return;
    }

    void loadFundingCapacity();
    const interval = window.setInterval(() => {
      void loadFundingCapacity();
    }, 1_000);

    return () => {
      window.clearInterval(interval);
    };
  }, [hasUserSession, loadFundingCapacity, walletOpen]);

  const connectWithSingleKey = React.useCallback(
    async (rawKey: string) => {
      const key = rawKey.trim();
      if (!key) {
        setConnectStatus("Enter an API key");
        return;
      }

      setConnectStatus("Connecting...");

      const userAuth: api.AuthKeys = {
        userApiKey: key,
        adminApiKey: "",
      };
      const adminAuth: api.AuthKeys = {
        userApiKey: "",
        adminApiKey: key,
      };

      try {
        const userSession = await api.getSession(apiBase, userAuth);
        if (userSession.account_id) {
          setUserApiKey(key);
          setAdminApiKey("");
          await refreshSession(userAuth);
          setConnectStatus("Connected");
          return;
        }
      } catch (err: unknown) {
        void err;
      }

      try {
        const adminSession = await api.getSession(apiBase, adminAuth);
        if (adminSession.is_admin) {
          setUserApiKey("");
          setAdminApiKey(key);
          await refreshSession(adminAuth);
          setConnectStatus("Connected");
          return;
        }
      } catch (err: unknown) {
        void err;
      }

      setUserApiKey("");
      setAdminApiKey("");
      await refreshSession({ userApiKey: "", adminApiKey: "" });
      setConnectStatus("Invalid API key");
    },
    [apiBase, refreshSession, setAdminApiKey, setUserApiKey],
  );

  const disconnect = React.useCallback(async () => {
    setDraftKey("");
    setRegisterUsername("");
    setRegisterKey("");
    setConnectStatus("idle");
    setRegisterStatus("idle");
    setFundingStatus("idle");
    setFundingCapacity(null);
    setAuthTab("login");

    setUserApiKey("");
    setAdminApiKey("");
    await refreshSession({ userApiKey: "", adminApiKey: "" });
  }, [refreshSession, setAdminApiKey, setUserApiKey]);

  const renderStatus = (status: string) => {
    if (status === "idle" || status === "ok") {
      return null;
    }
    return <p className="text-xs text-muted-foreground">{status}</p>;
  };

  return (
    <Dialog open={walletOpen} onOpenChange={setWalletOpen}>
      <DialogTrigger render={<Button variant="outline" size="sm" />}>Wallet</DialogTrigger>
      <DialogContent
        className="max-h-[90svh] overflow-y-auto p-6 sm:p-7"
        style={{ width: "26rem", maxWidth: "calc(100% - 2rem)" }}
      >
        <div className="space-y-8">
          <DialogHeader className="space-y-2">
            <DialogTitle>Wallet</DialogTitle>
          </DialogHeader>

          {hasSession ? (
            <div className="space-y-5">
              <Card size="sm">
                <CardContent className="flex items-center justify-between gap-3">
                  <div className="min-w-0">
                    <p className="text-sm font-medium capitalize">
                      {hasAdminSession ? "Admin session" : "User session"}
                    </p>
                    {session?.account_id ? (
                      <p className="truncate text-xs text-muted-foreground">{session.account_id}</p>
                    ) : null}
                  </div>
                  <Button
                    type="button"
                    variant="outline"
                    size="sm"
                    onClick={() => void disconnect()}
                  >
                    Disconnect
                  </Button>
                </CardContent>
              </Card>

              {hasUserSession ? (
                <>
                  <Card className="gap-2">
                    <CardHeader className="space-y-1">
                      <CardTitle>Available to deposit</CardTitle>
                      <CardAction>
                        <Button
                          type="button"
                          variant="outline"
                          size="icon-sm"
                          aria-label="Refresh available funds"
                          disabled={fundingRefreshing}
                          onClick={() => void loadFundingCapacity()}
                        >
                          <RiRefreshLine className={`size-4 ${fundingRefreshing ? "animate-spin" : ""}`} />
                        </Button>
                      </CardAction>
                    </CardHeader>
                    <CardContent className="space-y-3">
                      <p className="text-2xl font-semibold tabular-nums">
                        {formatUsdFromCents(fundingCapacity?.unlocked_holdings_minor)}
                      </p>
                      {fundingStatus !== "loading..." ? renderStatus(fundingStatus) : null}
                    </CardContent>
                  </Card>

                  <Card>
                    <CardHeader className="space-y-1">
                      <CardTitle>Faucet</CardTitle>
                    </CardHeader>
                    <CardContent className="space-y-5">
                      <label className="block space-y-2">
                        <span className="text-xs text-muted-foreground">Amount (cents)</span>
                        <Input
                          value={faucetAmountMinor}
                          onChange={(event) => setFaucetAmountMinor(event.target.value)}
                          placeholder="1000000"
                          inputMode="numeric"
                        />
                      </label>
                      <Button
                        type="button"
                        className="w-full"
                        onClick={async () => {
                          if (!auth.userApiKey.trim()) {
                            toast.error("Connect a user key first.");
                            return;
                          }
                          const amountMinor = Number(faucetAmountMinor);
                          if (!Number.isInteger(amountMinor) || amountMinor <= 0) {
                            toast.error("Amount must be an integer greater than 0.");
                            return;
                          }

                          const toastId = toast.loading("Minting...");
                          try {
                            const result = await api.faucetMe(
                              apiBase,
                              { userApiKey: auth.userApiKey.trim(), adminApiKey: "" },
                              {
                                amount_minor: amountMinor,
                              },
                            );
                            toast.success(`Minted ${formatUsdFromCents(result.amount_minor)}`, {
                              id: toastId,
                            });
                            await loadFundingCapacity();
                          } catch (err: unknown) {
                            toast.error("Mint failed.", {
                              id: toastId,
                              description: describeError(err),
                            });
                          }
                        }}
                      >
                        Mint
                      </Button>
                    </CardContent>
                  </Card>
                </>
              ) : (
                <Card size="sm">
                  <CardContent className="text-xs text-muted-foreground">
                    This key has admin scope only.
                  </CardContent>
                </Card>
              )}
            </div>
          ) : (
            <Tabs
              value={authTab}
              onValueChange={(value) => setAuthTab(value as "login" | "register")}
              className="space-y-3"
            >
              <TabsList variant="line" className="grid w-full grid-cols-2">
                <TabsTrigger value="login">Log in</TabsTrigger>
                <TabsTrigger value="register">Register</TabsTrigger>
              </TabsList>

              <TabsContent value="login" className="pt-1">
                <Card>
                  <CardContent className="space-y-6 pt-1">
                    <label className="block space-y-2">
                      <span className="text-xs text-muted-foreground">API key</span>
                      <Input
                        value={draftKey}
                        onChange={(event) => setDraftKey(event.target.value)}
                        placeholder="Enter your key"
                      />
                    </label>
                    <Button
                      type="button"
                      className="w-full"
                      onClick={() => void connectWithSingleKey(draftKey)}
                    >
                      Connect
                    </Button>
                    {renderStatus(connectStatus)}
                  </CardContent>
                </Card>
              </TabsContent>

              <TabsContent value="register" className="pt-1">
                <Card>
                  <CardContent className="space-y-5 pt-1">
                    <label className="block space-y-2">
                      <span className="text-xs text-muted-foreground">Username</span>
                      <Input
                        value={registerUsername}
                        onChange={(event) => setRegisterUsername(event.target.value)}
                        placeholder="alice"
                      />
                    </label>
                    <label className="block space-y-2">
                      <span className="text-xs text-muted-foreground">API key</span>
                      <Input
                        value={registerKey}
                        onChange={(event) => setRegisterKey(event.target.value)}
                        placeholder="Choose a key"
                      />
                    </label>
                    <Button
                      type="button"
                      className="w-full"
                      onClick={async () => {
                        const username = registerUsername.trim();
                        const key = registerKey.trim();
                        if (!username) {
                          setRegisterStatus("Username is required");
                          return;
                        }
                        if (!key) {
                          setRegisterStatus("API key is required");
                          return;
                        }

                        setRegisterStatus("Creating account...");
                        try {
                          const result = await api.registerUser(apiBase, {
                            username,
                            api_key: key,
                          });
                          setUserApiKey(key);
                          setAdminApiKey("");
                          await refreshSession({ userApiKey: key, adminApiKey: "" });
                          setRegisterStatus(`Registered ${result.account_id}`);
                        } catch (err: unknown) {
                          setRegisterStatus(describeError(err));
                        }
                      }}
                    >
                      Create account
                    </Button>
                    {renderStatus(registerStatus)}
                  </CardContent>
                </Card>
              </TabsContent>
            </Tabs>
          )}
        </div>
      </DialogContent>
    </Dialog>
  );
}
