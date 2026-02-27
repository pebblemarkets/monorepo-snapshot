import React from "react";

import * as api from "@/lib/api";
import { SessionContext, type SessionContextValue } from "@/app/session-context";
import {
  describeError,
  getApiBase,
  loadInitialAdminApiKey,
  loadInitialUserApiKey,
  persistAdminApiKey,
  persistUserApiKey,
} from "@/app/session-utils";

export function SessionProvider(props: { children: React.ReactNode }) {
  const apiBase = React.useMemo(() => getApiBase(), []);
  const [userApiKey, setUserApiKey] = React.useState<string>(loadInitialUserApiKey);
  const [adminApiKey, setAdminApiKey] = React.useState<string>(loadInitialAdminApiKey);
  const [session, setSession] = React.useState<api.SessionView | null>(null);
  const [sessionStatus, setSessionStatus] = React.useState<string>("not checked");

  React.useEffect(() => {
    persistUserApiKey(userApiKey.trim());
  }, [userApiKey]);

  React.useEffect(() => {
    persistAdminApiKey(adminApiKey.trim());
  }, [adminApiKey]);

  const auth = React.useMemo<api.AuthKeys>(
    () => ({
      userApiKey: userApiKey.trim(),
      adminApiKey: adminApiKey.trim(),
    }),
    [adminApiKey, userApiKey],
  );

  const refreshSession = React.useCallback(
    async (authOverride?: api.AuthKeys) => {
      const effectiveAuth = authOverride ?? auth;

      if (!effectiveAuth.userApiKey && !effectiveAuth.adminApiKey) {
        setSession(null);
        setSessionStatus("no keys configured");
        return;
      }

      setSessionStatus("checking...");
      try {
        const next = await api.getSession(apiBase, effectiveAuth);
        setSession(next);
        setSessionStatus("ok");
      } catch (err: unknown) {
        setSession(null);
        setSessionStatus(describeError(err));
      }
    },
    [apiBase, auth],
  );

  React.useEffect(() => {
    void refreshSession();
  }, [refreshSession]);

  const value = React.useMemo<SessionContextValue>(
    () => ({
      apiBase,
      auth,
      session,
      sessionStatus,
      setUserApiKey,
      setAdminApiKey,
      refreshSession,
    }),
    [apiBase, auth, refreshSession, session, sessionStatus],
  );

  return <SessionContext.Provider value={value}>{props.children}</SessionContext.Provider>;
}
