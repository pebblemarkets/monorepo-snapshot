import React from "react";

import * as api from "@/lib/api";

export type SessionContextValue = {
  apiBase: string;
  auth: api.AuthKeys;
  session: api.SessionView | null;
  sessionStatus: string;
  setUserApiKey: (value: string) => void;
  setAdminApiKey: (value: string) => void;
  refreshSession: (authOverride?: api.AuthKeys) => Promise<void>;
};

export const SessionContext = React.createContext<SessionContextValue | null>(null);

export function useSessionContext(): SessionContextValue {
  const value = React.useContext(SessionContext);
  if (!value) {
    throw new Error("session context is unavailable");
  }

  return value;
}
