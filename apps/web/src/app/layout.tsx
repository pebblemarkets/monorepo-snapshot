import { Link, useRouterState } from "@tanstack/react-router";
import { Button } from "@/components/ui/button";
import { Switch } from "@/components/ui/switch";
import { useSessionContext } from "@/app/session";
import { WalletPanel } from "@/features/auth/wallet-panel";
import { RiMoonLine, RiSunLine } from "@remixicon/react";
import React from "react";
import type { ReactNode } from "react";

import * as api from "@/lib/api";

type AppLayoutProps = {
  children: ReactNode;
};

export function AppLayout(props: AppLayoutProps) {
  const { apiBase, session } = useSessionContext();
  const pathname = useRouterState({
    select: (state) => state.location.pathname,
  });
  const [darkThemeEnabled, setDarkThemeEnabled] = React.useState<boolean>(() => {
    if (typeof window === "undefined") {
      return false;
    }

    const stored = window.localStorage.getItem("pebble-theme");
    if (stored === "dark") {
      return true;
    }
    if (stored === "light") {
      return false;
    }

    return window.matchMedia("(prefers-color-scheme: dark)").matches;
  });

  React.useEffect(() => {
    const root = document.documentElement;
    root.classList.toggle("dark", darkThemeEnabled);
    window.localStorage.setItem("pebble-theme", darkThemeEnabled ? "dark" : "light");
  }, [darkThemeEnabled]);
  const [apiStatus, setApiStatus] = React.useState<"checking" | "live" | "issue">("checking");

  React.useEffect(() => {
    let disposed = false;
    let intervalId: number | null = null;

    const refreshApiStatus = async () => {
      try {
        const health = await api.getHealthz(apiBase);
        if (disposed) {
          return;
        }

        const normalizedStatus = health.status.trim().toLowerCase();
        const healthy =
          normalizedStatus.length === 0 ||
          normalizedStatus === "ok" ||
          normalizedStatus === "healthy" ||
          normalizedStatus === "live";
        setApiStatus(healthy ? "live" : "issue");
      } catch {
        if (!disposed) {
          setApiStatus("issue");
        }
      }
    };

    void refreshApiStatus();
    intervalId = window.setInterval(() => {
      void refreshApiStatus();
    }, 1_000);

    return () => {
      disposed = true;
      if (intervalId !== null) {
        window.clearInterval(intervalId);
      }
    };
  }, [apiBase]);

  const apiStatusIndicator =
    apiStatus === "live"
      ? { label: "API live", dotClass: "bg-emerald-500" }
      : apiStatus === "checking"
        ? { label: "API syncing", dotClass: "bg-amber-500 animate-pulse" }
        : { label: "API issue", dotClass: "bg-red-500 animate-pulse" };

  const hasAdminSession = Boolean(session?.is_admin);

  const navLinks = [
    { label: "Home", to: "/" },
    { label: "Markets", to: "/markets" },
    { label: "Portfolio", to: "/portfolio" },
    ...(hasAdminSession
      ? [
          { label: "Admin", to: "/admin" },
        ]
      : []),
  ];

  const isActive = (to: string): boolean => {
    if (to === "/") {
      return pathname === "/";
    }

    return pathname === to || pathname.startsWith(`${to}/`);
  };

  return (
    <div className="min-h-screen bg-background text-foreground">
      <header className="border-b">
        <div className="mx-auto flex w-full max-w-7xl flex-col gap-4 px-4 py-4">
          <div className="flex flex-wrap items-end justify-between gap-2">
            <div>
              <div className="text-xl font-bold tracking-[-0.07em]">pebble</div>
              <p className="text-xs text-muted-foreground">
                The Canton outcome trading platform
              </p>
            </div>
            <div className="flex flex-wrap items-center gap-3">
              <label
                className="inline-flex items-center gap-2 border border-border/80 bg-background/80 px-2.5 py-1 text-xs text-muted-foreground"
                htmlFor="theme-switch"
              >
                <RiSunLine className="size-3.5" />
                <Switch
                  id="theme-switch"
                  aria-label="Enable dark mode"
                  checked={darkThemeEnabled}
                  onCheckedChange={(checked) => setDarkThemeEnabled(checked)}
                  size="sm"
                />
                <RiMoonLine className="size-3.5" />
              </label>
              {hasAdminSession ? (
                <Button
                  variant={isActive("/control-panel") ? "default" : "outline"}
                  size="sm"
                  nativeButton={false}
                  render={<Link to="/control-panel" />}
                >
                  Control panel
                </Button>
              ) : null}
              <WalletPanel />
            </div>
          </div>

          <nav className="flex flex-wrap items-center gap-2">
            {navLinks.map((entry) => (
              <Button
                key={entry.to}
                variant={isActive(entry.to) ? "default" : "outline"}
                size="sm"
                nativeButton={false}
                render={<Link to={entry.to} />}
              >
                {entry.label}
              </Button>
            ))}
          </nav>
        </div>
      </header>

      <main className="mx-auto w-full max-w-7xl px-4 py-8">{props.children}</main>

      <div className="pointer-events-none fixed bottom-4 left-4 z-40">
        <div className="inline-flex items-center gap-2 rounded-full border border-border/70 bg-background/90 px-3 py-1.5 text-xs text-foreground shadow-lg backdrop-blur">
          <span className={`h-2.5 w-2.5 rounded-full ${apiStatusIndicator.dotClass}`} />
          <span>{apiStatusIndicator.label}</span>
        </div>
      </div>
    </div>
  );
}
