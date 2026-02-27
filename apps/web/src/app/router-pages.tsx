import { Outlet, useParams, useRouterState } from "@tanstack/react-router";
import { TanStackRouterDevtools } from "@tanstack/react-router-devtools";

import { RequireAdminSession, RequireUserSession } from "@/app/guards";
import { AppLayout } from "@/app/layout";
import { SessionProvider } from "@/app/session-provider";
import { Toaster } from "@/components/ui/sonner";
import { AdminPage } from "@/features/admin/page";
import {
  ControlPanelFundsRoutePage as ControlPanelFundsPage,
  ControlPanelHomeRoutePage as ControlPanelHomePage,
  ControlPanelLayout,
  ControlPanelMarketDetailRoutePage as ControlPanelMarketDetailPage,
  ControlPanelMarketsRoutePage as ControlPanelMarketsPage,
  ControlPanelOpsRoutePage as ControlPanelOpsPage,
} from "@/features/barebone/pages";
import { PortfolioPage } from "@/features/portfolio/page";
import { TradingPage } from "@/features/trading/page";

export function RootLayoutPage() {
  const pathname = useRouterState({
    select: (state) => state.location.pathname,
  });
  const isControlPanelRoute =
    pathname === "/control-panel" || pathname.startsWith("/control-panel/");

  return (
    <SessionProvider>
      {isControlPanelRoute ? (
        <RequireAdminSession>
          <ControlPanelLayout>
            <Outlet />
          </ControlPanelLayout>
        </RequireAdminSession>
      ) : (
        <AppLayout>
          <Outlet />
        </AppLayout>
      )}
      <Toaster />
      {import.meta.env.DEV ? (
        <TanStackRouterDevtools
          initialIsOpen={false}
          toggleButtonProps={{
            style: { display: "none" },
          }}
        />
      ) : null}
    </SessionProvider>
  );
}

export function PortfolioRoutePage() {
  return (
    <RequireUserSession>
      <PortfolioPage />
    </RequireUserSession>
  );
}

export function AdminRoutePage() {
  return (
    <RequireAdminSession>
      <AdminPage />
    </RequireAdminSession>
  );
}

export function TradingRoutePage() {
  const { marketId } = useParams({ from: "/markets/$marketId" });
  return <TradingPage marketId={marketId} />;
}

export function ControlPanelHomeRoutePage() {
  return <ControlPanelHomePage />;
}

export function ControlPanelMarketsRoutePage() {
  return <ControlPanelMarketsPage />;
}

export function ControlPanelMarketDetailRoutePage() {
  return <ControlPanelMarketDetailPage />;
}

export function ControlPanelFundsRoutePage() {
  return <ControlPanelFundsPage />;
}

export function ControlPanelOpsRoutePage() {
  return <ControlPanelOpsPage />;
}
