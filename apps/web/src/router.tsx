import { createRootRoute, createRoute, createRouter } from "@tanstack/react-router";

import {
  AdminRoutePage,
  ControlPanelFundsRoutePage,
  ControlPanelHomeRoutePage,
  ControlPanelMarketDetailRoutePage,
  ControlPanelMarketsRoutePage,
  ControlPanelOpsRoutePage,
  PortfolioRoutePage,
  RootLayoutPage,
  TradingRoutePage,
} from "@/app/router-pages";
import {
  BareboneFundsRoutePage,
  BareboneMarketDetailRoutePage,
  BareboneMarketsRoutePage,
  BareboneOpsRoutePage,
} from "@/features/barebone/pages";
import { BareboneLandingPage } from "@/features/barebone/landing-page";
import { DiscoveryPage } from "@/features/discovery/page";
import { LandingPage } from "@/features/landing/page";

const rootRoute = createRootRoute({
  component: RootLayoutPage,
});

const landingRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/",
  component: LandingPage,
});

const discoveryRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/markets",
  component: DiscoveryPage,
});

const tradingRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/markets/$marketId",
  component: TradingRoutePage,
});

const portfolioRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/portfolio",
  component: PortfolioRoutePage,
});

const adminRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/admin",
  component: AdminRoutePage,
});

const bareboneHomeRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/barebone",
  component: BareboneLandingPage,
});

const bareboneMarketsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/barebone/markets",
  component: BareboneMarketsRoutePage,
});

const bareboneMarketDetailRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/barebone/markets/$marketId",
  component: BareboneMarketDetailRoutePage,
});

const bareboneFundsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/barebone/funds",
  component: BareboneFundsRoutePage,
});

const bareboneOpsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/barebone/ops",
  component: BareboneOpsRoutePage,
});

const controlPanelHomeRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/control-panel",
  component: ControlPanelHomeRoutePage,
});

const controlPanelMarketsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/control-panel/markets",
  component: ControlPanelMarketsRoutePage,
});

const controlPanelMarketDetailRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/control-panel/markets/$marketId",
  component: ControlPanelMarketDetailRoutePage,
});

const controlPanelFundsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/control-panel/funds",
  component: ControlPanelFundsRoutePage,
});

const controlPanelOpsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/control-panel/ops",
  component: ControlPanelOpsRoutePage,
});

const routeTree = rootRoute.addChildren([
  landingRoute,
  discoveryRoute,
  tradingRoute,
  portfolioRoute,
  adminRoute,
  bareboneHomeRoute,
  bareboneMarketsRoute,
  bareboneMarketDetailRoute,
  bareboneFundsRoute,
  bareboneOpsRoute,
  controlPanelHomeRoute,
  controlPanelMarketsRoute,
  controlPanelMarketDetailRoute,
  controlPanelFundsRoute,
  controlPanelOpsRoute,
]);

export const router = createRouter({ routeTree });

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}
