import { Link } from "@tanstack/react-router";

import { useSessionContext } from "@/app/session";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";

const sections = [
  {
    title: "Market Ops",
    description: "Create markets, inspect books, and test order placement against backend routes.",
    href: "/barebone/markets",
    cta: "Open markets tools",
  },
  {
    title: "Funds Ops",
    description: "Inspect balances, deposit/withdrawal states, and request withdrawal flows.",
    href: "/barebone/funds",
    cta: "Open funds tools",
  },
  {
    title: "Admin Ops",
    description: "Run privileged operator workflows like quarantines and escalated reconciliations.",
    href: "/barebone/ops",
    cta: "Open admin tools",
  },
] as const;

export function BareboneLandingPage() {
  const { sessionStatus, session } = useSessionContext();

  return (
    <div className="space-y-6">
      <Card className="py-0">
        <CardContent className="space-y-3 p-6">
          <Badge variant="outline">Internal testing surface</Badge>
          <h1 className="text-3xl font-semibold tracking-tight">Barebone API Playground</h1>
          <p className="max-w-3xl text-sm text-muted-foreground">
            This route keeps backend verification tools available while the user-facing pages evolve.
            All controls below use shadcn components and route into focused test panels.
          </p>
        </CardContent>
      </Card>

      <section className="grid gap-4 md:grid-cols-3">
        {sections.map((entry) => (
          <Card key={entry.href}>
            <CardHeader>
              <CardTitle>{entry.title}</CardTitle>
              <CardDescription>{entry.description}</CardDescription>
            </CardHeader>
            <CardContent>
              <Button
                variant="outline"
                nativeButton={false}
                render={<Link to={entry.href} />}
              >
                {entry.cta}
              </Button>
            </CardContent>
          </Card>
        ))}
      </section>

      <Card>
        <CardHeader>
          <CardTitle>Session Snapshot</CardTitle>
          <CardDescription>Current browser auth context for operator tool access.</CardDescription>
        </CardHeader>
        <CardContent className="space-y-1 text-sm">
          <div>
            status: <span className="font-medium">{sessionStatus}</span>
          </div>
          <div>
            account: <span className="font-medium">{session?.account_id ?? "-"}</span>
          </div>
          <div>
            admin: <span className="font-medium">{session?.is_admin ? "yes" : "no"}</span>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
