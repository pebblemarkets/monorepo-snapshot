import React from "react";

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

import { useSessionContext } from "@/app/session";

type GuardProps = {
  children: React.ReactNode;
};

function GuardMessage(props: { title: string; body: string }) {
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">{props.title}</CardTitle>
      </CardHeader>
      <CardContent>
        <p className="text-sm text-muted-foreground">{props.body}</p>
      </CardContent>
    </Card>
  );
}

export function RequireUserSession(props: GuardProps) {
  const { session, sessionStatus } = useSessionContext();

  if (sessionStatus === "checking...") {
    return (
      <GuardMessage
        title="Checking session"
        body="Validating user session before loading this page."
      />
    );
  }

  if (!session?.account_id) {
    return (
      <GuardMessage
        title="User session required"
        body="Connect a valid user API key to access this route."
      />
    );
  }

  return <>{props.children}</>;
}

export function RequireAdminSession(props: GuardProps) {
  const { session, sessionStatus } = useSessionContext();

  if (sessionStatus === "checking...") {
    return (
      <GuardMessage
        title="Checking session"
        body="Validating admin session before loading this page."
      />
    );
  }

  if (!session?.is_admin) {
    return (
      <GuardMessage
        title="Admin session required"
        body="Connect a valid admin API key to access this route."
      />
    );
  }

  return <>{props.children}</>;
}
