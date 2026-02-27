import * as api from "@/lib/api";

const USER_API_KEY_STORAGE = "pebble.user_api_key";
const ADMIN_API_KEY_STORAGE = "pebble.admin_api_key";

export function getApiBase(): string {
  return (
    import.meta.env.VITE_API_URL?.replace(/\/$/, "") ?? "http://127.0.0.1:3030"
  );
}

export function loadInitialUserApiKey(): string {
  if (typeof window === "undefined") {
    return "";
  }
  return window.localStorage.getItem(USER_API_KEY_STORAGE) ?? "";
}

export function loadInitialAdminApiKey(): string {
  if (typeof window === "undefined") {
    return "";
  }
  return window.localStorage.getItem(ADMIN_API_KEY_STORAGE) ?? "";
}

export function persistUserApiKey(value: string): void {
  if (typeof window === "undefined") {
    return;
  }
  if (!value) {
    window.localStorage.removeItem(USER_API_KEY_STORAGE);
    return;
  }
  window.localStorage.setItem(USER_API_KEY_STORAGE, value);
}

export function persistAdminApiKey(value: string): void {
  if (typeof window === "undefined") {
    return;
  }
  if (!value) {
    window.localStorage.removeItem(ADMIN_API_KEY_STORAGE);
    return;
  }
  window.localStorage.setItem(ADMIN_API_KEY_STORAGE, value);
}

export function describeError(err: unknown): string {
  if (err instanceof api.ApiClientError) {
    return `${err.message} (${err.status})`;
  }
  if (err instanceof Error) {
    return err.message;
  }
  return "unknown error";
}
