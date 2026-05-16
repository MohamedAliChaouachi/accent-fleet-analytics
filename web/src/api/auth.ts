import { request } from "./client";
import type { MeResponse, TokenPair } from "./types";

export function login(email: string, password: string): Promise<TokenPair> {
  return request<TokenPair>("/auth/login", {
    method: "POST",
    body: { email, password },
    anonymous: true,
  });
}

export function refresh(refresh_token: string): Promise<TokenPair> {
  return request<TokenPair>("/auth/refresh", {
    method: "POST",
    body: { refresh_token },
    anonymous: true,
  });
}

export function logout(refresh_token: string): Promise<void> {
  return request<void>("/auth/logout", {
    method: "POST",
    body: { refresh_token },
  });
}

export function me(): Promise<MeResponse> {
  return request<MeResponse>("/auth/me");
}
