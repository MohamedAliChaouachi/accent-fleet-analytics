import { request } from "./client";
import type { DeviceProfileResponse } from "./types";

export function fetchDeviceProfile(
  deviceId: number,
  months = 12,
  signal?: AbortSignal,
) {
  return request<DeviceProfileResponse>(`/devices/${deviceId}/profile`, {
    query: { months },
    signal,
  });
}
