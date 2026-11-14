import { z } from 'zod';

export const telemetryEventSchema = z.object({
  eventId: z.string().uuid(),
  deviceId: z.string().regex(/^BUS-\d{4}$/),
  seq: z.number().int().nonnegative(),
  ts: z.number().int().positive(),
  route: z.string().max(10),
  lat: z.number().min(12.8).max(13.2),
  lng: z.number().min(77.4).max(77.8),
  speed: z.number().min(0).max(120),
  soc: z.number().min(0).max(100),
  motorTemp: z.number().min(-10).max(120),
  tyrePsi: z.number().min(60).max(160),
  load: z.number().int().min(0).max(60),
});

export type TelemetryEvent = z.infer<typeof telemetryEventSchema>;

export const batchPayloadSchema = z.array(telemetryEventSchema).min(1).max(500);
export type BatchPayload = z.infer<typeof batchPayloadSchema>;

// Used by history merge logic
export interface HistoryPoint {
  seq: number;
  ts: number;
  speed?: number;
  soc?: number;
  motorTemp?: number;
  tyrePsi?: number;
  load?: number;
  lat?: number;
  lng?: number;
}
