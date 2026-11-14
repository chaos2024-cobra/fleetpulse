import { telemetryEventSchema, batchPayloadSchema, type TelemetryEvent } from '../schemas';

const validEvent: TelemetryEvent = {
  eventId: '123e4567-e89b-12d3-a456-426614174000',
  deviceId: 'BUS-0042',
  seq: 1,
  ts: 1731605000123,
  route: 'R1',
  lat: 12.97161,
  lng: 77.59456,
  speed: 34.2,
  soc: 71.4,
  motorTemp: 63.8,
  tyrePsi: 112.0,
  load: 38,
};

test('valid event passes', () => {
  const result = telemetryEventSchema.safeParse(validEvent);
  expect(result.success).toBe(true);
});

test('lat out of Bengaluru bounds fails', () => {
  const result = telemetryEventSchema.safeParse({ ...validEvent, lat: 0 });
  expect(result.success).toBe(false);
});

test('lng out of Bengaluru bounds fails', () => {
  const result = telemetryEventSchema.safeParse({ ...validEvent, lng: 100 });
  expect(result.success).toBe(false);
});

test('deviceId wrong format fails', () => {
  const result = telemetryEventSchema.safeParse({ ...validEvent, deviceId: 'BUS-042' });
  expect(result.success).toBe(false);
});

test('soc out of range fails', () => {
  const result = telemetryEventSchema.safeParse({ ...validEvent, soc: 101 });
  expect(result.success).toBe(false);
});

test('speed over 120 fails', () => {
  const result = telemetryEventSchema.safeParse({ ...validEvent, speed: 121 });
  expect(result.success).toBe(false);
});

test('batch of 1 valid event passes', () => {
  expect(batchPayloadSchema.safeParse([validEvent]).success).toBe(true);
});

test('batch over 500 fails', () => {
  const big = Array.from({ length: 501 }, () => validEvent);
  expect(batchPayloadSchema.safeParse(big).success).toBe(false);
});

test('empty batch fails', () => {
  expect(batchPayloadSchema.safeParse([]).success).toBe(false);
});
