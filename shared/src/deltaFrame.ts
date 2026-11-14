export type BusStatus = 0 | 1 | 2 | 3; // 0=ok, 1=warn, 2=crit, 3=offline

export interface BusFrame {
  la?: number;  // lat 5dp
  ln?: number;  // lng 5dp
  s?: number;   // speed 1dp
  b?: number;   // soc 1dp
  m?: number;   // motorTemp 1dp
  p?: number;   // tyrePsi 1dp
  lo?: number;  // load
  st?: BusStatus; // status
}

export interface DeltaFrame {
  t: number;   // server timestamp ms
  f: number;   // frame sequence number
  d: [string, BusFrame, number][];  // [deviceId, changed fields, seq]
}

export interface BusState {
  deviceId: string;
  route: string;
  lat: number;
  lng: number;
  speed: number;
  soc: number;
  motorTemp: number;
  tyrePsi: number;
  load: number;
  seq: number;
  ts: number;
  status: BusStatus;
  lastSeen: number;
}

export interface SnapshotPayload {
  buses: Record<string, BusState>;
  serverSeqCursor: number;
  serverTime: number;
}
