export interface Replacer {
  victim(): number | null;
  pin(frameId: number): void;
  unpin(frameId: number): void;
}
