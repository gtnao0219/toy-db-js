export interface Replacer {
  victim(): number;
  pin(frameId: number): void;
  unpin(frameId: number): void;
  size(): number;
}
