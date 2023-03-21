import { Replacer } from "./replacer";

export class LRUReplacer implements Replacer {
  private frameMap: Map<number, number>;
  constructor() {
    this.frameMap = new Map();
  }
  victim(): number | null {
    if (this.frameMap.size === 0) {
      return null;
    }
    let minTime = Number.MAX_SAFE_INTEGER;
    let minFrameId = -1;
    for (const [frameId, timestamp] of this.frameMap) {
      if (timestamp < minTime) {
        minTime = timestamp;
        minFrameId = frameId;
      }
    }
    this.frameMap.delete(minFrameId);
    return minFrameId;
  }
  pin(frameId: number): void {
    this.frameMap.delete(frameId);
  }
  unpin(frameId: number): void {
    this.frameMap.set(frameId, new Date().getTime());
  }
}
