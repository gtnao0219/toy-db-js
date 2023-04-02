import { Debuggable } from "../common/common";
import { Replacer } from "./replacer";

export class LRUReplacer implements Replacer, Debuggable {
  constructor(private _frameMap: Map<number, number> = new Map()) {}
  victim(): number {
    if (this._frameMap.size === 0) {
      throw new Error("no available frame");
    }
    let minTime = Number.MAX_SAFE_INTEGER;
    let minFrameId = -1;
    for (const [frameId, timestamp] of this._frameMap) {
      if (timestamp < minTime) {
        minTime = timestamp;
        minFrameId = frameId;
      }
    }
    this._frameMap.delete(minFrameId);
    return minFrameId;
  }
  pin(frameId: number): void {
    this._frameMap.delete(frameId);
  }
  unpin(frameId: number): void {
    this._frameMap.set(frameId, new Date().getTime());
  }
  size(): number {
    return this._frameMap.size;
  }
  debug(): object {
    return {
      frameMap: Array.from(this._frameMap.entries()),
    };
  }
}
