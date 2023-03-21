export const PAGE_SIZE = 4096;

export const INVALID_PAGE_ID = -1;

export enum PageType {
  TABLE_PAGE,
}

export abstract class Page {
  protected dirty: boolean;
  protected pinCount: number;
  protected pageId: number;
  constructor() {
    this.dirty = false;
    this.pinCount = 0;
    this.pageId = INVALID_PAGE_ID;
  }
  getPageId(): number {
    return this.pageId;
  }
  isDirty(): boolean {
    return this.dirty;
  }
  markDirty(): void {
    this.dirty = true;
  }
  getPinCount(): number {
    return this.pinCount;
  }
  addPinCount(): void {
    this.pinCount++;
  }
  subPinCount(): void {
    this.pinCount--;
  }
  abstract serialize(): ArrayBuffer;
}
