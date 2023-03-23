export const PAGE_SIZE = 128;

export const INVALID_PAGE_ID = -1;

export enum PageType {
  TABLE_PAGE,
  HEADER_PAGE,
}

export abstract class Page {
  constructor(
    protected _buffer: ArrayBuffer,
    protected _isDirty: boolean = false,
    protected _pinCount: number = 0,
    protected _pageId: number = INVALID_PAGE_ID
  ) {}
  get pageId(): number {
    return this._pageId;
  }
  get isDirty(): boolean {
    return this._isDirty;
  }
  get pinCount(): number {
    return this._pinCount;
  }
  markDirty(): void {
    this._isDirty = true;
  }
  addPinCount(): void {
    this._pinCount++;
  }
  subPinCount(): void {
    this._pinCount--;
  }
  abstract serialize(): ArrayBuffer;
}
