import { Page } from "../storage/page/page";

export class BufferedPage {
  private _isDirty: boolean;
  private _pinCount: number;

  constructor(private _page: Page) {
    this._isDirty = false;
    this._pinCount = 0;
  }

  addPinCount(): void {
    this._pinCount++;
  }

  subPinCount(): void {
    if (this._pinCount > 0) {
      this._pinCount--;
    }
  }

  markDirty(): void {
    this._isDirty = true;
  }

  get page(): Page {
    return this._page;
  }

  get isDirty(): boolean {
    return this._isDirty;
  }

  get pinCount(): number {
    return this._pinCount;
  }

  get pageId(): number {
    return this._page.pageId;
  }

  serialize(): ArrayBuffer {
    return this._page.serialize();
  }
}
