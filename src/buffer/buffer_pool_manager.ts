import { DiskManager } from "../storage/disk/disk_manager";
import { Page, PageDeserializer, PageType } from "../storage/page/page";
import { newEmptyPage } from "../storage/page/page_generator";
import { LRUReplacer } from "./lru_replacer";
import { Replacer } from "./replacer";

const POOL_SIZE = 10;

export class BufferPoolManager {
  constructor(
    private _diskManager: DiskManager,
    private _replacer: Replacer = new LRUReplacer(),
    private _pages: Array<Page | null> = new Array(POOL_SIZE)
      .fill(0)
      .map((_) => null),
    private _pageTable: Map<number, number> = new Map(),
    private _freeFrameIds: number[] = new Array(POOL_SIZE)
      .fill(0)
      .map((_, i) => i)
  ) {}
  fetchPage(pageId: number, pageDeserializer: PageDeserializer): Page {
    const frameId = this.getFrameId(pageId);
    if (frameId != null) {
      const page = this.getPage(frameId);
      page.addPinCount();
      this._replacer.pin(frameId);
      return page;
    }
    const availableFrameId = this.getAvailableFrameId();
    const page = this._diskManager.readPage(pageId, pageDeserializer);
    this._pages[availableFrameId] = page;
    this._pageTable.set(pageId, availableFrameId);
    page.addPinCount();
    this._replacer.pin(availableFrameId);
    return page;
  }
  newPage(pageType: PageType): Page {
    const availableFrameId = this.getAvailableFrameId();
    const pageId = this._diskManager.allocatePageId();
    const page = newEmptyPage(pageId, pageType);
    this._pages[availableFrameId] = page;
    this._pageTable.set(pageId, availableFrameId);
    page.addPinCount();
    this._replacer.pin(availableFrameId);
    return page;
  }
  unpinPage(pageId: number, isDirty: boolean): void {
    const frameId = this.getFrameId(pageId);
    if (frameId == null) {
      throw new Error(`page(${pageId}) is not in buffer pool`);
    }
    const page = this.getPage(frameId);
    if (isDirty) {
      page.markDirty();
    }
    page.subPinCount();
    if (page.pinCount === 0) {
      this._replacer.unpin(frameId);
    }
  }
  flushPage(pageId: number): void {
    const frameId = this.getFrameId(pageId);
    if (frameId == null) {
      throw new Error("page is not in buffer pool");
    }
    const page = this.getPage(frameId);
    if (page.isDirty) {
      this._diskManager.writePage(page);
    }
    // TODO: should we delete the page from buffer pool?
  }
  flushAllPages(): void {
    this._pages.forEach((page) => {
      if (page != null) {
        this._diskManager.writePage(page);
      }
    });
  }
  private getPage(frameId: number): Page {
    const page = this._pages[frameId];
    if (page == null) {
      throw new Error("page is null");
    }
    return page;
  }
  private getFrameId(pageId: number): number | null {
    return this._pageTable.get(pageId) ?? null;
  }
  private getAvailableFrameId(): number {
    if (this._freeFrameIds.length !== 0) {
      const frameId = this._freeFrameIds.pop();
      if (frameId == null) {
        throw new Error("frameId is null");
      }
      return frameId;
    }
    const frameId = this._replacer.victim();
    const page = this.getPage(frameId);
    if (page.isDirty) {
      this._diskManager.writePage(page);
    }
    this._pageTable.delete(page.pageId);
    return frameId;
  }
}
