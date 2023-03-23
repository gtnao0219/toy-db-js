import { DiskManager } from "../storage/disk/disk_manager";
import { Page, PageType } from "../storage/page/page";
import { TablePage } from "../storage/page/table_page";
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
  fetchPage(pageId: number, pageType: PageType): Page | null {
    const frameId = this.getFrameId(pageId);
    if (frameId != null) {
      const page = this.getPage(frameId);
      if (page == null) {
        return null;
      }
      page.addPinCount();
      this._replacer.pin(frameId);
      return page;
    }
    const availableFrameId = this.getAvailableFrameId();
    if (availableFrameId == null) {
      return null;
    }
    const page = this._diskManager.readPage(pageId, pageType);
    this._pages[availableFrameId] = page;
    this._pageTable.set(pageId, availableFrameId);
    page.addPinCount();
    this._replacer.pin(availableFrameId);
    return page;
  }
  unpinPage(pageId: number, isDirty: boolean): void {
    const frameId = this.getFrameId(pageId);
    if (frameId == null) {
      return;
    }
    const page = this.getPage(frameId);
    if (page == null) {
      return;
    }
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
      return;
    }
    const page = this.getPage(frameId);
    if (page == null) {
      return;
    }
    if (page.isDirty) {
      this._diskManager.writePage(page);
    }
    this._pages[frameId] = null;
    this._pageTable.delete(pageId);
    this._freeFrameIds.push(frameId);
    // TODO: replacer
  }
  flushAllPages(): void {
    this._pages.forEach((page) => {
      if (page != null && page.isDirty) {
        this._diskManager.writePage(page);
      }
    });
  }
  newPage(pageType: PageType): Page | null {
    const availableFrameId = this.getAvailableFrameId();
    if (availableFrameId == null) {
      return null;
    }
    const pageId = this._diskManager.allocatePage();
    const page = this.newEmptyPage(pageId, pageType);
    page.markDirty();
    this._pages[availableFrameId] = page;
    this._pageTable.set(pageId, availableFrameId);
    page.addPinCount();
    this._replacer.pin(availableFrameId);
    return page;
  }
  private newEmptyPage(pageId: number, pageType: PageType): Page {
    switch (pageType) {
      case PageType.TABLE_PAGE:
        return TablePage.newEmptyTablePage(pageId);
      case PageType.HEADER_PAGE:
        return TablePage.newEmptyTablePage(pageId);
    }
  }
  private getPage(frameId: number): Page | null {
    return this._pages[frameId] ?? null;
  }
  private getFrameId(pageId: number): number | null {
    return this._pageTable.get(pageId) ?? null;
  }
  private getAvailableFrameId(): number | null {
    if (this._freeFrameIds.length !== 0) {
      return this._freeFrameIds.pop() ?? null;
    }
    const frameId = this._replacer.victim();
    if (frameId == null) {
      return null;
    }
    const page = this.getPage(frameId);
    if (page == null) {
      return null;
    }
    if (page.isDirty) {
      this._diskManager.writePage(page);
    }
    this._pageTable.delete(page.pageId);
    return frameId;
  }
}
