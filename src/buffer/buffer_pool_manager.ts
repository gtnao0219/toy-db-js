import { DiskManager } from "../storage/disk/disk_manager";
import { Page, PageType } from "../storage/page/page";
import { LRUReplacer } from "./lru_replacer";
import { Replacer } from "./replacer";

const POOL_SIZE = 10;

export class BufferPoolManager {
  private diskManager: DiskManager;
  private pages: Array<Page | null>;
  private pageTable: Map<number, number>;
  private freeFrameIds: number[];
  private replacer: Replacer;
  constructor(
    diskManager: DiskManager,
    replacer: Replacer = new LRUReplacer()
  ) {
    this.diskManager = diskManager;
    this.pages = new Array(POOL_SIZE).fill(0).map((_) => null);
    this.pageTable = new Map();
    this.freeFrameIds = new Array(POOL_SIZE).fill(0).map((_, i) => i);
    this.replacer = replacer;
  }
  fetchPage(pageId: number): Page | null {
    const frameId = this.getFrameId(pageId);
    if (frameId != null) {
      const page = this.getPage(frameId);
      if (page == null) {
        return null;
      }
      page.addPinCount();
      this.replacer.pin(frameId);
      return page;
    }
    const availableFrameId = this.getAvailableFrameId();
    if (availableFrameId == null) {
      return null;
    }
    // TODO: page type
    const page = this.diskManager.readPage(pageId, PageType.TABLE_PAGE);
    this.pages[availableFrameId] = page;
    this.pageTable.set(pageId, availableFrameId);
    page.addPinCount();
    this.replacer.pin(availableFrameId);
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
    if (page.getPinCount() === 0) {
      this.replacer.unpin(frameId);
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
    if (page.isDirty()) {
      this.diskManager.writePage(page);
    }
    this.pages[frameId] = null;
    this.pageTable.delete(pageId);
    this.freeFrameIds.push(frameId);
    // TODO: replacer
  }
  flushAllPages(): void {
    this.pages.forEach((page) => {
      if (page != null && page.isDirty()) {
        this.diskManager.writePage(page);
      }
    });
  }
  private getPage(frameId: number): Page | null {
    return this.pages[frameId] ?? null;
  }
  private getFrameId(pageId: number): number | null {
    return this.pageTable.get(pageId) ?? null;
  }
  private getAvailableFrameId(): number | null {
    if (this.freeFrameIds.length !== 0) {
      return this.freeFrameIds.pop() ?? null;
    }
    const frameId = this.replacer.victim();
    if (frameId == null) {
      return null;
    }
    const page = this.getPage(frameId);
    if (page == null) {
      return null;
    }
    if (page.isDirty()) {
      this.diskManager.writePage(page);
    }
    this.pageTable.delete(page.getPageId());
    return frameId;
  }
}
