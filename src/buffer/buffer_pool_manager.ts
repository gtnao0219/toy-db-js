import { DiskManager } from "../storage/disk/disk_manager";
import { Page, PageDeserializer, PageGenerator } from "../storage/page/page";
import { BufferedPage } from "./buffered_page";
import { LRUReplacer } from "./lru_replacer";
import { Replacer } from "./replacer";

const DEFAULT_POOL_SIZE = 32;

export interface BufferPoolManager {
  fetchPage(pageId: number, pageDeserializer: PageDeserializer): Promise<Page>;
  unpinPage(pageId: number, isDirty: boolean): void;
  newPage(pageGenerator: PageGenerator): Promise<Page>;
  flushPage(pageId: number): Promise<void>;
  flushAllPages(): Promise<void>;
}

export class BufferPoolManagerImpl implements BufferPoolManager {
  private _pages: Array<BufferedPage | null>;
  private _pageTable: Map<number, number> = new Map();
  private _freeFrameIds: number[];
  constructor(
    private _diskManager: DiskManager,
    private _poolSize: number = DEFAULT_POOL_SIZE,
    private _replacer: Replacer = new LRUReplacer()
  ) {
    this._pages = new Array(_poolSize).fill(0).map((_) => null);
    this._freeFrameIds = new Array(_poolSize).fill(0).map((_, i) => i);
  }
  async fetchPage(
    pageId: number,
    pageDeserializer: PageDeserializer
  ): Promise<Page> {
    const frameId = this.getFrameId(pageId);
    if (frameId != null) {
      const page = this.getPage(frameId);
      page.addPinCount();
      this._replacer.pin(frameId);
      return page.page;
    }
    const availableFrameId = await this.getAvailableFrameId();
    const buffer = await this._diskManager.readPage(pageId);
    const bufferedPage = new BufferedPage(pageDeserializer.deserialize(buffer));
    this._pages[availableFrameId] = bufferedPage;
    this._pageTable.set(pageId, availableFrameId);
    bufferedPage.addPinCount();
    this._replacer.pin(availableFrameId);
    return bufferedPage.page;
  }
  async newPage(pageGenerator: PageGenerator): Promise<Page> {
    const availableFrameId = await this.getAvailableFrameId();
    const pageId = await this._diskManager.allocatePageId();
    const page = pageGenerator.generate(pageId);
    const bufferedPage = new BufferedPage(page);
    this._pages[availableFrameId] = bufferedPage;
    this._pageTable.set(pageId, availableFrameId);
    bufferedPage.addPinCount();
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
  async flushPage(pageId: number): Promise<void> {
    const frameId = this.getFrameId(pageId);
    if (frameId == null) {
      throw new Error("page is not in buffer pool");
    }
    const page = this.getPage(frameId);
    if (page.isDirty) {
      await this._diskManager.writePage(page.pageId, page.serialize());
    }
    // TODO: should we delete the page from buffer pool?
  }
  async flushAllPages(): Promise<void> {
    for (const page of this._pages) {
      if (page != null) {
        await this._diskManager.writePage(page.pageId, page.serialize());
      }
    }
  }
  private getPage(frameId: number): BufferedPage {
    const bufferedPage = this._pages[frameId];
    if (bufferedPage == null) {
      throw new Error("page is null");
    }
    return bufferedPage;
  }
  private getFrameId(pageId: number): number | null {
    return this._pageTable.get(pageId) ?? null;
  }
  private async getAvailableFrameId(): Promise<number> {
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
      await this._diskManager.writePage(page.pageId, page.serialize());
    }
    this._pageTable.delete(page.pageId);
    return frameId;
  }
}
