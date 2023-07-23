import { BufferPoolManager } from "../../buffer/buffer_pool_manager";
import { Catalog } from "../../catalog/catalog";
import { RID } from "../../common/RID";
import {
  BPlusTreeInternalPage,
  BPlusTreeInternalPageGenerator,
} from "../page/b_plus_tree_internal_page";
import {
  BPlusTreeLeafPage,
  BPlusTreeLeafPageDeserializer,
  BPlusTreeLeafPageGenerator,
  BPlusTreePageDeserializer,
} from "../page/b_plus_tree_leaf_page";
import { INVALID_PAGE_ID } from "../page/page";

export class BPlusTreeIndex {
  constructor(
    private _indexName: string,
    private _bufferPoolManager: BufferPoolManager,
    private _catalog: Catalog,
    private _rootPageId: number = INVALID_PAGE_ID
  ) {}
  isEmpty(): boolean {
    return this._rootPageId === INVALID_PAGE_ID;
  }
  async getValues(key: number): Promise<RID[]> {
    let leafPage = await this.findLeafPage(key);
    const values: RID[] = [];
    while (true) {
      const lookupValues = leafPage.lookup(key);
      const nextPageId = leafPage.nextPageId;
      this._bufferPoolManager.unpinPage(leafPage.pageId, false);
      if (lookupValues == null) {
        break;
      }
      values.push(...lookupValues);
      if (nextPageId == null) {
        break;
      }
      const nextPage = await this._bufferPoolManager.fetchPage(
        nextPageId,
        new BPlusTreeLeafPageDeserializer()
      );
      if (!(nextPage instanceof BPlusTreeLeafPage)) {
        throw new Error("Unexpected page type");
      }
      leafPage = nextPage;
    }
    return values;
  }
  async insert(key: number, value: RID): Promise<void> {
    if (this.isEmpty()) {
      await this.startNewTree(key, value);
      return;
    }
    await this.insertIntoLeafPage(key, value);
  }
  private async startNewTree(key: number, value: RID): Promise<void> {
    const page = await this._bufferPoolManager.newPage(
      new BPlusTreeLeafPageGenerator()
    );
    if (!(page instanceof BPlusTreeLeafPage)) {
      throw new Error("Unexpected page type");
    }
    page.insert(key, value);
    this._rootPageId = page.pageId;
    this._bufferPoolManager.unpinPage(page.pageId, true);

    await this._catalog.updateIndexRootPageId(
      this._indexName,
      this._rootPageId
    );
  }
  private async insertIntoLeafPage(key: number, value: RID): Promise<void> {
    const leafPage = await this.findLeafPage(key);
    leafPage.insert(key, value);
    if (leafPage.isFull()) {
      const siblingLeafPage = await this.split(leafPage);
      if (!(siblingLeafPage instanceof BPlusTreeLeafPage)) {
        throw new Error("Unexpected page type");
      }
      siblingLeafPage.nextPageId = leafPage.nextPageId;
      leafPage.nextPageId = siblingLeafPage.pageId;
      const risenKey = siblingLeafPage.keyAt(0);
      await this.insertIntoParentPage(leafPage, risenKey, siblingLeafPage);
      this._bufferPoolManager.unpinPage(leafPage.pageId, true);
      this._bufferPoolManager.unpinPage(siblingLeafPage.pageId, true);
    } else {
      this._bufferPoolManager.unpinPage(leafPage.pageId, true);
    }
  }
  private async split(
    page: BPlusTreeLeafPage | BPlusTreeInternalPage
  ): Promise<BPlusTreeLeafPage | BPlusTreeInternalPage> {
    if (page instanceof BPlusTreeLeafPage) {
      const siblingPage = await this._bufferPoolManager.newPage(
        new BPlusTreeLeafPageGenerator()
      );
      if (!(siblingPage instanceof BPlusTreeLeafPage)) {
        throw new Error("Unexpected page type");
      }
      page.moveHalfTo(siblingPage);
      return siblingPage;
    }
    if (page instanceof BPlusTreeInternalPage) {
      const siblingPage = await this._bufferPoolManager.newPage(
        new BPlusTreeInternalPageGenerator()
      );
      if (!(siblingPage instanceof BPlusTreeInternalPage)) {
        throw new Error("Unexpected page type");
      }
      await page.moveHalfTo(siblingPage, this._bufferPoolManager);
      return siblingPage;
    }
    throw new Error("Unexpected page type");
  }
  private async insertIntoParentPage(
    oldPage: BPlusTreeLeafPage | BPlusTreeInternalPage,
    risenKey: number,
    newPage: BPlusTreeLeafPage | BPlusTreeInternalPage
  ): Promise<void> {
    if (oldPage.pageId === this._rootPageId) {
      const rootPage = await this._bufferPoolManager.newPage(
        new BPlusTreeInternalPageGenerator()
      );
      if (!(rootPage instanceof BPlusTreeInternalPage)) {
        throw new Error("Unexpected page type");
      }
      rootPage.populateNewRoot(oldPage.pageId, risenKey, newPage.pageId);
      oldPage.parentPageId = rootPage.pageId;
      newPage.parentPageId = rootPage.pageId;
      this._rootPageId = rootPage.pageId;
      this._bufferPoolManager.unpinPage(rootPage.pageId, true);

      await this._catalog.updateIndexRootPageId(
        this._indexName,
        this._rootPageId
      );
      return;
    }
    const parentPage = await this._bufferPoolManager.fetchPage(
      oldPage.parentPageId,
      new BPlusTreePageDeserializer()
    );
    if (!(parentPage instanceof BPlusTreeInternalPage)) {
      throw new Error("Unexpected page type");
    }
    parentPage.insertNodeAfter(oldPage.pageId, risenKey, newPage.pageId);
    if (parentPage.isFull()) {
      const siblingPage = await this.split(parentPage);
      if (!(siblingPage instanceof BPlusTreeInternalPage)) {
        throw new Error("Unexpected page type");
      }
      const siblingRisenKey = siblingPage.keyAt(1);
      if (siblingRisenKey == null) {
        throw new Error("Unexpected key");
      }
      await this.insertIntoParentPage(parentPage, siblingRisenKey, siblingPage);
      this._bufferPoolManager.unpinPage(parentPage.pageId, true);
      this._bufferPoolManager.unpinPage(siblingPage.pageId, true);
    } else {
      this._bufferPoolManager.unpinPage(parentPage.pageId, true);
    }
  }
  private async findLeafPage(key: number): Promise<BPlusTreeLeafPage> {
    let pageId = this._rootPageId;
    while (true) {
      const page = await this._bufferPoolManager.fetchPage(
        pageId,
        new BPlusTreePageDeserializer()
      );
      if (page instanceof BPlusTreeLeafPage) {
        return page;
      }
      if (!(page instanceof BPlusTreeInternalPage)) {
        throw new Error("Unexpected page type");
      }
      pageId = page.lookup(key);
      // TODO: write
      this._bufferPoolManager.unpinPage(page.pageId, false);
    }
  }
}
