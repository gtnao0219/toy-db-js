import { BufferPoolManager } from "../../buffer/buffer_pool_manager";
import { Schema } from "../../catalog/schema";
import { INVALID_PAGE_ID, PageType } from "../page/page";
import { TablePage } from "../page/table_page";
import { Tuple } from "./tuple";

export class TableHeap {
  private constructor(
    private _bufferPoolManager: BufferPoolManager,
    private _schema: Schema,
    private _firstPageId: number
  ) {}
  static get(
    _bufferPoolManager: BufferPoolManager,
    _schema: Schema,
    firstPageId: number
  ): TableHeap {
    return new TableHeap(_bufferPoolManager, _schema, firstPageId);
  }
  static create(
    _bufferPoolManager: BufferPoolManager,
    _schema: Schema
  ): TableHeap | null {
    const page = _bufferPoolManager.newPage(PageType.TABLE_PAGE);
    if (page == null || !(page instanceof TablePage)) {
      return null;
    }
    _bufferPoolManager.unpinPage(page.pageId, true);
    return new TableHeap(_bufferPoolManager, _schema, page.pageId);
  }
  // TEMP
  scan(): Tuple[] {
    const tuples: Tuple[] = [];
    let pageId = this._firstPageId;
    while (true) {
      if (pageId == null || pageId == INVALID_PAGE_ID) {
        return tuples;
      }
      const page = this._bufferPoolManager.fetchPage(
        pageId,
        PageType.TABLE_PAGE
      );
      if (page == null || !(page instanceof TablePage)) {
        return tuples;
      }
      page.init(this._schema);
      tuples.push(...page.tuples);
      pageId = page.nextPageId;
      this._bufferPoolManager.unpinPage(pageId, false);
    }
  }
  insertTuple(tuple: Tuple): void {
    let prevPageId = INVALID_PAGE_ID;
    let pageId = this._firstPageId;
    while (true) {
      if (pageId === INVALID_PAGE_ID) {
        const newPage = this._bufferPoolManager.newPage(PageType.TABLE_PAGE);
        if (newPage == null || !(newPage instanceof TablePage)) {
          return;
        }
        newPage.prevPageId = prevPageId;
        const prevPage = this._bufferPoolManager.fetchPage(
          prevPageId,
          PageType.TABLE_PAGE
        );
        if (prevPage == null || !(prevPage instanceof TablePage)) {
          return;
        }
        prevPage.init(this._schema);
        prevPage.nextPageId = newPage.pageId;
        this._bufferPoolManager.unpinPage(prevPageId, true);
        newPage.insertTuple(tuple);
        this._bufferPoolManager.unpinPage(newPage.pageId, true);
        return;
      }

      const page = this._bufferPoolManager.fetchPage(
        pageId,
        PageType.TABLE_PAGE
      );
      if (page == null || !(page instanceof TablePage)) {
        return;
      }
      page.init(this._schema);
      if (page.insertTuple(tuple)) {
        this._bufferPoolManager.unpinPage(pageId, true);
        return;
      }
      prevPageId = pageId;
      pageId = page.nextPageId;
      this._bufferPoolManager.unpinPage(prevPageId, false);
    }
  }
  begin(): void {}
  end(): void {}
}
