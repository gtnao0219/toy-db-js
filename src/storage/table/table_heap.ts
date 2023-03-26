import { BufferPoolManager } from "../../buffer/buffer_pool_manager";
import { Schema } from "../../catalog/schema";
import { RID } from "../../common/RID";
import { INVALID_PAGE_ID, PageType } from "../page/page";
import { TablePage, TablePageDeserializer } from "../page/table_page";
import { Tuple } from "./tuple";

// TODO: temp
type TupleWithRID = {
  tuple: Tuple;
  rid: RID;
};

export class TableHeap {
  private constructor(
    private _bufferPoolManager: BufferPoolManager,
    private _oid: number,
    private _firstPageId: number,
    private _schema: Schema
  ) {}
  static new(
    _bufferPoolManager: BufferPoolManager,
    _oid: number,
    _firstPageId: number,
    _schema: Schema
  ): TableHeap {
    return new TableHeap(_bufferPoolManager, _oid, _firstPageId, _schema);
  }
  static create(
    _bufferPoolManager: BufferPoolManager,
    _oid: number,
    _schema: Schema
  ): TableHeap {
    const page = _bufferPoolManager.newPage(PageType.TABLE_PAGE);
    _bufferPoolManager.unpinPage(page.pageId, true);
    return new TableHeap(_bufferPoolManager, _oid, page.pageId, _schema);
  }
  get firstPageId(): number {
    return this._firstPageId;
  }
  get schema(): Schema {
    return this._schema;
  }
  // TODO: implement iterator
  scan(): TupleWithRID[] {
    const tuples: TupleWithRID[] = [];
    let pageId = this._firstPageId;
    while (true) {
      if (pageId == INVALID_PAGE_ID) {
        return tuples;
      }
      const page = this._bufferPoolManager.fetchPage(
        pageId,
        new TablePageDeserializer(this._schema)
      );
      if (!(page instanceof TablePage)) {
        throw new Error("invalid page type");
      }
      tuples.push(
        ...page.tuples.map((tuple, i) => {
          return { tuple, rid: { pageId, slotId: i } };
        })
      );
      const prevPageId = pageId;
      pageId = page.nextPageId;
      this._bufferPoolManager.unpinPage(prevPageId, false);
    }
  }
  insertTuple(tuple: Tuple): void {
    let prevPageId = INVALID_PAGE_ID;
    let pageId = this._firstPageId;
    while (true) {
      if (pageId === INVALID_PAGE_ID) {
        const newPage = this._bufferPoolManager.newPage(PageType.TABLE_PAGE);
        if (!(newPage instanceof TablePage)) {
          throw new Error("invalid page type");
        }
        const prevPage = this._bufferPoolManager.fetchPage(
          prevPageId,
          new TablePageDeserializer(this._schema)
        );
        if (!(prevPage instanceof TablePage)) {
          throw new Error("invalid page type");
        }
        prevPage.nextPageId = newPage.pageId;
        this._bufferPoolManager.unpinPage(prevPageId, true);
        newPage.insertTuple(tuple);
        this._bufferPoolManager.unpinPage(newPage.pageId, true);
        return;
      }

      const page = this._bufferPoolManager.fetchPage(
        pageId,
        new TablePageDeserializer(this._schema)
      );
      if (!(page instanceof TablePage)) {
        throw new Error("invalid page type");
      }
      if (page.insertTuple(tuple)) {
        this._bufferPoolManager.unpinPage(pageId, true);
        return;
      }
      prevPageId = pageId;
      pageId = page.nextPageId;
      this._bufferPoolManager.unpinPage(prevPageId, false);
    }
  }
  deleteTuple(rid: RID): void {
    const page = this._bufferPoolManager.fetchPage(
      rid.pageId,
      new TablePageDeserializer(this._schema)
    );
    if (!(page instanceof TablePage)) {
      throw new Error("invalid page type");
    }
    page.deleteTuple(rid);
  }
  updateTuple(rid: RID, tuple: Tuple): void {
    const page = this._bufferPoolManager.fetchPage(
      rid.pageId,
      new TablePageDeserializer(this._schema)
    );
    if (!(page instanceof TablePage)) {
      throw new Error("invalid page type");
    }
    page.updateTuple(rid, tuple);
  }
}
