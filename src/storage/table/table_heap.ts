import { BufferPoolManager } from "../../buffer/buffer_pool_manager";
import { Schema } from "../../catalog/schema";
import { RID } from "../../common/RID";
import { Transaction, WriteType } from "../../concurrency/transaction";
import { INVALID_PAGE_ID, PageType } from "../page/page";
import {
  TablePage,
  TablePageDeserializer,
  TablePageGenerator,
} from "../page/table_page";
import { Tuple } from "./tuple";

// TODO: temp
export type TupleWithRID = {
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
  static async create(
    _bufferPoolManager: BufferPoolManager,
    _oid: number,
    _schema: Schema
  ): Promise<TableHeap> {
    const page = await _bufferPoolManager.newPage(new TablePageGenerator());
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
  async scan(): Promise<TupleWithRID[]> {
    const tuples: TupleWithRID[] = [];
    let pageId = this._firstPageId;
    while (true) {
      if (pageId == INVALID_PAGE_ID) {
        return tuples;
      }
      const page = await this._bufferPoolManager.fetchPage(
        pageId,
        new TablePageDeserializer(this._schema)
      );
      if (!(page instanceof TablePage)) {
        throw new Error("invalid page type");
      }
      tuples.push(
        ...page.tuples
          .map((tuple, slotId) => {
            if (
              tuple == null ||
              page.isMarkedDelete({
                pageId,
                slotId,
              })
            ) {
              return null;
            }
            return { tuple, rid: { pageId, slotId } };
          })
          .filter((tuple): tuple is TupleWithRID => tuple != null)
      );
      const prevPageId = pageId;
      pageId = page.nextPageId;
      this._bufferPoolManager.unpinPage(prevPageId, false);
    }
  }
  async insertTuple(tuple: Tuple, transaction: Transaction): Promise<RID> {
    let prevPageId = INVALID_PAGE_ID;
    let pageId = this._firstPageId;
    while (true) {
      if (pageId === INVALID_PAGE_ID) {
        const newPage = await this._bufferPoolManager.newPage(
          new TablePageGenerator()
        );
        if (!(newPage instanceof TablePage)) {
          throw new Error("invalid page type");
        }
        const prevPage = await this._bufferPoolManager.fetchPage(
          prevPageId,
          new TablePageDeserializer(this._schema)
        );
        if (!(prevPage instanceof TablePage)) {
          throw new Error("invalid page type");
        }
        prevPage.nextPageId = newPage.pageId;
        this._bufferPoolManager.unpinPage(prevPageId, true);
        const rid = newPage.insertTuple(tuple, transaction);
        if (rid == null) {
          throw new Error("insert tuple failed");
        }
        transaction.addWriteRecord(WriteType.INSERT, rid, null, this);
        this._bufferPoolManager.unpinPage(newPage.pageId, true);
        return rid;
      }

      const page = await this._bufferPoolManager.fetchPage(
        pageId,
        new TablePageDeserializer(this._schema)
      );
      if (!(page instanceof TablePage)) {
        throw new Error("invalid page type");
      }
      const rid = page.insertTuple(tuple, transaction);
      if (rid !== null) {
        transaction.addWriteRecord(WriteType.INSERT, rid, null, this);
        this._bufferPoolManager.unpinPage(pageId, true);
        return rid;
      }
      prevPageId = pageId;
      pageId = page.nextPageId;
      this._bufferPoolManager.unpinPage(prevPageId, false);
    }
  }
  async markDelete(rid: RID, transaction: Transaction): Promise<void> {
    const page = await this._bufferPoolManager.fetchPage(
      rid.pageId,
      new TablePageDeserializer(this._schema)
    );
    if (!(page instanceof TablePage)) {
      throw new Error("invalid page type");
    }
    const oldTuple = page.getTuple(rid);
    page.markDelete(rid, transaction);
    transaction.addWriteRecord(WriteType.DELETE, rid, oldTuple, this);
  }
  async applyDelete(rid: RID, transaction: Transaction): Promise<void> {
    const page = await this._bufferPoolManager.fetchPage(
      rid.pageId,
      new TablePageDeserializer(this._schema)
    );
    if (!(page instanceof TablePage)) {
      throw new Error("invalid page type");
    }
    page.applyDelete(rid, transaction);
  }
  async rollbackDelete(rid: RID, transaction: Transaction): Promise<void> {
    const page = await this._bufferPoolManager.fetchPage(
      rid.pageId,
      new TablePageDeserializer(this._schema)
    );
    if (!(page instanceof TablePage)) {
      throw new Error("invalid page type");
    }
    page.rollbackDelete(rid, transaction);
  }
  async updateTuple(
    rid: RID,
    tuple: Tuple,
    transaction: Transaction
  ): Promise<void> {
    const page = await this._bufferPoolManager.fetchPage(
      rid.pageId,
      new TablePageDeserializer(this._schema)
    );
    if (!(page instanceof TablePage)) {
      throw new Error("invalid page type");
    }
    const oldTuple = page.getTuple(rid);
    page.updateTuple(rid, tuple, transaction);
    transaction.addWriteRecord(WriteType.UPDATE, rid, oldTuple, this);
  }
}
