import { BufferPoolManager } from "../../buffer/buffer_pool_manager";
import { Schema } from "../../catalog/schema";
import { RID } from "../../common/RID";
import { Transaction, WriteType } from "../../concurrency/transaction";
import { LogManager } from "../../recovery/log_manager";
import {
  ApplyDeleteLogRecord,
  InsertLogRecord,
  MarkDeleteLogRecord,
  RollbackDeleteLogRecord,
  UpdateLogRecord,
} from "../../recovery/log_record";
import { INVALID_PAGE_ID } from "../page/page";
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
    private _logManager: LogManager,
    private _oid: number,
    private _firstPageId: number,
    private _schema: Schema
  ) {}
  static new(
    _bufferPoolManager: BufferPoolManager,
    _logManager: LogManager,
    _oid: number,
    _firstPageId: number,
    _schema: Schema
  ): TableHeap {
    return new TableHeap(
      _bufferPoolManager,
      _logManager,
      _oid,
      _firstPageId,
      _schema
    );
  }
  static async create(
    _bufferPoolManager: BufferPoolManager,
    _logManager: LogManager,
    _oid: number,
    _schema: Schema
  ): Promise<TableHeap> {
    const page = await _bufferPoolManager.newPage(
      new TablePageGenerator(_oid, _schema)
    );
    _bufferPoolManager.unpinPage(page.pageId, true);
    return new TableHeap(
      _bufferPoolManager,
      _logManager,
      _oid,
      page.pageId,
      _schema
    );
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
          new TablePageGenerator(this._oid, this._schema)
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

        const lsn = await this._logManager.appendLogRecord(
          new InsertLogRecord(
            -1,
            transaction.prevLsn,
            transaction.transactionId,
            rid,
            tuple.serialize()
          )
        );
        transaction.prevLsn = lsn;
        newPage.lsn = lsn;
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

        const lsn = await this._logManager.appendLogRecord(
          new InsertLogRecord(
            -1,
            transaction.prevLsn,
            transaction.transactionId,
            rid,
            tuple.serialize()
          )
        );
        transaction.prevLsn = lsn;
        page.lsn = lsn;
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

    const lsn = await this._logManager.appendLogRecord(
      new MarkDeleteLogRecord(
        -1,
        transaction.prevLsn,
        transaction.transactionId,
        rid,
        oldTuple?.serialize() ?? new Uint8Array(0)
      )
    );
    transaction.prevLsn = lsn;
    page.lsn = lsn;
    this._bufferPoolManager.unpinPage(page.pageId, true);
  }
  async applyDelete(rid: RID, transaction: Transaction): Promise<void> {
    const page = await this._bufferPoolManager.fetchPage(
      rid.pageId,
      new TablePageDeserializer(this._schema)
    );
    if (!(page instanceof TablePage)) {
      throw new Error("invalid page type");
    }
    const oldTuple = page.getTuple(rid);
    page.applyDelete(rid, transaction);

    const lsn = await this._logManager.appendLogRecord(
      new ApplyDeleteLogRecord(
        -1,
        transaction.prevLsn,
        transaction.transactionId,
        rid,
        oldTuple?.serialize() ?? new Uint8Array(0)
      )
    );
    transaction.prevLsn = lsn;
    page.lsn = lsn;
    this._bufferPoolManager.unpinPage(page.pageId, true);
  }
  async rollbackDelete(rid: RID, transaction: Transaction): Promise<void> {
    const page = await this._bufferPoolManager.fetchPage(
      rid.pageId,
      new TablePageDeserializer(this._schema)
    );
    if (!(page instanceof TablePage)) {
      throw new Error("invalid page type");
    }
    const oldTuple = page.getTuple(rid);
    page.rollbackDelete(rid, transaction);

    const lsn = await this._logManager.appendLogRecord(
      new RollbackDeleteLogRecord(
        -1,
        transaction.prevLsn,
        transaction.transactionId,
        rid,
        oldTuple?.serialize() ?? new Uint8Array(0)
      )
    );
    transaction.prevLsn = lsn;
    page.lsn = lsn;
    this._bufferPoolManager.unpinPage(page.pageId, true);
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

    const lsn = await this._logManager.appendLogRecord(
      new UpdateLogRecord(
        -1,
        transaction.prevLsn,
        transaction.transactionId,
        rid,
        oldTuple?.serialize() ?? new ArrayBuffer(0),
        tuple.serialize()
      )
    );
    transaction.prevLsn = lsn;
    page.lsn = lsn;
    this._bufferPoolManager.unpinPage(page.pageId, true);
  }
  async getTuple(rid: RID): Promise<Tuple | null> {
    const page = await this._bufferPoolManager.fetchPage(
      rid.pageId,
      new TablePageDeserializer(this._schema)
    );
    if (!(page instanceof TablePage)) {
      throw new Error("invalid page type");
    }
    const tuple = page.getTuple(rid);
    this._bufferPoolManager.unpinPage(page.pageId, false);
    return tuple;
  }
}
