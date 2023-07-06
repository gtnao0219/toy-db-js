import { Catalog } from "../../catalog/catalog";
import { Schema } from "../../catalog/schema";
import { RID } from "../../common/RID";
import { Transaction } from "../../concurrency/transaction";
import { Tuple } from "../table/tuple";
import {
  INVALID_PAGE_ID,
  PAGE_SIZE,
  Page,
  PageDeserializer,
  PageGenerator,
} from "./page";

const HEADER_PAGE_ID_SIZE = 4;
const HEADER_OID_SIZE = 4;
const HEADER_LSN_SIZE = 4;
const HEADER_NEXT_PAGE_ID_SIZE = 4;
const HEADER_LOWER_OFFSET_SIZE = 2;
const HEADER_UPPER_OFFSET_SIZE = 2;
const HEADER_SIZE =
  HEADER_PAGE_ID_SIZE +
  HEADER_OID_SIZE +
  HEADER_LSN_SIZE +
  HEADER_NEXT_PAGE_ID_SIZE +
  HEADER_LOWER_OFFSET_SIZE +
  HEADER_UPPER_OFFSET_SIZE;
const LINE_POINTER_OFFSET_SIZE = 2;
const LINE_POINTER_SIZE_SIZE = 2;
const LINE_POINTERS_SIZE = LINE_POINTER_OFFSET_SIZE + LINE_POINTER_SIZE_SIZE;

export class TablePage extends Page {
  constructor(
    protected _pageId: number = INVALID_PAGE_ID,
    private _oid: number = -1,
    private _schema: Schema,
    private _lsn: number = -1,
    private _nextPageId: number = INVALID_PAGE_ID,
    private _tuples: Array<Tuple | null> = [],
    private _lowerOffset: number = HEADER_SIZE,
    private _upperOffset: number = PAGE_SIZE,

    private _deleteMarkedSet: Set<RID> = new Set()
  ) {
    super(_pageId);
  }
  get schema(): Schema {
    return this._schema;
  }
  get oid(): number {
    return this._oid;
  }
  get lsn(): number {
    return this._lsn;
  }
  set lsn(lsn: number) {
    this._lsn = lsn;
  }
  get nextPageId(): number {
    return this._nextPageId;
  }
  set nextPageId(pageId: number) {
    this._nextPageId = pageId;
  }
  get tuples(): Array<Tuple | null> {
    return this._tuples;
  }
  serialize(): ArrayBuffer {
    const buffer = new ArrayBuffer(PAGE_SIZE);
    const dataView = new DataView(buffer);
    dataView.setInt32(0, this.pageId);
    dataView.setInt32(HEADER_PAGE_ID_SIZE, this._oid);
    dataView.setInt32(HEADER_PAGE_ID_SIZE + HEADER_OID_SIZE, this._lsn);
    dataView.setInt32(
      HEADER_PAGE_ID_SIZE + HEADER_OID_SIZE + HEADER_LSN_SIZE,
      this._nextPageId
    );
    dataView.setInt16(
      HEADER_PAGE_ID_SIZE +
        HEADER_OID_SIZE +
        HEADER_LSN_SIZE +
        HEADER_NEXT_PAGE_ID_SIZE,
      HEADER_SIZE + this._tuples.length * LINE_POINTERS_SIZE
    );
    let offset = PAGE_SIZE;
    for (let i = 0; i < this._tuples.length; ++i) {
      const tuple = this._tuples[i];
      const tupleBuffer =
        tuple == null ? new ArrayBuffer(0) : tuple.serialize();
      const tupleDataView = new DataView(tupleBuffer);
      const tupleSize = tupleBuffer.byteLength;
      offset -= tupleSize;
      for (let j = 0; j < tupleSize; ++j) {
        dataView.setInt8(offset + j, tupleDataView.getInt8(j));
      }
      dataView.setInt16(HEADER_SIZE + i * LINE_POINTERS_SIZE, offset);
      dataView.setInt16(
        HEADER_SIZE + i * LINE_POINTERS_SIZE + LINE_POINTER_OFFSET_SIZE,
        tupleSize
      );
    }
    dataView.setInt16(
      HEADER_PAGE_ID_SIZE +
        HEADER_OID_SIZE +
        HEADER_LSN_SIZE +
        HEADER_NEXT_PAGE_ID_SIZE +
        HEADER_UPPER_OFFSET_SIZE,
      offset
    );
    return buffer;
  }
  insertTuple(tuple: Tuple, transaction: Transaction | null): RID | null {
    const size = tuple.serialize().byteLength;
    if (PAGE_SIZE - HEADER_SIZE - LINE_POINTERS_SIZE < size) {
      throw new Error("Tuple is too large");
    }
    if (this.freeSpaceSize() < LINE_POINTERS_SIZE + size) {
      return null;
    }
    const nextRID = { pageId: this.pageId, slotId: this._tuples.length };
    this._tuples.push(tuple);
    this._lowerOffset += LINE_POINTERS_SIZE;
    this._upperOffset -= size;
    return nextRID;
  }
  markDelete(rid: RID, transaction: Transaction | null): void {
    this._deleteMarkedSet.add(rid);
  }
  applyDelete(rid: RID, transaction: Transaction | null): void {
    const oldTuple = this._tuples[rid.slotId];
    if (oldTuple == null) {
      throw new Error("Tuple is already deleted");
    }
    const oldSize = oldTuple.serialize().byteLength;
    this._tuples[rid.slotId] = null;
    this._upperOffset += oldSize;
  }
  rollbackDelete(rid: RID, transaction: Transaction | null): void {
    this._deleteMarkedSet.delete(rid);
  }
  updateTuple(
    rid: RID,
    newTuple: Tuple,
    transaction: Transaction | null
  ): boolean {
    const oldTuple = this._tuples[rid.slotId];
    if (oldTuple == null) {
      throw new Error("Tuple not found");
    }
    const oldSize = oldTuple.serialize().byteLength;
    const newSize = newTuple.serialize().byteLength;
    if (this.freeSpaceSize() + oldSize < newSize) {
      return false;
    }
    this._tuples[rid.slotId] = newTuple;
    this._upperOffset += oldSize - newSize;
    return true;
  }
  getTuple(rid: RID): Tuple | null {
    return this._tuples[rid.slotId];
  }
  isMarkedDelete(rid: RID): boolean {
    return this._deleteMarkedSet.has(rid);
  }
  toJSON() {
    return {
      pageId: this.pageId,
      oid: this._oid,
      lsn: this._lsn,
      nextPageId: this._nextPageId,
      tuples: this._tuples,
      lowerOffset: this._lowerOffset,
      upperOffset: this._upperOffset,
      deleteMarkedSet: this._deleteMarkedSet,
    };
  }
  private freeSpaceSize(): number {
    return this._upperOffset - this._lowerOffset;
  }
}

export class TablePageDeserializerUsingCatalog {
  constructor(private _catalog: Catalog) {}
  async deserialize(buffer: ArrayBuffer): Promise<TablePage> {
    const dataView = new DataView(buffer);
    const oid = dataView.getInt32(HEADER_PAGE_ID_SIZE);
    const schema = await this._catalog.getSchemaByOid(oid);
    const tablePageDeserializer = new TablePageDeserializer(schema);
    return tablePageDeserializer.deserialize(buffer);
  }
}

export class TablePageDeserializer implements PageDeserializer {
  constructor(private _schema: Schema) {}
  async deserialize(buffer: ArrayBuffer): Promise<TablePage> {
    const dataView = new DataView(buffer);
    const pageId = dataView.getInt32(0);
    const oid = dataView.getInt32(HEADER_PAGE_ID_SIZE);
    const lsn = dataView.getInt32(HEADER_PAGE_ID_SIZE + HEADER_OID_SIZE);
    const nextPageId = dataView.getInt32(
      HEADER_PAGE_ID_SIZE + HEADER_OID_SIZE + HEADER_LSN_SIZE
    );
    const lowerOffset = dataView.getInt16(
      HEADER_PAGE_ID_SIZE +
        HEADER_OID_SIZE +
        HEADER_LSN_SIZE +
        HEADER_NEXT_PAGE_ID_SIZE
    );
    const upperOffset = dataView.getInt16(
      HEADER_PAGE_ID_SIZE +
        HEADER_OID_SIZE +
        HEADER_LSN_SIZE +
        HEADER_NEXT_PAGE_ID_SIZE +
        HEADER_LOWER_OFFSET_SIZE
    );
    const linePointerCount = (lowerOffset - HEADER_SIZE) / LINE_POINTERS_SIZE;
    const tuples: Array<Tuple | null> = [];
    for (let i = 0; i < linePointerCount; i++) {
      const offset = dataView.getInt16(HEADER_SIZE + i * LINE_POINTERS_SIZE);
      const size = dataView.getInt16(
        HEADER_SIZE + i * LINE_POINTERS_SIZE + LINE_POINTER_OFFSET_SIZE
      );
      if (size === 0) {
        tuples.push(null);
        continue;
      }
      const tupleBuffer = buffer.slice(offset, offset + size);
      tuples.push(Tuple.deserialize(tupleBuffer, this._schema));
    }
    return new TablePage(
      pageId,
      oid,
      this._schema,
      lsn,
      nextPageId,
      tuples,
      lowerOffset,
      upperOffset
    );
  }
}

export class TablePageGenerator implements PageGenerator {
  constructor(private _oid: number, private _schema: Schema) {}
  generate(pageId: number): TablePage {
    return new TablePage(pageId, this._oid, this._schema);
  }
}
