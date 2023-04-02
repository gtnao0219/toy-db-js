import { Schema } from "../../catalog/schema";
import { RID } from "../../common/RID";
import { Transaction } from "../../concurrency/transaction";
import { Tuple, deserializeTuple } from "../table/tuple";
import { INVALID_PAGE_ID, PAGE_SIZE, Page, PageDeserializer } from "./page";

export const TABLE_PAGE_HEADER_PAGE_ID_SIZE = 4;
export const TABLE_PAGE_HEADER_NEXT_PAGE_ID_SIZE = 4;
export const TABLE_PAGE_HEADER_LOWER_OFFSET_SIZE = 2;
export const TABLE_PAGE_HEADER_UPPER_OFFSET_SIZE = 2;
export const TABLE_PAGE_HEADER_SIZE =
  TABLE_PAGE_HEADER_PAGE_ID_SIZE +
  TABLE_PAGE_HEADER_NEXT_PAGE_ID_SIZE +
  TABLE_PAGE_HEADER_LOWER_OFFSET_SIZE +
  TABLE_PAGE_HEADER_UPPER_OFFSET_SIZE;
export const TABLE_PAGE_LINE_POINTER_OFFSET_SIZE = 2;
export const TABLE_PAGE_LINE_POINTER_SIZE_SIZE = 2;
export const TABLE_PAGE_LINE_POINTERS_SIZE =
  TABLE_PAGE_LINE_POINTER_OFFSET_SIZE + TABLE_PAGE_LINE_POINTER_SIZE_SIZE;

export class TablePage extends Page {
  constructor(
    protected _pageId: number = INVALID_PAGE_ID,
    protected _isDirty: boolean = false,
    protected _pinCount: number = 0,
    private _nextPageId: number = INVALID_PAGE_ID,
    private _tuples: Array<Tuple | null> = [],
    private _lowerOffset: number = TABLE_PAGE_HEADER_SIZE,
    private _upperOffset: number = PAGE_SIZE,

    private _deleteMarkedSet: Set<RID> = new Set()
  ) {
    super(_pageId, _isDirty, _pinCount);
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
    dataView.setInt32(TABLE_PAGE_HEADER_PAGE_ID_SIZE, this._nextPageId);
    dataView.setInt16(
      TABLE_PAGE_HEADER_PAGE_ID_SIZE + TABLE_PAGE_HEADER_NEXT_PAGE_ID_SIZE,
      TABLE_PAGE_HEADER_SIZE +
        this._tuples.length * TABLE_PAGE_LINE_POINTERS_SIZE
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
      dataView.setInt16(
        TABLE_PAGE_HEADER_SIZE + i * TABLE_PAGE_LINE_POINTERS_SIZE,
        offset
      );
      dataView.setInt16(
        TABLE_PAGE_HEADER_SIZE +
          i * TABLE_PAGE_LINE_POINTERS_SIZE +
          TABLE_PAGE_LINE_POINTER_OFFSET_SIZE,
        tupleSize
      );
    }
    dataView.setInt16(
      TABLE_PAGE_HEADER_PAGE_ID_SIZE +
        TABLE_PAGE_HEADER_NEXT_PAGE_ID_SIZE +
        TABLE_PAGE_HEADER_UPPER_OFFSET_SIZE,
      offset
    );
    return buffer;
  }
  insertTuple(tuple: Tuple, transaction: Transaction): RID | null {
    const size = tuple.serialize().byteLength;
    if (
      PAGE_SIZE - TABLE_PAGE_HEADER_SIZE - TABLE_PAGE_LINE_POINTERS_SIZE <
      size
    ) {
      throw new Error("Tuple is too large");
    }
    if (this.freeSpaceSize() < TABLE_PAGE_LINE_POINTERS_SIZE + size) {
      return null;
    }
    const nextRID = { pageId: this.pageId, slotId: this._tuples.length };
    this._tuples.push(tuple);
    this._lowerOffset += TABLE_PAGE_LINE_POINTERS_SIZE;
    this._upperOffset -= size;
    return nextRID;
  }
  markDelete(rid: RID, transaction: Transaction): void {
    this._deleteMarkedSet.add(rid);
  }
  applyDelete(rid: RID, transaction: Transaction): void {
    const oldTuple = this._tuples[rid.slotId];
    if (oldTuple == null) {
      throw new Error("Tuple is already deleted");
    }
    const oldSize = oldTuple.serialize().byteLength;
    this._tuples[rid.slotId] = null;
    this._upperOffset += oldSize;
  }
  rollbackDelete(rid: RID, transaction: Transaction): void {
    this._deleteMarkedSet.delete(rid);
  }
  updateTuple(rid: RID, newTuple: Tuple, transaction: Transaction): boolean {
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
  private freeSpaceSize(): number {
    return this._upperOffset - this._lowerOffset;
  }
}

export class TablePageDeserializer implements PageDeserializer {
  constructor(private _schema: Schema) {}
  deserialize(buffer: ArrayBuffer): TablePage {
    const dataView = new DataView(buffer);
    const pageId = dataView.getInt32(0);
    const nextPageId = dataView.getInt32(TABLE_PAGE_HEADER_PAGE_ID_SIZE);
    const lowerOffset = dataView.getInt16(
      TABLE_PAGE_HEADER_PAGE_ID_SIZE + TABLE_PAGE_HEADER_NEXT_PAGE_ID_SIZE
    );
    const upperOffset = dataView.getInt16(
      TABLE_PAGE_HEADER_PAGE_ID_SIZE +
        TABLE_PAGE_HEADER_NEXT_PAGE_ID_SIZE +
        TABLE_PAGE_HEADER_LOWER_OFFSET_SIZE
    );
    const linePointerCount =
      (lowerOffset - TABLE_PAGE_HEADER_SIZE) / TABLE_PAGE_LINE_POINTERS_SIZE;
    const tuples: Array<Tuple | null> = [];
    for (let i = 0; i < linePointerCount; i++) {
      const offset = dataView.getInt16(
        TABLE_PAGE_HEADER_SIZE + i * TABLE_PAGE_LINE_POINTERS_SIZE
      );
      const size = dataView.getInt16(
        TABLE_PAGE_HEADER_SIZE +
          i * TABLE_PAGE_LINE_POINTERS_SIZE +
          TABLE_PAGE_LINE_POINTER_OFFSET_SIZE
      );
      if (size === 0) {
        tuples.push(null);
        continue;
      }
      const tupleBuffer = buffer.slice(offset, offset + size);
      tuples.push(deserializeTuple(tupleBuffer, this._schema));
    }
    return new TablePage(
      pageId,
      false,
      0,
      nextPageId,
      tuples,
      lowerOffset,
      upperOffset
    );
  }
}
