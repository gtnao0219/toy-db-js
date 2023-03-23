import { Schema } from "../../catalog/schema";
import { RID, Tuple, deserializeTuple } from "../table/tuple";
import { INVALID_PAGE_ID, PAGE_SIZE, Page } from "./page";

export const TABLE_PAGE_HEADER_PAGE_ID_SIZE = 4;
export const TABLE_PAGE_HEADER_PREV_PAGE_ID_SIZE = 4;
export const TABLE_PAGE_HEADER_NEXT_PAGE_ID_SIZE = 4;
export const TABLE_PAGE_HEADER_LOWER_OFFSET_SIZE = 2;
export const TABLE_PAGE_HEADER_UPPER_OFFSET_SIZE = 2;
export const TABLE_PAGE_HEADER_SIZE =
  TABLE_PAGE_HEADER_PAGE_ID_SIZE +
  TABLE_PAGE_HEADER_PREV_PAGE_ID_SIZE +
  TABLE_PAGE_HEADER_NEXT_PAGE_ID_SIZE +
  TABLE_PAGE_HEADER_LOWER_OFFSET_SIZE +
  TABLE_PAGE_HEADER_UPPER_OFFSET_SIZE;
export const TABLE_PAGE_LINE_POINTER_OFFSET_SIZE = 2;
export const TABLE_PAGE_LINE_POINTER_SIZE_SIZE = 2;
export const TABLE_PAGE_LINE_POINTERS_SIZE =
  TABLE_PAGE_LINE_POINTER_OFFSET_SIZE + TABLE_PAGE_LINE_POINTER_SIZE_SIZE;

export class TablePage extends Page {
  constructor(
    _buffer: ArrayBuffer,
    protected _isDirty: boolean = false,
    protected _pinCount: number = 0,
    protected _pageId: number = INVALID_PAGE_ID,
    private _prevPageId: number = INVALID_PAGE_ID,
    private _nextPageId: number = INVALID_PAGE_ID,
    private _tuples: Tuple[] = [],
    private _lowerOffset: number = TABLE_PAGE_HEADER_SIZE,
    private _upperOffset: number = PAGE_SIZE,
    private _init: boolean = false
  ) {
    super(_buffer, _isDirty, _pinCount, _pageId);
  }
  static newEmptyTablePage(
    pageId: number,
    prevPageId: number = INVALID_PAGE_ID
  ): TablePage {
    const tablePage = new TablePage(
      new ArrayBuffer(PAGE_SIZE),
      false,
      0,
      pageId,
      prevPageId
    );
    tablePage.buffer = tablePage.serialize();
    return tablePage;
  }
  get nextPageId(): number {
    return this._nextPageId;
  }
  set prevPageId(pageId: number) {
    this._prevPageId = pageId;
  }
  set nextPageId(pageId: number) {
    this._nextPageId = pageId;
  }
  // TODO: temp
  get tuples(): Tuple[] {
    return this._tuples;
  }
  set buffer(_buffer: ArrayBuffer) {
    this._buffer = _buffer;
  }
  init(schema: Schema) {
    if (this._init) {
      return;
    }
    const dataView = new DataView(this._buffer);
    this._pageId = dataView.getInt32(0);
    this._prevPageId = dataView.getInt32(TABLE_PAGE_HEADER_PAGE_ID_SIZE);
    this._nextPageId = dataView.getInt32(
      TABLE_PAGE_HEADER_PAGE_ID_SIZE + TABLE_PAGE_HEADER_PREV_PAGE_ID_SIZE
    );
    this._lowerOffset = dataView.getInt16(
      TABLE_PAGE_HEADER_PAGE_ID_SIZE +
        TABLE_PAGE_HEADER_PREV_PAGE_ID_SIZE +
        TABLE_PAGE_HEADER_NEXT_PAGE_ID_SIZE
    );
    this._upperOffset = dataView.getInt16(
      TABLE_PAGE_HEADER_PAGE_ID_SIZE +
        TABLE_PAGE_HEADER_PREV_PAGE_ID_SIZE +
        TABLE_PAGE_HEADER_NEXT_PAGE_ID_SIZE +
        TABLE_PAGE_HEADER_LOWER_OFFSET_SIZE
    );
    const linePointerCount =
      (this._lowerOffset - TABLE_PAGE_HEADER_SIZE) /
      TABLE_PAGE_LINE_POINTERS_SIZE;
    for (let i = 0; i < linePointerCount; i++) {
      const offset = dataView.getInt16(
        TABLE_PAGE_HEADER_SIZE + i * TABLE_PAGE_LINE_POINTERS_SIZE
      );
      const size = dataView.getInt16(
        TABLE_PAGE_HEADER_SIZE +
          i * TABLE_PAGE_LINE_POINTERS_SIZE +
          TABLE_PAGE_LINE_POINTER_OFFSET_SIZE
      );
      const tupleBuffer = this._buffer.slice(offset, offset + size);
      this._tuples.push(
        deserializeTuple(
          tupleBuffer,
          { pageId: this.pageId, slotId: i },
          schema
        )
      );
    }
    this._init = true;
  }
  serialize(): ArrayBuffer {
    const buffer = new ArrayBuffer(PAGE_SIZE);
    const dataView = new DataView(buffer);
    dataView.setInt32(0, this.pageId);
    dataView.setInt32(TABLE_PAGE_HEADER_PAGE_ID_SIZE, this._prevPageId);
    dataView.setInt32(
      TABLE_PAGE_HEADER_PAGE_ID_SIZE + TABLE_PAGE_HEADER_PREV_PAGE_ID_SIZE,
      this._nextPageId
    );
    dataView.setInt16(
      TABLE_PAGE_HEADER_PAGE_ID_SIZE +
        TABLE_PAGE_HEADER_PREV_PAGE_ID_SIZE +
        TABLE_PAGE_HEADER_NEXT_PAGE_ID_SIZE,
      TABLE_PAGE_HEADER_SIZE +
        this._tuples.length * TABLE_PAGE_LINE_POINTERS_SIZE
    );
    let offset = PAGE_SIZE;
    for (let i = 0; i < this._tuples.length; ++i) {
      const tuple = this._tuples[i];
      const tupleBuffer = tuple.serialize();
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
        TABLE_PAGE_HEADER_PREV_PAGE_ID_SIZE +
        TABLE_PAGE_HEADER_NEXT_PAGE_ID_SIZE +
        TABLE_PAGE_HEADER_UPPER_OFFSET_SIZE,
      offset
    );
    return buffer;
  }
  insertTuple(tuple: Tuple): boolean {
    const size = tuple.serialize().byteLength;
    if (
      this._upperOffset - this._lowerOffset - TABLE_PAGE_LINE_POINTERS_SIZE <
      size
    ) {
      return false;
    }
    tuple.rid = this.nextRID();
    this._tuples.push(tuple);
    this._lowerOffset += TABLE_PAGE_LINE_POINTERS_SIZE;
    this._upperOffset -= size;
    // this._isDirty = true;
    return true;
  }
  nextRID(): RID {
    return { pageId: this._pageId, slotId: this._tuples.length };
  }
}
