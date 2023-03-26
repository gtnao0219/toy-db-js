import { Schema } from "../../catalog/schema";
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
    private _tuples: Tuple[] = [],
    private _lowerOffset: number = TABLE_PAGE_HEADER_SIZE,
    private _upperOffset: number = PAGE_SIZE
  ) {
    super(_pageId, _isDirty, _pinCount);
  }
  get nextPageId(): number {
    return this._nextPageId;
  }
  set nextPageId(pageId: number) {
    this._nextPageId = pageId;
  }
  get tuples(): Tuple[] {
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
    this._tuples.push(tuple);
    this._lowerOffset += TABLE_PAGE_LINE_POINTERS_SIZE;
    this._upperOffset -= size;
    return true;
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
    const tuples: Tuple[] = [];
    for (let i = 0; i < linePointerCount; i++) {
      const offset = dataView.getInt16(
        TABLE_PAGE_HEADER_SIZE + i * TABLE_PAGE_LINE_POINTERS_SIZE
      );
      const size = dataView.getInt16(
        TABLE_PAGE_HEADER_SIZE +
          i * TABLE_PAGE_LINE_POINTERS_SIZE +
          TABLE_PAGE_LINE_POINTER_OFFSET_SIZE
      );
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
