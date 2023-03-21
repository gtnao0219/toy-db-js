import { INVALID_PAGE_ID, PAGE_SIZE, Page } from "./page";

// TODO: Implement this class
export type TablePageTuple = {
  value: number;
};

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
  private prevPageId: number;
  private nextPageId: number;
  private tuples: TablePageTuple[];
  constructor(
    pageId: number,
    prevPageId: number = INVALID_PAGE_ID,
    nextPageId: number = INVALID_PAGE_ID,
    tuples: TablePageTuple[] = []
  ) {
    super();
    this.dirty = false;
    this.pinCount = 0;
    this.pageId = pageId;
    this.prevPageId = prevPageId;
    this.nextPageId = nextPageId;
    this.tuples = tuples;
  }
  serialize(): ArrayBuffer {
    const buffer = new ArrayBuffer(PAGE_SIZE);
    const dataView = new DataView(buffer);
    dataView.setInt32(0, this.pageId);
    dataView.setInt32(TABLE_PAGE_HEADER_PAGE_ID_SIZE, this.prevPageId);
    dataView.setInt32(
      TABLE_PAGE_HEADER_PAGE_ID_SIZE + TABLE_PAGE_HEADER_PREV_PAGE_ID_SIZE,
      this.nextPageId
    );
    dataView.setInt16(
      TABLE_PAGE_HEADER_PAGE_ID_SIZE +
        TABLE_PAGE_HEADER_PREV_PAGE_ID_SIZE +
        TABLE_PAGE_HEADER_NEXT_PAGE_ID_SIZE,
      TABLE_PAGE_HEADER_SIZE +
        this.tuples.length * TABLE_PAGE_LINE_POINTERS_SIZE
    );
    let offset = PAGE_SIZE;
    for (let i = 0; i < this.tuples.length; ++i) {
      const tuple = this.tuples[i];
      // TODO: variable size
      const tupleSize = 32;
      offset -= tupleSize;
      dataView.setInt32(offset, tuple.value);
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
  insertTuple(tuple: TablePageTuple): void {
    // TODO: size check
    this.tuples.push(tuple);
  }
}

export function deserializeTablePage(buffer: ArrayBuffer): TablePage {
  const dataView = new DataView(buffer);
  const pageId = dataView.getInt32(0);
  const prevPageId = dataView.getInt32(TABLE_PAGE_HEADER_PAGE_ID_SIZE);
  const nextPageId = dataView.getInt32(
    TABLE_PAGE_HEADER_PAGE_ID_SIZE + TABLE_PAGE_HEADER_PREV_PAGE_ID_SIZE
  );
  const lowerOffset = dataView.getInt16(
    TABLE_PAGE_HEADER_PAGE_ID_SIZE +
      TABLE_PAGE_HEADER_PREV_PAGE_ID_SIZE +
      TABLE_PAGE_HEADER_NEXT_PAGE_ID_SIZE
  );
  const upperOffset = dataView.getInt16(
    TABLE_PAGE_HEADER_PAGE_ID_SIZE +
      TABLE_PAGE_HEADER_PREV_PAGE_ID_SIZE +
      TABLE_PAGE_HEADER_NEXT_PAGE_ID_SIZE +
      TABLE_PAGE_HEADER_LOWER_OFFSET_SIZE
  );
  const linePointerCount =
    (lowerOffset - TABLE_PAGE_HEADER_SIZE) / TABLE_PAGE_LINE_POINTERS_SIZE;
  const tuples: TablePageTuple[] = [];
  for (let i = 0; i < linePointerCount; i++) {
    const offset = dataView.getInt16(
      TABLE_PAGE_HEADER_SIZE + i * TABLE_PAGE_LINE_POINTERS_SIZE
    );
    const size = dataView.getInt16(
      TABLE_PAGE_HEADER_SIZE +
        i * TABLE_PAGE_LINE_POINTERS_SIZE +
        TABLE_PAGE_LINE_POINTER_OFFSET_SIZE
    );
    const value = dataView.getInt32(offset);
    tuples.push({ value });
  }
  return new TablePage(pageId, prevPageId, nextPageId, tuples);
}
