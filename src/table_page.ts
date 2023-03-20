import { PAGE_SIZE } from "./page";

export const TABLE_PAGE_SIZE = PAGE_SIZE;
export const TABLE_PAGE_HEADER_NEXT_BLOCK_NUMBER_SIZE = 4;
export const TABLE_PAGE_LOWER_OFFSET_SIZE = 2;
export const TABLE_PAGE_UPPER_OFFSET_SIZE = 2;
export const TABLE_PAGE_HEADER_SIZE =
  TABLE_PAGE_HEADER_NEXT_BLOCK_NUMBER_SIZE +
  TABLE_PAGE_LOWER_OFFSET_SIZE +
  TABLE_PAGE_UPPER_OFFSET_SIZE;
export const TABLE_PAGE_LINE_POINTER_OFFSET_SIZE = 2;
export const TABLE_PAGE_LINE_POINTER_SIZE_SIZE = 2;
export const TABLE_PAGE_LINE_POINTERS_SIZE =
  TABLE_PAGE_LINE_POINTER_OFFSET_SIZE + TABLE_PAGE_LINE_POINTER_SIZE_SIZE;
export type TablePage = {
  header: TablePageHeader;
  linePointers: TablePageLinePointer[];
  tuples: TablePageTuple[];
};
export type TablePageHeader = {
  nextBlockNumber: number;
  lowerOffset: number;
  upperOffset: number;
};
export type TablePageLinePointer = {
  offset: number;
  size: number;
};
export type TablePageTuple = {
  value: number;
};

export function createTablePage(): TablePage {
  return {
    header: {
      nextBlockNumber: -1,
      lowerOffset: TABLE_PAGE_HEADER_SIZE,
      upperOffset: TABLE_PAGE_SIZE,
    },
    linePointers: [],
    tuples: [],
  };
}
export function serializeTablePage(tablePage: TablePage): ArrayBuffer {
  const buffer = new ArrayBuffer(TABLE_PAGE_SIZE);
  const dataView = new DataView(buffer);
  dataView.setInt32(0, tablePage.header.nextBlockNumber);
  dataView.setInt16(
    TABLE_PAGE_HEADER_NEXT_BLOCK_NUMBER_SIZE,
    tablePage.header.lowerOffset
  );
  dataView.setInt16(
    TABLE_PAGE_HEADER_NEXT_BLOCK_NUMBER_SIZE + TABLE_PAGE_LOWER_OFFSET_SIZE,
    tablePage.header.upperOffset
  );
  for (let i = 0; i < tablePage.linePointers.length; i++) {
    const linePointer = tablePage.linePointers[i];
    dataView.setInt16(
      TABLE_PAGE_HEADER_SIZE + i * TABLE_PAGE_LINE_POINTERS_SIZE,
      linePointer.offset
    );
    dataView.setInt16(
      TABLE_PAGE_HEADER_SIZE +
        i * TABLE_PAGE_LINE_POINTERS_SIZE +
        TABLE_PAGE_LINE_POINTER_OFFSET_SIZE,
      linePointer.size
    );

    const tuple = tablePage.tuples[i];
    // TODO: variable size
    dataView.setInt32(linePointer.offset, tuple.value);
  }
  return buffer;
}
export function deserializeTablePage(buffer: ArrayBuffer): TablePage {
  const dataView = new DataView(buffer);
  const tablePage = createTablePage();
  tablePage.header.nextBlockNumber = dataView.getInt32(0);
  tablePage.header.lowerOffset = dataView.getInt16(
    TABLE_PAGE_HEADER_NEXT_BLOCK_NUMBER_SIZE
  );
  tablePage.header.upperOffset = dataView.getInt16(
    TABLE_PAGE_HEADER_NEXT_BLOCK_NUMBER_SIZE + TABLE_PAGE_LOWER_OFFSET_SIZE
  );
  const linePointerCount =
    (tablePage.header.lowerOffset - TABLE_PAGE_HEADER_SIZE) /
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
    tablePage.linePointers.push({ offset, size });

    const value = dataView.getInt32(offset);
    tablePage.tuples.push({ value });
  }
  return tablePage;
}

export function insertTuple(tablePage: TablePage, tuple: TablePageTuple): void {
  const linePointer = {
    // TODO: variable size
    offset: tablePage.header.upperOffset - 32,
    size: 32,
  };
  tablePage.linePointers.push(linePointer);
  tablePage.tuples.push(tuple);
  tablePage.header.lowerOffset += TABLE_PAGE_LINE_POINTERS_SIZE;
  tablePage.header.upperOffset -= 32;
}
