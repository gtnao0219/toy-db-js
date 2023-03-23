import { INVALID_PAGE_ID, PAGE_SIZE, Page } from "./page";

export const HEADER_PAGE_HEADER_PAGE_ID_SIZE = 4;
export const HEADER_PAGE_HEADER_PREV_PAGE_ID_SIZE = 4;
export const HEADER_PAGE_HEADER_NEXT_PAGE_ID_SIZE = 4;
export const HEADER_PAGE_HEADER_ENTRY_COUNT_SIZE = 4;
export const HEADER_PAGE_HEADER_SIZE =
  HEADER_PAGE_HEADER_PAGE_ID_SIZE +
  HEADER_PAGE_HEADER_PREV_PAGE_ID_SIZE +
  HEADER_PAGE_HEADER_NEXT_PAGE_ID_SIZE +
  HEADER_PAGE_HEADER_ENTRY_COUNT_SIZE;
export const HEADER_PAGE_ENTRY_NAME_SIZE = 32;
export const HEADER_PAGE_ENTRY_FIRST_PAGE_ID_SIZE = 4;
export const HEADER_PAGE_ENTRY_SIZE =
  HEADER_PAGE_ENTRY_NAME_SIZE + HEADER_PAGE_ENTRY_FIRST_PAGE_ID_SIZE;

export type HeaderPageEntry = {
  name: string;
  firstPageId: number;
};

export class HeaderPage extends Page {
  constructor(
    _buffer: ArrayBuffer,
    protected _isDirty: boolean = false,
    protected _pinCount: number = 0,
    protected _pageId: number = INVALID_PAGE_ID,
    private _prevPageId: number = INVALID_PAGE_ID,
    private _nextPageId: number = INVALID_PAGE_ID,
    private _entries: HeaderPageEntry[] = []
  ) {
    super(_buffer, _isDirty, _pinCount, _pageId);
  }
  static newEmptyTablePage(
    pageId: number,
    prevPageId: number = INVALID_PAGE_ID
  ): HeaderPage {
    const headerPage = new HeaderPage(
      new ArrayBuffer(PAGE_SIZE),
      false,
      0,
      pageId,
      prevPageId
    );
    headerPage.buffer = headerPage.serialize();
    return headerPage;
  }
  set buffer(_buffer: ArrayBuffer) {
    this._buffer = _buffer;
  }
  init() {
    const dataView = new DataView(this._buffer);
    this._pageId = dataView.getInt32(0);
    this._prevPageId = dataView.getInt32(HEADER_PAGE_HEADER_PAGE_ID_SIZE);
    this._nextPageId = dataView.getInt32(
      HEADER_PAGE_HEADER_PAGE_ID_SIZE + HEADER_PAGE_HEADER_PREV_PAGE_ID_SIZE
    );
    const entryCount = dataView.getInt32(
      HEADER_PAGE_HEADER_PAGE_ID_SIZE +
        HEADER_PAGE_HEADER_PREV_PAGE_ID_SIZE +
        HEADER_PAGE_HEADER_NEXT_PAGE_ID_SIZE
    );
    for (let i = 0; i < entryCount; i++) {
      const nameUint8Array = new Uint8Array(
        this._buffer,
        HEADER_PAGE_HEADER_SIZE + i * HEADER_PAGE_ENTRY_SIZE,
        HEADER_PAGE_ENTRY_NAME_SIZE
      );
      nameUint8Array.slice(0, nameUint8Array.indexOf(0));
      const name = new TextDecoder().decode(nameUint8Array);
      const firstPageId = dataView.getInt32(
        HEADER_PAGE_HEADER_SIZE +
          i * HEADER_PAGE_ENTRY_SIZE +
          HEADER_PAGE_ENTRY_NAME_SIZE
      );
      this._entries.push({
        name,
        firstPageId,
      });
    }
  }
  serialize(): ArrayBuffer {
    const buffer = new ArrayBuffer(PAGE_SIZE);
    const dataView = new DataView(buffer);
    dataView.setInt32(0, this.pageId);
    dataView.setInt32(HEADER_PAGE_HEADER_PAGE_ID_SIZE, this._prevPageId);
    dataView.setInt32(
      HEADER_PAGE_HEADER_PAGE_ID_SIZE + HEADER_PAGE_HEADER_PREV_PAGE_ID_SIZE,
      this._nextPageId
    );
    dataView.setInt32(
      HEADER_PAGE_HEADER_PAGE_ID_SIZE +
        HEADER_PAGE_HEADER_PREV_PAGE_ID_SIZE +
        HEADER_PAGE_HEADER_NEXT_PAGE_ID_SIZE,
      this._entries.length
    );
    for (let i = 0; i < this._entries.length; ++i) {
      const entry = this._entries[i];
      const nameUint8Array = new TextEncoder().encode(entry.name);
      for (let j = 0; j < nameUint8Array.length; ++j) {
        dataView.setUint8(
          HEADER_PAGE_HEADER_SIZE + i * HEADER_PAGE_ENTRY_SIZE + j,
          nameUint8Array[j]
        );
      }
      for (
        let j = nameUint8Array.length;
        j < HEADER_PAGE_ENTRY_NAME_SIZE;
        ++j
      ) {
        dataView.setUint8(
          HEADER_PAGE_HEADER_SIZE + i * HEADER_PAGE_ENTRY_SIZE + j,
          0
        );
      }
      dataView.setInt32(
        HEADER_PAGE_HEADER_SIZE + i * HEADER_PAGE_ENTRY_SIZE + 32,
        entry.firstPageId
      );
    }
    return buffer;
  }
}
