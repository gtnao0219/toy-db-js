import { INVALID_PAGE_ID, PAGE_SIZE, Page, PageGenerator } from "./page";

export const HEADER_PAGE_HEADER_PAGE_ID_SIZE = 4;
export const HEADER_PAGE_HEADER_NEXT_PAGE_ID_SIZE = 4;
export const HEADER_PAGE_HEADER_ENTRY_COUNT_SIZE = 4;
export const HEADER_PAGE_HEADER_SIZE =
  HEADER_PAGE_HEADER_PAGE_ID_SIZE +
  HEADER_PAGE_HEADER_NEXT_PAGE_ID_SIZE +
  HEADER_PAGE_HEADER_ENTRY_COUNT_SIZE;
export const HEADER_PAGE_ENTRY_OID_SIZE = 4;
export const HEADER_PAGE_ENTRY_FIRST_PAGE_ID_SIZE = 4;
export const HEADER_PAGE_ENTRY_SIZE =
  HEADER_PAGE_ENTRY_OID_SIZE + HEADER_PAGE_ENTRY_FIRST_PAGE_ID_SIZE;

export type HeaderPageEntry = {
  oid: number;
  firstPageId: number;
};

export class HeaderPage extends Page {
  constructor(
    protected _pageId: number = INVALID_PAGE_ID,
    private _nextPageId: number = INVALID_PAGE_ID,
    private _entries: HeaderPageEntry[] = []
  ) {
    super(_pageId);
  }
  get nextPageId(): number {
    return this._nextPageId;
  }
  set nextPageId(pageId: number) {
    this._nextPageId = pageId;
  }
  get entries(): HeaderPageEntry[] {
    return this._entries;
  }
  serialize(): ArrayBuffer {
    const buffer = new ArrayBuffer(PAGE_SIZE);
    const dataView = new DataView(buffer);
    dataView.setInt32(0, this.pageId);
    dataView.setInt32(HEADER_PAGE_HEADER_PAGE_ID_SIZE, this._nextPageId);
    dataView.setInt32(
      HEADER_PAGE_HEADER_PAGE_ID_SIZE + HEADER_PAGE_HEADER_NEXT_PAGE_ID_SIZE,
      this._entries.length
    );
    for (let i = 0; i < this._entries.length; ++i) {
      const entry = this._entries[i];
      dataView.setInt32(
        HEADER_PAGE_HEADER_SIZE + i * HEADER_PAGE_ENTRY_SIZE,
        entry.oid
      );
      dataView.setInt32(
        HEADER_PAGE_HEADER_SIZE +
          i * HEADER_PAGE_ENTRY_SIZE +
          HEADER_PAGE_ENTRY_OID_SIZE,
        entry.firstPageId
      );
    }
    return buffer;
  }
  firstPageId(oid: number): number | null {
    for (const entry of this._entries) {
      if (entry.oid === oid) {
        return entry.firstPageId;
      }
    }
    return null;
  }
  insert(oid: number, firstPageId: number): boolean {
    if (
      PAGE_SIZE <
      HEADER_PAGE_HEADER_SIZE +
        (this._entries.length + 1) * HEADER_PAGE_ENTRY_SIZE
    ) {
      return false;
    }
    this._entries.push({
      oid,
      firstPageId,
    });
    return true;
  }
}

export class HeaderPageDeserializer {
  deserialize(buffer: ArrayBuffer): HeaderPage {
    const dataView = new DataView(buffer);
    const pageId = dataView.getInt32(0);
    const nextPageId = dataView.getInt32(HEADER_PAGE_HEADER_PAGE_ID_SIZE);
    const entryCount = dataView.getInt32(
      HEADER_PAGE_HEADER_PAGE_ID_SIZE + HEADER_PAGE_HEADER_NEXT_PAGE_ID_SIZE
    );
    const entries: HeaderPageEntry[] = [];
    for (let i = 0; i < entryCount; i++) {
      const oid = dataView.getInt32(
        HEADER_PAGE_HEADER_SIZE + i * HEADER_PAGE_ENTRY_SIZE
      );
      const firstPageId = dataView.getInt32(
        HEADER_PAGE_HEADER_SIZE +
          i * HEADER_PAGE_ENTRY_SIZE +
          HEADER_PAGE_ENTRY_OID_SIZE
      );
      entries.push({
        oid,
        firstPageId,
      });
    }
    return new HeaderPage(pageId, nextPageId, entries);
  }
}
export class HeaderPageGenerator implements PageGenerator {
  generate(pageId: number): HeaderPage {
    return new HeaderPage(pageId);
  }
}
