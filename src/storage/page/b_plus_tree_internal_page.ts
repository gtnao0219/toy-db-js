import { BufferPoolManager } from "../../buffer/buffer_pool_manager";
import {
  INVALID_PAGE_ID,
  PAGE_SIZE,
  Page,
  PageDeserializer,
  PageGenerator,
} from "./page";

const HEADER_PAGE_ID_SIZE = 4;
const HEADER_TYPE_SIZE = 4;
const HEADER_PARENT_PAGE_ID_SIZE = 4;
const HEADER_CURRENT_SIZE_SIZE = 4;
const HEADER_SIZE =
  HEADER_PAGE_ID_SIZE +
  HEADER_TYPE_SIZE +
  HEADER_PARENT_PAGE_ID_SIZE +
  HEADER_CURRENT_SIZE_SIZE;
const ENTRY_KEY_SIZE = 4;
const ENTRY_VALUE_SIZE = 4;
const ENTRY_SIZE = ENTRY_KEY_SIZE + ENTRY_VALUE_SIZE;
const MAX_ENTRY_COUNT = Math.floor((PAGE_SIZE - HEADER_SIZE) / ENTRY_SIZE);

type BPlusTreeInternalPageEntry = {
  key: number | null; // only support number for now
  value: number;
};

export const B_PLUS_TREE_INTERNAL_PAGE_TYPE = 0;

export class BPlusTreeInternalPage extends Page {
  constructor(
    protected _pageId: number = INVALID_PAGE_ID,
    private _parentPageId: number = INVALID_PAGE_ID,
    private _entries: BPlusTreeInternalPageEntry[] = []
  ) {
    super(_pageId);
  }
  get parentPageId(): number {
    return this._parentPageId;
  }
  set parentPageId(pageId: number) {
    this._parentPageId = pageId;
  }
  get entries(): BPlusTreeInternalPageEntry[] {
    return this._entries;
  }
  serialize(): ArrayBuffer {
    const buffer = new ArrayBuffer(PAGE_SIZE);
    const dataView = new DataView(buffer);
    dataView.setInt32(0, this.pageId);
    dataView.setInt32(HEADER_PAGE_ID_SIZE, B_PLUS_TREE_INTERNAL_PAGE_TYPE);
    dataView.setInt32(
      HEADER_PAGE_ID_SIZE + HEADER_TYPE_SIZE,
      this._parentPageId
    );
    dataView.setInt32(
      HEADER_PAGE_ID_SIZE + HEADER_TYPE_SIZE + HEADER_PARENT_PAGE_ID_SIZE,
      this._entries.length
    );
    for (let i = 0; i < this._entries.length; ++i) {
      const entry = this._entries[i];
      dataView.setInt32(
        HEADER_SIZE + i * ENTRY_SIZE,
        entry.key ?? INVALID_PAGE_ID // dummy
      );
      dataView.setInt32(
        HEADER_SIZE + i * ENTRY_SIZE + ENTRY_KEY_SIZE,
        entry.value
      );
    }
    return buffer;
  }
  isFull(): boolean {
    return this._entries.length >= MAX_ENTRY_COUNT;
  }
  keyAt(index: number): number | null {
    return this._entries[index].key;
  }
  setKeyAt(index: number, key: number | null) {
    this._entries[index].key = key;
  }
  valueAt(index: number): number {
    return this._entries[index].value;
  }
  setValueAt(index: number, value: number) {
    this._entries[index].value = value;
  }
  valueIndex(value: number): number {
    const index = this._entries.findIndex((entry) => entry.value === value);
    if (index === -1) {
      throw new Error(`value ${value} not found`);
    }
    return index;
  }
  lookup(key: number): number {
    let ng = 0;
    let ok = this._entries.length;
    while (Math.abs(ok - ng) > 1) {
      const mid = Math.floor((ok + ng) / 2);
      const midKey = this._entries[mid].key;
      if (midKey === null) {
        throw new Error("invalid key");
      }
      if (midKey >= key) {
        ok = mid;
      } else {
        ng = mid;
      }
    }

    if (ok === this._entries.length) {
      return this._entries[ok - 1].value;
    }
    if (this._entries[ok].key === key) {
      return this._entries[ok].value;
    }
    return this._entries[ok - 1].value;
  }
  populateNewRoot(
    oldValue: number,
    newKey: number | null,
    newValue: number
  ): void {
    this._entries = [
      { key: null, value: oldValue },
      { key: newKey, value: newValue },
    ];
  }
  insertNodeAfter(
    oldValue: number,
    newKey: number | null,
    newValue: number
  ): void {
    const newIndex = this.valueIndex(oldValue) + 1;
    this._entries.splice(newIndex, 0, { key: newKey, value: newValue });
  }
  async moveHalfTo(
    recipient: BPlusTreeInternalPage,
    bufferPoolManager: BufferPoolManager
  ): Promise<void> {
    const moveStartIndex = Math.floor(MAX_ENTRY_COUNT / 2);
    const moveSize = this._entries.length - moveStartIndex;
    const moveEntries = this._entries.splice(moveStartIndex, moveSize);
    await recipient.copyNFrom(moveEntries, bufferPoolManager);
    this._entries.splice(moveStartIndex);
  }
  async copyNFrom(
    entries: BPlusTreeInternalPageEntry[],
    bufferPoolManager: BufferPoolManager
  ): Promise<void> {
    this._entries.push(...entries);
    for (const entry of this._entries) {
      const childPage = await bufferPoolManager.fetchPage(
        entry.value,
        new BPlusTreeInternalPageDeserializer()
      );
      if (!(childPage instanceof BPlusTreeInternalPage)) {
        throw new Error("invalid page type");
      }
      childPage.parentPageId = this.pageId;
      bufferPoolManager.unpinPage(childPage.pageId, true);
    }
  }
  remove(index: number): void {
    this._entries.splice(index, 1);
  }
}

export class BPlusTreeInternalPageDeserializer implements PageDeserializer {
  async deserialize(buffer: ArrayBuffer): Promise<BPlusTreeInternalPage> {
    const dataView = new DataView(buffer);
    const pageId = dataView.getInt32(0);
    const parentPageId = dataView.getInt32(
      HEADER_PAGE_ID_SIZE + HEADER_TYPE_SIZE
    );
    const entryCount = dataView.getInt32(
      HEADER_PAGE_ID_SIZE + HEADER_TYPE_SIZE + HEADER_PARENT_PAGE_ID_SIZE
    );
    const entries: BPlusTreeInternalPageEntry[] = [];
    for (let i = 0; i < entryCount; i++) {
      const key = dataView.getInt32(HEADER_SIZE + i * ENTRY_SIZE);
      const value = dataView.getInt32(
        HEADER_SIZE + i * ENTRY_SIZE + ENTRY_KEY_SIZE
      );
      entries.push({
        key: i === 0 ? null : key,
        value,
      });
    }
    return new BPlusTreeInternalPage(pageId, parentPageId, entries);
  }
}
export class BPlusTreeInternalPageGenerator implements PageGenerator {
  generate(
    pageId: number,
    parentPageId: number = INVALID_PAGE_ID
  ): BPlusTreeInternalPage {
    return new BPlusTreeInternalPage(pageId, parentPageId);
  }
}
