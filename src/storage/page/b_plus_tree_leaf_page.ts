import { RID } from "../../common/RID";
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
const HEADER_NEXT_PAGE_ID_SIZE = 4;
const HEADER_CURRENT_SIZE_SIZE = 4;
const HEADER_SIZE =
  HEADER_PAGE_ID_SIZE +
  HEADER_TYPE_SIZE +
  HEADER_PARENT_PAGE_ID_SIZE +
  HEADER_NEXT_PAGE_ID_SIZE +
  HEADER_CURRENT_SIZE_SIZE;
const ENTRY_KEY_SIZE = 4;
const ENTRY_PAGE_ID_SIZE = 4;
const ENTRY_SLOT_ID_SIZE = 4;
const ENTRY_RID_SIZE = ENTRY_PAGE_ID_SIZE + ENTRY_SLOT_ID_SIZE;
const ENTRY_SIZE = ENTRY_KEY_SIZE + ENTRY_RID_SIZE;
const MAX_ENTRY_COUNT = Math.floor((PAGE_SIZE - HEADER_SIZE) / ENTRY_SIZE);

type BPlusTreeLeafPageEntry = {
  key: number; // only support number for now
  value: RID;
};

const B_PLUS_TREE_LEAF_PAGE_TYPE = 1;

export class BPlusTreeLeafPage extends Page {
  constructor(
    protected _pageId: number = INVALID_PAGE_ID,
    private _parentPageId: number = INVALID_PAGE_ID,
    private _nextPageId: number = INVALID_PAGE_ID,
    private _entries: BPlusTreeLeafPageEntry[] = []
  ) {
    super(_pageId);
  }
  get parentPageId(): number {
    return this._parentPageId;
  }
  set parentPageId(pageId: number) {
    this._parentPageId = pageId;
  }
  get nextPageId(): number {
    return this._nextPageId;
  }
  set nextPageId(pageId: number) {
    this._nextPageId = pageId;
  }
  get entries(): BPlusTreeLeafPageEntry[] {
    return this._entries;
  }
  serialize(): ArrayBuffer {
    const buffer = new ArrayBuffer(PAGE_SIZE);
    const dataView = new DataView(buffer);
    dataView.setInt32(0, this.pageId);
    dataView.setInt32(HEADER_PAGE_ID_SIZE, B_PLUS_TREE_LEAF_PAGE_TYPE);
    dataView.setInt32(
      HEADER_PAGE_ID_SIZE + HEADER_TYPE_SIZE,
      this._parentPageId
    );
    dataView.setInt32(
      HEADER_PAGE_ID_SIZE + HEADER_TYPE_SIZE + HEADER_PARENT_PAGE_ID_SIZE,
      this._nextPageId
    );
    dataView.setInt32(
      HEADER_PAGE_ID_SIZE +
        HEADER_TYPE_SIZE +
        HEADER_PARENT_PAGE_ID_SIZE +
        HEADER_NEXT_PAGE_ID_SIZE,
      this._entries.length
    );
    for (let i = 0; i < this._entries.length; ++i) {
      const entry = this._entries[i];
      dataView.setInt32(HEADER_SIZE + i * ENTRY_SIZE, entry.key);
      dataView.setInt32(
        HEADER_SIZE + i * ENTRY_SIZE + ENTRY_KEY_SIZE,
        entry.value.pageId
      );
      dataView.setInt32(
        HEADER_SIZE + i * ENTRY_SIZE + ENTRY_KEY_SIZE + ENTRY_PAGE_ID_SIZE,
        entry.value.slotId
      );
    }
    return buffer;
  }
  keyAt(index: number): number {
    return this._entries[index].key;
  }
  valueAt(index: number): RID {
    return this._entries[index].value;
  }
  keyIndex(key: number): number {
    let ng = -1;
    let ok = this._entries.length;
    while (Math.abs(ok - ng) > 1) {
      const mid = Math.floor((ok + ng) / 2);
      if (this._entries[mid].key >= key) {
        ok = mid;
      } else {
        ng = mid;
      }
    }
    return ok;
  }
  insert(key: number, value: RID): void {
    const index = this.keyIndex(key);
    this._entries.splice(index, 0, { key, value });
  }
  lookup(key: number): RID | null {
    const index = this.keyIndex(key);
    if (index >= this._entries.length || this._entries[index].key !== key) {
      return null;
    }
    return this._entries[index].value;
  }
  deleteByKey(key: number): void {
    const index = this.keyIndex(key);
    const nextIndex = this.keyIndex(key + 1);
    this._entries.splice(index, nextIndex - index);
  }
  deleteByKeyValue(key: number, value: RID): void {
    const index = this.keyIndex(key);
    const nextIndex = this.keyIndex(key + 1);
    for (let i = index; i < nextIndex; ++i) {
      if (
        this._entries[i].value.pageId === value.pageId &&
        this._entries[i].value.slotId === value.slotId
      ) {
        this._entries.splice(i, 1);
        return;
      }
    }
  }
}

export class BPlusTreeLeafPageDeserializer implements PageDeserializer {
  async deserialize(buffer: ArrayBuffer): Promise<BPlusTreeLeafPage> {
    const dataView = new DataView(buffer);
    const pageId = dataView.getInt32(0);
    const parentPageId = dataView.getInt32(
      HEADER_PAGE_ID_SIZE + HEADER_TYPE_SIZE
    );
    const nextPageId = dataView.getInt32(
      HEADER_PAGE_ID_SIZE + HEADER_TYPE_SIZE + HEADER_PARENT_PAGE_ID_SIZE
    );
    const entryCount = dataView.getInt32(
      HEADER_PAGE_ID_SIZE +
        HEADER_TYPE_SIZE +
        HEADER_PARENT_PAGE_ID_SIZE +
        HEADER_NEXT_PAGE_ID_SIZE
    );
    const entries: BPlusTreeLeafPageEntry[] = [];
    for (let i = 0; i < entryCount; i++) {
      const key = dataView.getInt32(HEADER_SIZE + i * ENTRY_SIZE);
      const pageId = dataView.getInt32(
        HEADER_SIZE + i * ENTRY_SIZE + ENTRY_KEY_SIZE
      );
      const slotId = dataView.getInt32(
        HEADER_SIZE + i * ENTRY_SIZE + ENTRY_KEY_SIZE + ENTRY_PAGE_ID_SIZE
      );
      entries.push({
        key,
        value: { pageId, slotId },
      });
    }
    return new BPlusTreeLeafPage(pageId, parentPageId, nextPageId, entries);
  }
}
export class BPlusTreeLeafPageGenerator implements PageGenerator {
  generate(
    pageId: number,
    parentPageId: number = INVALID_PAGE_ID,
    nextPageId: number = INVALID_PAGE_ID
  ): BPlusTreeLeafPage {
    return new BPlusTreeLeafPage(pageId, parentPageId, nextPageId);
  }
}
