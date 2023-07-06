export const PAGE_SIZE = 128;

export const INVALID_PAGE_ID = -1;

export enum PageType {
  TABLE_PAGE,
  HEADER_PAGE,
}

export abstract class Page {
  constructor(protected _pageId: number = INVALID_PAGE_ID) {}
  get pageId(): number {
    return this._pageId;
  }
  abstract serialize(): ArrayBuffer;
}

export interface PageDeserializer {
  deserialize(arrayBuffer: ArrayBuffer): Promise<Page>;
}
export interface PageGenerator {
  generate(pageId: number): Page;
}
