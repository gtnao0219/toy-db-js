import { promises as fsp, existsSync } from "fs";
import { PAGE_SIZE, Page, PageDeserializer } from "../page/page";

const DEFAULT_DATA_FILE_NAME = "data";

export interface DiskManager {
  existsDataFile(): boolean;
  createDataFile(): Promise<boolean>;
  readPage(pageId: number, pageDeserializer: PageDeserializer): Promise<Page>;
  writePage(page: Page): Promise<void>;
  allocatePageId(): Promise<number>;
}

export class DiskManagerImpl implements DiskManager {
  constructor(private _data_file_name: string = DEFAULT_DATA_FILE_NAME) {}
  existsDataFile(): boolean {
    return existsSync(this._data_file_name);
  }
  async createDataFile(): Promise<boolean> {
    const exists = this.existsDataFile();
    if (exists) {
      return false;
    }
    const fd = await fsp.open(this._data_file_name, "w");
    await fd.write(new Uint8Array(new ArrayBuffer(0)), 0, 0);
    await fd.close();
    return true;
  }
  async readPage(
    pageId: number,
    pageDeserializer: PageDeserializer
  ): Promise<Page> {
    const buffer = new ArrayBuffer(PAGE_SIZE);
    const view = new DataView(buffer);
    const fd = await fsp.open(this._data_file_name, "r");
    await fd.read(view, 0, PAGE_SIZE, pageId * PAGE_SIZE);
    await fd.close();
    return pageDeserializer.deserialize(buffer);
  }
  async writePage(page: Page): Promise<void> {
    const fd = await fsp.open(this._data_file_name, "r+");
    await fd.write(
      new Uint8Array(page.serialize()),
      0,
      PAGE_SIZE,
      page.pageId * PAGE_SIZE
    );
    await fd.close();
  }
  async allocatePageId(): Promise<number> {
    const pageId = await this.pageCount();
    const fd = await fsp.open(this._data_file_name, "r+");
    await fd.write(
      new Uint8Array(new ArrayBuffer(PAGE_SIZE)),
      0,
      PAGE_SIZE,
      pageId * PAGE_SIZE
    );
    await fd.close();
    return pageId;
  }
  private async pageCount(): Promise<number> {
    const stats = await fsp.stat(this._data_file_name);
    return stats.size / PAGE_SIZE;
  }
}
