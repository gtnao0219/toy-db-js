import { promises as fsp, existsSync } from "fs";
import { PAGE_SIZE } from "../page/page";
import Mutex from "../../../node_modules/async-mutex/lib/Mutex";

const DEFAULT_DATA_FILE_NAME = "data";

export interface DiskManager {
  existsDataFile(): boolean;
  createDataFile(): Promise<boolean>;
  readPage(pageId: number): Promise<ArrayBuffer>;
  writePage(pageId: number, buffer: ArrayBuffer): Promise<void>;
  allocatePageId(): Promise<number>;
}

export class DiskManagerImpl implements DiskManager {
  private _mutex: Mutex;
  constructor(private _data_file_name: string = DEFAULT_DATA_FILE_NAME) {
    this._mutex = new Mutex();
  }
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
  async readPage(pageId: number): Promise<ArrayBuffer> {
    const buffer = new ArrayBuffer(PAGE_SIZE);
    const view = new DataView(buffer);
    const fd = await fsp.open(this._data_file_name, "r");
    await fd.read(view, 0, PAGE_SIZE, pageId * PAGE_SIZE);
    await fd.close();
    return buffer;
  }
  async writePage(pageId: number, buffer: ArrayBuffer): Promise<void> {
    const fd = await fsp.open(this._data_file_name, "r+");
    await fd.write(new Uint8Array(buffer), 0, PAGE_SIZE, pageId * PAGE_SIZE);
    await fd.close();
  }
  async allocatePageId(): Promise<number> {
    return await this._mutex.runExclusive(async () => {
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
    });
  }
  private async pageCount(): Promise<number> {
    const stats = await fsp.stat(this._data_file_name);
    return stats.size / PAGE_SIZE;
  }
}
