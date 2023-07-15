import { promises as fsp, existsSync } from "fs";
import { PAGE_SIZE, PageId } from "../page/page";
import Mutex from "../../../node_modules/async-mutex/lib/Mutex";

const DEFAULT_DATA_FILE_NAME = "data";
const DEFAULT_LOG_FILE_NAME = "log";

export interface DiskManager {
  bootstrap(): Promise<void>;
  reset(): Promise<void>;
  isEmpty(): Promise<boolean>;
  readPage(pageId: number): Promise<ArrayBuffer>;
  writePage(pageId: number, buffer: ArrayBuffer): Promise<void>;
  readLog(): Promise<ArrayBuffer>;
  writeLog(buffer: ArrayBuffer): Promise<void>;
  allocatePageId(): Promise<number>;
}

export class DiskManagerImpl implements DiskManager {
  private _mutex: Mutex;
  constructor(
    private _data_file_name: string = DEFAULT_DATA_FILE_NAME,
    private _log_file_name: string = DEFAULT_LOG_FILE_NAME
  ) {
    this._mutex = new Mutex();
  }
  async bootstrap(): Promise<void> {
    if (this.mustInitialize()) {
      await this.reset();
    }
  }
  private mustInitialize(): boolean {
    return !(this.existsDataFile() && this.existsLogFile());
  }
  private existsDataFile(): boolean {
    return existsSync(this._data_file_name);
  }
  private existsLogFile(): boolean {
    return existsSync(this._log_file_name);
  }
  async reset(): Promise<void> {
    if (this.existsDataFile()) {
      await fsp.unlink(this._data_file_name);
    }
    if (this.existsLogFile()) {
      await fsp.unlink(this._log_file_name);
    }
    await this.createDataFile();
    await this.createLogFile();
  }
  private async createDataFile(): Promise<void> {
    if (this.existsDataFile()) {
      return;
    }
    const fd = await fsp.open(this._data_file_name, "w");
    await fd.write(new Uint8Array(new ArrayBuffer(0)), 0, 0);
    await fd.close();
    return;
  }
  private async createLogFile(): Promise<void> {
    if (this.existsLogFile()) {
      return;
    }
    const fd = await fsp.open(this._log_file_name, "w");
    await fd.write(new Uint8Array(new ArrayBuffer(0)), 0, 0);
    await fd.close();
    return;
  }
  async isEmpty(): Promise<boolean> {
    const stats = await fsp.stat(this._data_file_name);
    return stats.size === 0;
  }
  async readPage(pageId: PageId): Promise<ArrayBuffer> {
    const fd = await fsp.open(this._data_file_name, "r");
    const buffer = new ArrayBuffer(PAGE_SIZE);
    await fd.read(new DataView(buffer), 0, PAGE_SIZE, pageId * PAGE_SIZE);
    await fd.close();
    return buffer;
  }
  async writePage(pageId: number, buffer: ArrayBuffer): Promise<void> {
    const fd = await fsp.open(this._data_file_name, "r+");
    await fd.write(new Uint8Array(buffer), 0, PAGE_SIZE, pageId * PAGE_SIZE);
    await fd.close();
  }
  async readLog(): Promise<ArrayBuffer> {
    const fd = await fsp.open(this._log_file_name, "r");
    const buffer = new ArrayBuffer((await fd.stat()).size);
    await fd.read(new DataView(buffer), 0, buffer.byteLength, 0);
    await fd.close();
    return buffer;
  }
  async writeLog(buffer: ArrayBuffer): Promise<void> {
    const fd = await fsp.open(this._log_file_name, "a");
    await fd.appendFile(new Uint8Array(buffer));
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
