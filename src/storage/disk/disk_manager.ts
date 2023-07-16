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
  private _dataFileMutex: Mutex;
  private _logFileMutex: Mutex;
  constructor(
    private _dataFileName: string = DEFAULT_DATA_FILE_NAME,
    private _logFileName: string = DEFAULT_LOG_FILE_NAME
  ) {
    this._dataFileMutex = new Mutex();
    this._logFileMutex = new Mutex();
  }
  async bootstrap(): Promise<void> {
    if (this.mustInitialize()) {
      await this.reset();
    }
  }
  async reset(): Promise<void> {
    if (this.existsDataFile()) {
      await fsp.unlink(this._dataFileName);
    }
    if (this.existsLogFile()) {
      await fsp.unlink(this._logFileName);
    }
    await this.createDataFile();
    await this.createLogFile();
  }
  async isEmpty(): Promise<boolean> {
    const stats = await fsp.stat(this._dataFileName);
    return stats.size === 0;
  }
  async readPage(pageId: PageId): Promise<ArrayBuffer> {
    const fd = await fsp.open(this._dataFileName, "r");
    const buffer = new ArrayBuffer(PAGE_SIZE);
    await fd.read(new DataView(buffer), 0, PAGE_SIZE, pageId * PAGE_SIZE);
    await fd.close();
    return buffer;
  }
  async writePage(pageId: number, buffer: ArrayBuffer): Promise<void> {
    return await this._dataFileMutex.runExclusive(async () => {
      const fd = await fsp.open(this._dataFileName, "r+");
      await fd.write(new Uint8Array(buffer), 0, PAGE_SIZE, pageId * PAGE_SIZE);
      await fd.close();
    });
  }
  async readLog(): Promise<ArrayBuffer> {
    const fd = await fsp.open(this._logFileName, "r");
    const buffer = new ArrayBuffer((await fd.stat()).size);
    await fd.read(new DataView(buffer), 0, buffer.byteLength, 0);
    await fd.close();
    return buffer;
  }
  async writeLog(buffer: ArrayBuffer): Promise<void> {
    return await this._logFileMutex.runExclusive(async () => {
      const fd = await fsp.open(this._logFileName, "a");
      await fd.appendFile(new Uint8Array(buffer));
      await fd.datasync();
      await fd.close();
    });
  }
  async allocatePageId(): Promise<number> {
    return await this._dataFileMutex.runExclusive(async () => {
      const pageId = await this.pageCount();
      const fd = await fsp.open(this._dataFileName, "r+");
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

  private mustInitialize(): boolean {
    return !(this.existsDataFile() && this.existsLogFile());
  }
  private existsDataFile(): boolean {
    return existsSync(this._dataFileName);
  }
  private existsLogFile(): boolean {
    return existsSync(this._logFileName);
  }
  private async createDataFile(): Promise<void> {
    if (this.existsDataFile()) {
      return;
    }
    const fd = await fsp.open(this._dataFileName, "w");
    await fd.write(new Uint8Array(new ArrayBuffer(0)), 0, 0);
    await fd.close();
    return;
  }
  private async createLogFile(): Promise<void> {
    if (this.existsLogFile()) {
      return;
    }
    const fd = await fsp.open(this._logFileName, "w");
    await fd.write(new Uint8Array(new ArrayBuffer(0)), 0, 0);
    await fd.close();
    return;
  }
  private async pageCount(): Promise<number> {
    const stats = await fsp.stat(this._dataFileName);
    return stats.size / PAGE_SIZE;
  }
}
