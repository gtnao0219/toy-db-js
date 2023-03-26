import fs from "fs";
import { PAGE_SIZE, Page, PageDeserializer } from "../page/page";

const DEFAULT_DATA_FILE_NAME = "data";

export class DiskManager {
  constructor(private _data_file_name: string = DEFAULT_DATA_FILE_NAME) {}
  existsDataFile(): boolean {
    return fs.existsSync(this._data_file_name);
  }
  createDataFile(): void {
    if (this.existsDataFile()) {
      return;
    }
    const fd = fs.openSync(this._data_file_name, "w");
    fs.writeSync(fd, new DataView(new ArrayBuffer(0)), 0, 0, 0);
  }
  readPage(pageId: number, pageDeserializer: PageDeserializer): Page {
    const buffer = new ArrayBuffer(PAGE_SIZE);
    const view = new DataView(buffer);
    const fd = fs.openSync(this._data_file_name, "r");
    fs.readSync(fd, view, 0, PAGE_SIZE, pageId * PAGE_SIZE);
    return pageDeserializer.deserialize(buffer);
  }
  writePage(page: Page): void {
    const fd = fs.openSync(this._data_file_name, "r+");
    fs.writeSync(
      fd,
      new DataView(page.serialize()),
      0,
      PAGE_SIZE,
      page.pageId * PAGE_SIZE
    );
  }
  allocatePageId(): number {
    const pageId = this.pageCount();
    const fd = fs.openSync(this._data_file_name, "r+");
    fs.writeSync(
      fd,
      new DataView(new ArrayBuffer(PAGE_SIZE)),
      0,
      PAGE_SIZE,
      pageId * PAGE_SIZE
    );
    return pageId;
  }
  private pageCount(): number {
    return fs.statSync(this._data_file_name).size / PAGE_SIZE;
  }
}
