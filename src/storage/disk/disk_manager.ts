import fs from "fs";
import { PAGE_SIZE, Page, PageType } from "../page/page";
import { TablePage } from "../page/table_page";
import { HeaderPage } from "../page/header_page";

const DATA_FILE_NAME = "data";

export class DiskManager {
  init(): boolean {
    if (fs.existsSync(DATA_FILE_NAME)) {
      return false;
    }
    const fd = fs.openSync(DATA_FILE_NAME, "w");
    fs.writeSync(fd, new DataView(new ArrayBuffer(0)), 0, 0, 0);
    return true;
  }
  readPage(pageId: number, pageType: PageType): Page {
    const buffer = new ArrayBuffer(PAGE_SIZE);
    const fd = fs.openSync(DATA_FILE_NAME, "r");
    fs.readSync(fd, new DataView(buffer), 0, PAGE_SIZE, pageId * PAGE_SIZE);
    switch (pageType) {
      case PageType.TABLE_PAGE:
        return new TablePage(buffer);
      case PageType.HEADER_PAGE:
        return new HeaderPage(buffer);
    }
  }
  writePage(page: Page) {
    const fd = fs.openSync(DATA_FILE_NAME, "r+");
    fs.writeSync(
      fd,
      new DataView(page.serialize()),
      0,
      PAGE_SIZE,
      page.pageId * PAGE_SIZE
    );
  }
  allocatePage(): number {
    return fs.statSync(DATA_FILE_NAME).size / PAGE_SIZE;
  }
}
