import fs from "fs";
import { PAGE_SIZE, Page, PageType } from "../page/page";
import { deserializeTablePage } from "../page/table_page";

const DATA_FILE_NAME = "data";

export class DiskManager {
  init(): boolean {
    if (fs.existsSync(DATA_FILE_NAME)) {
      return false;
    }
    const buffer = new ArrayBuffer(0);
    const view = new DataView(buffer);
    const fd = fs.openSync(DATA_FILE_NAME, "w");
    fs.writeSync(fd, view, 0, 0, 0);
    return true;
  }
  readPage(pageId: number, pageType: PageType): Page {
    const buffer = new ArrayBuffer(PAGE_SIZE);
    const view = new DataView(buffer);
    const fd = fs.openSync(DATA_FILE_NAME, "r");
    fs.readSync(fd, view, 0, PAGE_SIZE, pageId * PAGE_SIZE);
    switch (pageType) {
      case PageType.TABLE_PAGE:
        return deserializeTablePage(buffer);
    }
  }
  writePage(page: Page) {
    const view = new DataView(page.serialize());
    const fd = fs.openSync(DATA_FILE_NAME, "w");
    fs.writeSync(fd, view, 0, PAGE_SIZE, page.getPageId() * PAGE_SIZE);
  }
}
