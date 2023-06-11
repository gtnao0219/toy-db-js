import { promises as fsp } from "fs";
import {
  DiskManager,
  DiskManagerImpl,
} from "../../../src/storage/disk/disk_manager";
import {
  PAGE_SIZE,
  Page,
  PageDeserializer,
} from "../../../src/storage/page/page";

describe("DiskManagerImpl", () => {
  describe("existsDataFile", () => {
    it("returns false if data file does not exist", () => {
      const diskManager = new DiskManagerImpl(
        "test/data/disk_manager/does_not_exist"
      );
      expect(diskManager.existsDataFile()).toBe(false);
    });
    it("returns true if data file exists", () => {
      const diskManager = new DiskManagerImpl(
        "test/data/disk_manager/empty_data"
      );
      expect(diskManager.existsDataFile()).toBe(true);
    });
  });
  describe("createDataFile", () => {
    it("creates empty data file", async () => {
      const diskManager = new DiskManagerImpl(
        "test/data/disk_manager/does_not_exist"
      );
      const created = await diskManager.createDataFile();
      expect(created).toBe(true);
      const stats = await fsp.stat("test/data/disk_manager/does_not_exist");
      expect(stats.size).toBe(0);

      // Cleanup
      await fsp.unlink("test/data/disk_manager/does_not_exist");
    });
    it("does not create data file if it already exists", async () => {
      const diskManager = new DiskManagerImpl(
        "test/data/disk_manager/empty_data"
      );
      const created = await diskManager.createDataFile();
      expect(created).toBe(false);
    });
  });
  describe("readPage", () => {
    it("reads page from data file", async () => {
      const diskManager = new DiskManagerImpl(
        "test/data/disk_manager/test_page_read_data"
      );
      const page = await diskManager.readPage(0, new TestPageDeserializer());
      if (!(page instanceof TestPage)) {
        throw new Error("page is not TestPage");
      }
      expect(page.pageId).toBe(0);
      expect(page.randomNumbers).toEqual([2, 3, 5, 7, 11, 13, 17, 19]);
    });
    it("reads second page from data file", async () => {
      const diskManager = new DiskManagerImpl(
        "test/data/disk_manager/test_page_read_data"
      );
      const page = await diskManager.readPage(1, new TestPageDeserializer());
      if (!(page instanceof TestPage)) {
        throw new Error("page is not TestPage");
      }
      expect(page.pageId).toBe(1);
      expect(page.randomNumbers).toEqual([23, 29, 31, 37, 41, 43, 47, 53]);
    });
  });
  describe("writePage", () => {
    it("writes page to data file", async () => {
      // copy test data file
      await fsp.copyFile(
        "test/data/disk_manager/test_page_read_data",
        "test/data/disk_manager/test_page_write_data"
      );
      const diskManager = new DiskManagerImpl(
        "test/data/disk_manager/test_page_write_data"
      );
      const thirdPage = new TestPage(2, [59, 61, 67, 71, 73, 79, 83, 89]);
      await diskManager.writePage(thirdPage);

      const newPage = await diskManager.readPage(2, new TestPageDeserializer());
      if (!(newPage instanceof TestPage)) {
        throw new Error("page is not TestPage");
      }
      expect(newPage.pageId).toBe(2);
      expect(newPage.randomNumbers).toEqual([59, 61, 67, 71, 73, 79, 83, 89]);
      const firstPage = await diskManager.readPage(
        0,
        new TestPageDeserializer()
      );
      if (!(firstPage instanceof TestPage)) {
        throw new Error("page is not TestPage");
      }
      expect(firstPage.pageId).toBe(0);
      expect(firstPage.randomNumbers).toEqual([2, 3, 5, 7, 11, 13, 17, 19]);

      // Cleanup
      await fsp.unlink("test/data/disk_manager/test_page_write_data");
    });
  });
  describe("allocatePageId", () => {
    it("returns 0 if data file is empty", async () => {
      // copy test data file
      await fsp.copyFile(
        "test/data/disk_manager/empty_data",
        "test/data/disk_manager/test_page_temp_data"
      );

      const diskManager = new DiskManagerImpl(
        "test/data/disk_manager/test_page_temp_data"
      );
      const pageId = await diskManager.allocatePageId();
      expect(pageId).toBe(0);
      const stats = await fsp.stat(
        "test/data/disk_manager/test_page_temp_data"
      );
      expect(stats.size).toBe(PAGE_SIZE);

      // Cleanup
      await fsp.unlink("test/data/disk_manager/test_page_temp_data");
    });
    it("returns 1 if data file has one page", async () => {
      // copy test data file
      await fsp.copyFile(
        "test/data/disk_manager/test_page_read_data",
        "test/data/disk_manager/test_page_temp_data"
      );

      const diskManager = new DiskManagerImpl(
        "test/data/disk_manager/test_page_temp_data"
      );
      const pageId = await diskManager.allocatePageId();
      expect(pageId).toBe(2);
      const stats = await fsp.stat(
        "test/data/disk_manager/test_page_temp_data"
      );
      expect(stats.size).toBe(PAGE_SIZE * 3);

      // Cleanup
      await fsp.unlink("test/data/disk_manager/test_page_temp_data");
    });
  });
});

class TestPage extends Page {
  constructor(protected _pageId: number, public randomNumbers: number[]) {
    super(_pageId);
  }
  serialize(): ArrayBuffer {
    const size = 8 + this.randomNumbers.length * 4;
    if (size > PAGE_SIZE) {
      throw new Error("Page size exceeded");
    }
    const buffer = new ArrayBuffer(PAGE_SIZE);
    const view = new DataView(buffer);
    view.setInt32(0, this.pageId);
    view.setInt32(4, this.randomNumbers.length);
    for (let i = 0; i < this.randomNumbers.length; i++) {
      view.setInt32(8 + i * 4, this.randomNumbers[i]);
    }
    return buffer;
  }
}
class TestPageDeserializer implements PageDeserializer {
  deserialize(buffer: ArrayBuffer): Page {
    const view = new DataView(buffer);
    const pageId = view.getInt32(0);
    const size = view.getInt32(4);
    const randomNumbers = [];
    for (let i = 0; i < size; i++) {
      const value = view.getInt32(8 + i * 4);
      randomNumbers.push(value);
    }
    return new TestPage(pageId, randomNumbers);
  }
}

async function prepareTestPageReadData() {
  const diskManager = new DiskManagerImpl(
    "test/data/disk_manager/test_page_read_data"
  );
  diskManager.createDataFile();
  const firstPage = new TestPage(0, [2, 3, 5, 7, 11, 13, 17, 19]);
  const secondPage = new TestPage(1, [23, 29, 31, 37, 41, 43, 47, 53]);
  await diskManager.writePage(firstPage);
  await diskManager.writePage(secondPage);
}
