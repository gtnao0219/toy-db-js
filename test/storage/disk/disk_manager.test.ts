import { promises as fsp } from "fs";
import { DiskManagerImpl } from "../../../src/storage/disk/disk_manager";
import {
  PAGE_SIZE,
  Page,
  PageDeserializer,
} from "../../../src/storage/page/page";

describe("DiskManagerImpl", () => {
  describe("bootstrap", () => {
    it("data file and log file are created", async () => {});
  });
  describe("reset", () => {});
  // describe("existsDataFile", () => {
  //   it("returns false if data file does not exist", () => {
  //     const diskManager = new DiskManagerImpl(
  //       "test/data/disk_manager/does_not_exist"
  //     );
  //     expect(diskManager.existsDataFile()).toBe(false);
  //   });
  //   it("returns true if data file exists", () => {
  //     const diskManager = new DiskManagerImpl(
  //       "test/data/disk_manager/empty_data"
  //     );
  //     expect(diskManager.existsDataFile()).toBe(true);
  //   });
  // });
  // describe("createDataFile", () => {
  //   it("creates empty data file", async () => {
  //     const diskManager = new DiskManagerImpl(
  //       "test/data/disk_manager/does_not_exist"
  //     );
  //     const created = await diskManager.createDataFile();
  //     expect(created).toBe(true);
  //     const stats = await fsp.stat("test/data/disk_manager/does_not_exist");
  //     expect(stats.size).toBe(0);
  //     // Cleanup
  //     await fsp.unlink("test/data/disk_manager/does_not_exist");
  //   });
  //   it("does not create data file if it already exists", async () => {
  //     const diskManager = new DiskManagerImpl(
  //       "test/data/disk_manager/test_page_read_data"
  //     );
  //     const created = await diskManager.createDataFile();
  //     expect(created).toBe(false);
  //     const stats = await fsp.stat(
  //       "test/data/disk_manager/test_page_read_data"
  //     );
  //     expect(stats.size).toBe(PAGE_SIZE * 2);
  //   });
  // });
  describe("readPage", () => {
    it("reads page from data file", async () => {
      const diskManager = new DiskManagerImpl(
        "test/data/disk_manager/test_page_read_data"
      );
      const buffer = await diskManager.readPage(0);
      const view = new Uint8Array(buffer);
      for (let i = 0; i < view.length; i++) {
        expect(view[i]).toBe(1);
      }
    });
    it("reads second page from data file", async () => {
      const diskManager = new DiskManagerImpl(
        "test/data/disk_manager/test_page_read_data"
      );
      const buffer = await diskManager.readPage(1);
      const view = new Uint8Array(buffer);
      for (let i = 0; i < view.length; i++) {
        expect(view[i]).toBe(2);
      }
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
      const buffer = new ArrayBuffer(PAGE_SIZE);
      const view = new Uint8Array(buffer);
      for (let i = 0; i < view.length; i++) {
        view[i] = 3;
      }
      await diskManager.writePage(2, buffer);
      const newPageBuffer = await diskManager.readPage(2);
      const newPageView = new Uint8Array(newPageBuffer);
      for (let i = 0; i < newPageView.length; i++) {
        expect(newPageView[i]).toBe(3);
      }
      const firstPageBuffer = await diskManager.readPage(0);
      const firstPageView = new Uint8Array(firstPageBuffer);
      for (let i = 0; i < firstPageView.length; i++) {
        expect(firstPageView[i]).toBe(1);
      }
      // Cleanup
      await fsp.unlink("test/data/disk_manager/test_page_write_data");
    });
  });
  describe("readLog and writeLog", () => {
    it("reads and writes log", async () => {
      await fsp.copyFile(
        "test/data/disk_manager/empty_log",
        "test/data/disk_manager/temp_log"
      );
      const diskManager = new DiskManagerImpl(
        "test/data/disk_manager/empty_data",
        "test/data/disk_manager/temp_log"
      );
      const buffer = new ArrayBuffer(4);
      const view = new Uint8Array(buffer);
      for (let i = 0; i < view.length; i++) {
        view[i] = i;
      }
      await diskManager.writeLog(buffer);
      const logBuffer = await diskManager.readLog();
      console.log(logBuffer);
      const logView = new Uint8Array(logBuffer);
      for (let i = 0; i < logView.length; i++) {
        expect(logView[i]).toBe(i);
      }
      // Cleanup
      await fsp.unlink("test/data/disk_manager/temp_log");
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
    it("should allocate unique PageIds even when called concurrently", async () => {
      // copy test data file
      await fsp.copyFile(
        "test/data/disk_manager/test_page_read_data",
        "test/data/disk_manager/test_page_temp_data"
      );
      const diskManager = new DiskManagerImpl(
        "test/data/disk_manager/test_page_temp_data"
      );
      const pageIds = await Promise.all([
        diskManager.allocatePageId(),
        diskManager.allocatePageId(),
        diskManager.allocatePageId(),
        diskManager.allocatePageId(),
        diskManager.allocatePageId(),
        diskManager.allocatePageId(),
        diskManager.allocatePageId(),
        diskManager.allocatePageId(),
        diskManager.allocatePageId(),
        diskManager.allocatePageId(),
      ]);
      pageIds.sort((a, b) => a - b);
      expect(pageIds).toEqual([2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
      const stats = await fsp.stat(
        "test/data/disk_manager/test_page_temp_data"
      );
      expect(stats.size).toBe(PAGE_SIZE * 12);
      // Cleanup
      await fsp.unlink("test/data/disk_manager/test_page_temp_data");
    });
  });
});

async function prepareTestPageReadData() {
  const diskManager = new DiskManagerImpl(
    "test/data/disk_manager/test_page_read_data"
  );
  diskManager.reset();
  const firstPageBuffer = new ArrayBuffer(PAGE_SIZE);
  const firstPageView = new Uint8Array(firstPageBuffer);
  for (let i = 0; i < firstPageView.length; i++) {
    firstPageView[i] = 1;
  }
  const secondPageBuffer = new ArrayBuffer(PAGE_SIZE);
  const secondPageView = new Uint8Array(secondPageBuffer);
  for (let i = 0; i < secondPageView.length; i++) {
    secondPageView[i] = 2;
  }
  await diskManager.writePage(0, firstPageBuffer);
  await diskManager.writePage(1, secondPageBuffer);
}
