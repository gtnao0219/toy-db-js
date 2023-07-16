import { promises as fsp, existsSync } from "fs";
import path from "path";
import { DiskManagerImpl } from "../../../src/storage/disk/disk_manager";
import { PAGE_SIZE } from "../../../src/storage/page/page";

describe("DiskManagerImpl", () => {
  let diskManager: DiskManagerImpl;
  const TEST_DATA_DIR = "./test_data";
  const DATA_FILE = path.join(TEST_DATA_DIR, "data");
  const LOG_FILE = path.join(TEST_DATA_DIR, "log");
  beforeEach(async () => {
    await fsp.mkdir(TEST_DATA_DIR, { recursive: true });
    diskManager = new DiskManagerImpl(DATA_FILE, LOG_FILE);
  });
  afterEach(async () => {
    if (existsSync(DATA_FILE)) {
      await fsp.unlink(DATA_FILE);
    }
    if (existsSync(LOG_FILE)) {
      await fsp.unlink(LOG_FILE);
    }
    await fsp.rmdir(TEST_DATA_DIR);
  });
  describe("bootstrap", () => {
    it("should initialize correctly if data and log files do not exist", async () => {
      await diskManager.bootstrap();

      expect(existsSync(DATA_FILE)).toBe(true);
      expect(existsSync(LOG_FILE)).toBe(true);
    });
    it("should reset if only data file exists", async () => {
      await fsp.writeFile(DATA_FILE, "Test data");

      await diskManager.bootstrap();

      const dataFileContent = await fsp.readFile(DATA_FILE);
      const logFileContent = await fsp.readFile(LOG_FILE);
      expect(dataFileContent.byteLength).toBe(0);
      expect(logFileContent.byteLength).toBe(0);
    });

    it("should reset if only log file exists", async () => {
      await fsp.writeFile(LOG_FILE, "Test log");

      await diskManager.bootstrap();

      const dataFileContent = await fsp.readFile(DATA_FILE);
      const logFileContent = await fsp.readFile(LOG_FILE);
      expect(dataFileContent.byteLength).toBe(0);
      expect(logFileContent.byteLength).toBe(0);
    });
    it("should not initialize if data and log files exist", async () => {
      await fsp.writeFile(DATA_FILE, "Test data");
      await fsp.writeFile(LOG_FILE, "Test log");

      await diskManager.bootstrap();

      const dataFileContent = await fsp.readFile(DATA_FILE, "utf-8");
      const logFileContent = await fsp.readFile(LOG_FILE, "utf-8");
      expect(dataFileContent).toBe("Test data");
      expect(logFileContent).toBe("Test log");
    });
  });
  describe("reset", () => {
    it("should delete and recreate data and log files", async () => {
      await fsp.writeFile(DATA_FILE, "Test data");
      await fsp.writeFile(LOG_FILE, "Test log");

      await diskManager.reset();

      const dataFileContent = await fsp.readFile(DATA_FILE);
      const logFileContent = await fsp.readFile(LOG_FILE);
      expect(dataFileContent.byteLength).toBe(0);
      expect(logFileContent.byteLength).toBe(0);
    });
  });
  describe("isEmpty", () => {
    it("should return true if the data file is empty", async () => {
      await diskManager.reset();

      const isEmpty = await diskManager.isEmpty();

      expect(isEmpty).toBe(true);
    });

    it("should return false if the data file is not empty", async () => {
      await diskManager.reset();
      await fsp.writeFile(DATA_FILE, "Test data");

      const isEmpty = await diskManager.isEmpty();

      expect(isEmpty).toBe(false);
    });
  });
  describe("readPage", () => {
    it("should read correct data from a specified page", async () => {
      await diskManager.reset();
      const firstPageBuffer = new Uint8Array(PAGE_SIZE);
      const secondPageBuffer = new Uint8Array(PAGE_SIZE);
      for (let i = 0; i < PAGE_SIZE; i++) {
        firstPageBuffer[i] = i % 256;
        secondPageBuffer[i] = (i + 1) % 256;
      }
      await fsp.writeFile(
        DATA_FILE,
        Buffer.concat([firstPageBuffer, secondPageBuffer])
      );

      const resultBuffer = await diskManager.readPage(1);

      expect(new Uint8Array(resultBuffer)).toEqual(secondPageBuffer);
    });
  });
  describe("writePage", () => {
    it("should write correct data to a specified page", async () => {
      await diskManager.reset();
      const firstPageBuffer = new Uint8Array(PAGE_SIZE);
      const secondPageBuffer = new Uint8Array(PAGE_SIZE);
      const thirdPageBuffer = new Uint8Array(PAGE_SIZE);
      const newSecondPageBuffer = new Uint8Array(PAGE_SIZE);
      for (let i = 0; i < PAGE_SIZE; i++) {
        firstPageBuffer[i] = i % 256;
        secondPageBuffer[i] = (i + 1) % 256;
        thirdPageBuffer[i] = (i + 2) % 256;
        newSecondPageBuffer[i] = (i + 3) % 256;
      }
      await fsp.writeFile(
        DATA_FILE,
        Buffer.concat([firstPageBuffer, secondPageBuffer, thirdPageBuffer])
      );

      await diskManager.writePage(1, newSecondPageBuffer.buffer);

      const dataFileContent = await fsp.readFile(DATA_FILE);
      expect(dataFileContent.byteLength).toBe(PAGE_SIZE * 3);
      expect(new Uint8Array(dataFileContent.subarray(0, PAGE_SIZE))).toEqual(
        firstPageBuffer
      );
      expect(
        new Uint8Array(dataFileContent.subarray(PAGE_SIZE, PAGE_SIZE * 2))
      ).toEqual(newSecondPageBuffer);
      expect(new Uint8Array(dataFileContent.subarray(PAGE_SIZE * 2))).toEqual(
        thirdPageBuffer
      );
    });
  });
  describe("readLog", () => {
    it("should read the entire log file", async () => {
      await diskManager.reset();
      const logBuffer = new Uint8Array([1, 2, 3, 4]);
      await fsp.writeFile(LOG_FILE, logBuffer);

      const resultBuffer = await diskManager.readLog();

      expect(new Uint8Array(resultBuffer)).toEqual(logBuffer);
    });
  });
  describe("writeLog", () => {
    it("should append data to the log file", async () => {
      await diskManager.reset();
      const logBuffer1 = new Uint8Array([1, 2, 3, 4]);
      await diskManager.writeLog(logBuffer1.buffer);
      const logBuffer2 = new Uint8Array([5, 6, 7, 8]);

      await diskManager.writeLog(logBuffer2.buffer);

      const resultBuffer = await fsp.readFile(LOG_FILE);
      expect(new Uint8Array(resultBuffer)).toEqual(
        new Uint8Array([...logBuffer1, ...logBuffer2])
      );
    });
  });
  describe("allocatePageId", () => {
    it("should correctly allocate a new page id", async () => {
      await diskManager.reset();
      const pageBuffer = new Uint8Array(PAGE_SIZE);
      await fsp.writeFile(DATA_FILE, pageBuffer);

      const pageId = await diskManager.allocatePageId();

      expect(pageId).toBe(1);
      const dataFileContent = await fsp.readFile(DATA_FILE);
      expect(dataFileContent.byteLength).toBe(PAGE_SIZE * 2);
    });
    it("should allocate unique page ids even in concurrent operations", async () => {
      await diskManager.reset();
      const promises = Array(10)
        .fill(null)
        .map(() => diskManager.allocatePageId());
      const pageIds = await Promise.all(promises);
      const uniquePageIds = new Set(pageIds);
      expect(pageIds.length).toBe(uniquePageIds.size);
    });
  });
});
