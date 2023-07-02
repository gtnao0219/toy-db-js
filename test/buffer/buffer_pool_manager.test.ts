import { promises as fsp } from "fs";

import { BufferPoolManagerImpl } from "../../src/buffer/buffer_pool_manager";
import { DiskManagerImpl } from "../../src/storage/disk/disk_manager";
import { PageDeserializer } from "../../src/storage/page/page";
import { PAGE_SIZE } from "../../src/storage/page/page";
import { Page, PageGenerator } from "../../src/storage/page/page";

describe("BufferPoolManagerImpl", () => {
  it("Scenario", async () => {
    const testFilePath = "test/buffer/data/temp";
    await fsp.open(testFilePath, "w");
    const diskManager = new DiskManagerImpl(testFilePath);
    const bufferPoolManager = new BufferPoolManagerImpl(diskManager, 3);

    // Scenario: The buffer pool is empty. We should be able to create a new page.
    const page1_1 = await bufferPoolManager.newPage(new TestPageGenerator(1));
    expect(page1_1.pageId).toBe(0);
    expect((page1_1 as TestPage).testData).toBe(1);
    bufferPoolManager.unpinPage(page1_1.pageId, true);

    // Scenario: Once we have a page, we should be able to read and write content.
    const page1_2 = await bufferPoolManager.fetchPage(
      0,
      new TestPageDeserializer()
    );
    expect(page1_2.pageId).toBe(0);
    expect((page1_2 as TestPage).testData).toBe(1);
    (page1_2 as TestPage).testData = 10;

    // Scenario: We should be able to create new pages until we fill up the buffer pool.
    const page2_1 = await bufferPoolManager.newPage(new TestPageGenerator(2));
    const page3_1 = await bufferPoolManager.newPage(new TestPageGenerator(3));
    expect(page2_1.pageId).toBe(1);
    expect((page2_1 as TestPage).testData).toBe(2);
    expect(page3_1.pageId).toBe(2);
    expect((page3_1 as TestPage).testData).toBe(3);

    // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
    try {
      await bufferPoolManager.newPage(new TestPageGenerator(4));
      fail("Should not reach here");
    } catch (e) {}

    // Scenario: After unpinning pages {0, 1, 2} we should be able to create 4, 5, 6 new pages
    bufferPoolManager.unpinPage(0, true);
    bufferPoolManager.unpinPage(1, true);
    bufferPoolManager.unpinPage(2, true);
    const page4 = await bufferPoolManager.newPage(new TestPageGenerator(4));
    const page5 = await bufferPoolManager.newPage(new TestPageGenerator(5));
    const page6 = await bufferPoolManager.newPage(new TestPageGenerator(6));
    bufferPoolManager.unpinPage(page4.pageId, false);
    bufferPoolManager.unpinPage(page5.pageId, false);
    bufferPoolManager.unpinPage(page6.pageId, false);

    // Scenario: We should be able to fetch the data we wrote a while ago.
    const page1_3 = await bufferPoolManager.fetchPage(
      0,
      new TestPageDeserializer()
    );
    expect((page1_3 as TestPage).testData).toBe(10);
    const page2_2 = await bufferPoolManager.fetchPage(
      1,
      new TestPageDeserializer()
    );
    expect((page2_2 as TestPage).testData).toBe(2);
    const page3_2 = await bufferPoolManager.fetchPage(
      2,
      new TestPageDeserializer()
    );
    expect((page3_2 as TestPage).testData).toBe(3);

    // Cleanup
    await fsp.unlink(testFilePath);
  });
});

class TestPage extends Page {
  constructor(pageId: number, private _testData: number) {
    super(pageId);
  }
  serialize(): ArrayBuffer {
    const buffer = new ArrayBuffer(PAGE_SIZE);
    const dataView = new DataView(buffer);
    dataView.setInt32(0, this.pageId);
    dataView.setInt32(4, this._testData);
    return buffer;
  }
  get testData(): number {
    return this._testData;
  }
  set testData(value: number) {
    this._testData = value;
  }
}
class TestPageDeserializer implements PageDeserializer {
  deserialize(buffer: ArrayBuffer): Page {
    const dataView = new DataView(buffer);
    const pageId = dataView.getInt32(0);
    const testData = dataView.getInt32(4);
    return new TestPage(pageId, testData);
  }
}
class TestPageGenerator implements PageGenerator {
  constructor(private _testData: number) {}
  generate(pageId: number): Page {
    return new TestPage(pageId, this._testData);
  }
}
