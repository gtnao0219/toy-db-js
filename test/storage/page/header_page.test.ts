import {
  HeaderPage,
  HeaderPageDeserializer,
  HeaderPageGenerator,
} from "../../../src/storage/page/header_page";

describe("HeaderPage", () => {
  test("serialize", () => {
    const headerPage = new HeaderPage(1, 2, [
      { oid: 1, firstPageId: 3 },
      { oid: 2, firstPageId: 4 },
    ]);
    const buffer = headerPage.serialize();
    const dataView = new DataView(buffer);
    expect(dataView.getInt32(0)).toBe(1);
    expect(dataView.getInt32(4)).toBe(2);
    expect(dataView.getInt32(8)).toBe(2);
    expect(dataView.getInt32(12)).toBe(1);
    expect(dataView.getInt32(16)).toBe(3);
    expect(dataView.getInt32(20)).toBe(2);
    expect(dataView.getInt32(24)).toBe(4);
  });
  describe("firstPageId", () => {
    test("found", () => {
      const headerPage = new HeaderPage(1, 2, [
        { oid: 1, firstPageId: 3 },
        { oid: 2, firstPageId: 4 },
      ]);
      expect(headerPage.firstPageId(1)).toBe(3);
      expect(headerPage.firstPageId(2)).toBe(4);
    });
    test("not found", () => {
      const headerPage = new HeaderPage(1, 2, [
        { oid: 1, firstPageId: 3 },
        { oid: 2, firstPageId: 4 },
      ]);
      expect(headerPage.firstPageId(3)).toBeNull();
    });
  });
  describe("insert", () => {
    test("success", () => {
      const headerPage = new HeaderPage(1, 2, [
        { oid: 1, firstPageId: 3 },
        { oid: 2, firstPageId: 4 },
      ]);
      expect(headerPage.insert(3, 5)).toBe(true);
      expect(headerPage.firstPageId(3)).toBe(5);
    });
    test.skip("failure", () => {
      const headerPage = new HeaderPage(1, 2, [
        { oid: 1, firstPageId: 3 },
        { oid: 2, firstPageId: 4 },
      ]);
      expect(headerPage.insert(3, 5)).toBe(true);
      expect(headerPage.insert(6, 7)).toBe(false);
    });
  });
});

describe("HeaderPageDeserializer", () => {
  test("deserialize", async () => {
    const buffer = new ArrayBuffer(28);
    const dataView = new DataView(buffer);
    dataView.setInt32(0, 1);
    dataView.setInt32(4, 2);
    dataView.setInt32(8, 2);
    dataView.setInt32(12, 1);
    dataView.setInt32(16, 3);
    dataView.setInt32(20, 2);
    dataView.setInt32(24, 4);
    const headerPage = await new HeaderPageDeserializer().deserialize(buffer);
    expect(headerPage.pageId).toBe(1);
    expect(headerPage.nextPageId).toBe(2);
    expect(headerPage.entries).toEqual([
      { oid: 1, firstPageId: 3 },
      { oid: 2, firstPageId: 4 },
    ]);
  });
});
describe("HeaderPageGenerator", () => {
  test("generate", () => {
    const headerPage = new HeaderPageGenerator().generate(1);
    expect(headerPage.pageId).toBe(1);
    expect(headerPage.nextPageId).toBe(-1);
    expect(headerPage.entries).toEqual([]);
  });
});
