// import {
//   BPlusTreeLeafPage,
//   BPlusTreeLeafPageDeserializer,
// } from "../../../src/storage/page/b_plus_tree_leaf_page";

// describe("BPlusTreeLeafPage", () => {
//   test("serialize and deserialize", async () => {
//     const page = new BPlusTreeLeafPage(1, 2, 3, [
//       { key: 1, value: { pageId: 4, slotId: 0 } },
//       { key: 2, value: { pageId: 5, slotId: 1 } },
//     ]);
//     const buffer = page.serialize();
//     const dataView = new DataView(buffer);
//     expect(dataView.getInt32(0)).toBe(1);
//     expect(dataView.getInt32(4)).toBe(1);
//     expect(dataView.getInt32(8)).toBe(2);
//     expect(dataView.getInt32(12)).toBe(3);
//     expect(dataView.getInt32(16)).toBe(2);
//     expect(dataView.getInt32(20)).toBe(1);
//     expect(dataView.getInt32(24)).toBe(4);
//     expect(dataView.getInt32(28)).toBe(0);
//     expect(dataView.getInt32(32)).toBe(2);
//     expect(dataView.getInt32(36)).toBe(5);
//     expect(dataView.getInt32(40)).toBe(1);

//     const deserializedPage =
//       await new BPlusTreeLeafPageDeserializer().deserialize(buffer);
//     expect(deserializedPage.pageId).toBe(1);
//     expect(deserializedPage.parentPageId).toBe(2);
//     expect(deserializedPage.nextPageId).toBe(3);
//     expect(deserializedPage.entries).toEqual([
//       { key: 1, value: { pageId: 4, slotId: 0 } },
//       { key: 2, value: { pageId: 5, slotId: 1 } },
//     ]);
//   });
//   test("keyAt", () => {
//     const page = new BPlusTreeLeafPage(1, 2, 3, [
//       { key: 1, value: { pageId: 4, slotId: 0 } },
//       { key: 2, value: { pageId: 5, slotId: 1 } },
//     ]);
//     expect(page.keyAt(0)).toBe(1);
//     expect(page.keyAt(1)).toBe(2);
//   });
//   test("valueAt", () => {
//     const page = new BPlusTreeLeafPage(1, 2, 3, [
//       { key: 1, value: { pageId: 4, slotId: 0 } },
//       { key: 2, value: { pageId: 5, slotId: 1 } },
//     ]);
//     expect(page.valueAt(0)).toEqual({ pageId: 4, slotId: 0 });
//     expect(page.valueAt(1)).toEqual({ pageId: 5, slotId: 1 });
//   });
//   test("keyIndex", () => {
//     const page = new BPlusTreeLeafPage(1, 2, 3, [
//       { key: 1, value: { pageId: 4, slotId: 1 } },
//       { key: 3, value: { pageId: 4, slotId: 3 } },
//       { key: 5, value: { pageId: 4, slotId: 5 } },
//       { key: 5, value: { pageId: 4, slotId: 6 } },
//       { key: 10, value: { pageId: 4, slotId: 10 } },
//     ]);
//     expect(page.keyIndex(0)).toBe(0);
//     expect(page.keyIndex(1)).toBe(0);
//     expect(page.keyIndex(2)).toBe(1);
//     expect(page.keyIndex(5)).toBe(2);
//     expect(page.keyIndex(10)).toBe(4);
//     expect(page.keyIndex(11)).toBe(5);
//   });
//   describe("insert", () => {
//     const entries = [
//       { key: 1, value: { pageId: 4, slotId: 1 } },
//       { key: 3, value: { pageId: 4, slotId: 3 } },
//       { key: 5, value: { pageId: 4, slotId: 5 } },
//       { key: 5, value: { pageId: 4, slotId: 6 } },
//       { key: 10, value: { pageId: 4, slotId: 10 } },
//     ];
//     test("insert not exists", () => {
//       const page = new BPlusTreeLeafPage(1, 2, 3, [...entries]);
//       page.insert(2, { pageId: 5, slotId: 0 });
//       expect(page.entries).toEqual([
//         { key: 1, value: { pageId: 4, slotId: 1 } },
//         { key: 2, value: { pageId: 5, slotId: 0 } },
//         { key: 3, value: { pageId: 4, slotId: 3 } },
//         { key: 5, value: { pageId: 4, slotId: 5 } },
//         { key: 5, value: { pageId: 4, slotId: 6 } },
//         { key: 10, value: { pageId: 4, slotId: 10 } },
//       ]);
//     });
//     test("insert exists", () => {
//       const page = new BPlusTreeLeafPage(1, 2, 3, [...entries]);
//       page.insert(5, { pageId: 5, slotId: 0 });
//       expect(page.entries).toEqual([
//         { key: 1, value: { pageId: 4, slotId: 1 } },
//         { key: 3, value: { pageId: 4, slotId: 3 } },
//         { key: 5, value: { pageId: 5, slotId: 0 } },
//         { key: 5, value: { pageId: 4, slotId: 5 } },
//         { key: 5, value: { pageId: 4, slotId: 6 } },
//         { key: 10, value: { pageId: 4, slotId: 10 } },
//       ]);
//     });
//     test("insert left", () => {
//       const page = new BPlusTreeLeafPage(1, 2, 3, [...entries]);
//       page.insert(0, { pageId: 5, slotId: 0 });
//       expect(page.entries).toEqual([
//         { key: 0, value: { pageId: 5, slotId: 0 } },
//         { key: 1, value: { pageId: 4, slotId: 1 } },
//         { key: 3, value: { pageId: 4, slotId: 3 } },
//         { key: 5, value: { pageId: 4, slotId: 5 } },
//         { key: 5, value: { pageId: 4, slotId: 6 } },
//         { key: 10, value: { pageId: 4, slotId: 10 } },
//       ]);
//     });
//     test("insert right", () => {
//       const page = new BPlusTreeLeafPage(1, 2, 3, [...entries]);
//       page.insert(11, { pageId: 5, slotId: 0 });
//       expect(page.entries).toEqual([
//         { key: 1, value: { pageId: 4, slotId: 1 } },
//         { key: 3, value: { pageId: 4, slotId: 3 } },
//         { key: 5, value: { pageId: 4, slotId: 5 } },
//         { key: 5, value: { pageId: 4, slotId: 6 } },
//         { key: 10, value: { pageId: 4, slotId: 10 } },
//         { key: 11, value: { pageId: 5, slotId: 0 } },
//       ]);
//     });
//   });
//   describe("deleteByKey", () => {
//     const entries = [
//       { key: 1, value: { pageId: 4, slotId: 1 } },
//       { key: 3, value: { pageId: 4, slotId: 3 } },
//       { key: 5, value: { pageId: 4, slotId: 5 } },
//       { key: 5, value: { pageId: 4, slotId: 6 } },
//       { key: 10, value: { pageId: 4, slotId: 10 } },
//     ];
//     test("delete exists", () => {
//       const page = new BPlusTreeLeafPage(1, 2, 3, [...entries]);
//       page.deleteByKey(3);
//       expect(page.entries).toEqual([
//         { key: 1, value: { pageId: 4, slotId: 1 } },
//         { key: 5, value: { pageId: 4, slotId: 5 } },
//         { key: 5, value: { pageId: 4, slotId: 6 } },
//         { key: 10, value: { pageId: 4, slotId: 10 } },
//       ]);
//     });
//     test("delete multiple exists", () => {
//       const page = new BPlusTreeLeafPage(1, 2, 3, [...entries]);
//       page.deleteByKey(5);
//       expect(page.entries).toEqual([
//         { key: 1, value: { pageId: 4, slotId: 1 } },
//         { key: 3, value: { pageId: 4, slotId: 3 } },
//         { key: 10, value: { pageId: 4, slotId: 10 } },
//       ]);
//     });
//     test("delete not exists", () => {
//       const page = new BPlusTreeLeafPage(1, 2, 3, [...entries]);
//       page.deleteByKey(2);
//       expect(page.entries).toEqual([
//         { key: 1, value: { pageId: 4, slotId: 1 } },
//         { key: 3, value: { pageId: 4, slotId: 3 } },
//         { key: 5, value: { pageId: 4, slotId: 5 } },
//         { key: 5, value: { pageId: 4, slotId: 6 } },
//         { key: 10, value: { pageId: 4, slotId: 10 } },
//       ]);
//     });
//   });
//   describe("deleteByKeyValue", () => {
//     const entries = [
//       { key: 1, value: { pageId: 4, slotId: 1 } },
//       { key: 3, value: { pageId: 4, slotId: 3 } },
//       { key: 5, value: { pageId: 4, slotId: 5 } },
//       { key: 5, value: { pageId: 4, slotId: 6 } },
//       { key: 10, value: { pageId: 4, slotId: 10 } },
//     ];
//     test("delete not exists", () => {
//       const page = new BPlusTreeLeafPage(1, 2, 3, [...entries]);
//       page.deleteByKeyValue(2, { pageId: 4, slotId: 3 });
//       expect(page.entries).toEqual([
//         { key: 1, value: { pageId: 4, slotId: 1 } },
//         { key: 3, value: { pageId: 4, slotId: 3 } },
//         { key: 5, value: { pageId: 4, slotId: 5 } },
//         { key: 5, value: { pageId: 4, slotId: 6 } },
//         { key: 10, value: { pageId: 4, slotId: 10 } },
//       ]);
//     });
//     test("delete not exists, but key exists", () => {
//       const page = new BPlusTreeLeafPage(1, 2, 3, [...entries]);
//       page.deleteByKeyValue(2, { pageId: 5, slotId: 7 });
//       expect(page.entries).toEqual([
//         { key: 1, value: { pageId: 4, slotId: 1 } },
//         { key: 3, value: { pageId: 4, slotId: 3 } },
//         { key: 5, value: { pageId: 4, slotId: 5 } },
//         { key: 5, value: { pageId: 4, slotId: 6 } },
//         { key: 10, value: { pageId: 4, slotId: 10 } },
//       ]);
//     });
//     test("delete exists", () => {
//       const page = new BPlusTreeLeafPage(1, 2, 3, [...entries]);
//       page.deleteByKeyValue(3, { pageId: 4, slotId: 3 });
//       expect(page.entries).toEqual([
//         { key: 1, value: { pageId: 4, slotId: 1 } },
//         { key: 5, value: { pageId: 4, slotId: 5 } },
//         { key: 5, value: { pageId: 4, slotId: 6 } },
//         { key: 10, value: { pageId: 4, slotId: 10 } },
//       ]);
//     });
//     test("delete multiple exists", () => {
//       const page = new BPlusTreeLeafPage(1, 2, 3, [...entries]);
//       page.deleteByKeyValue(5, { pageId: 4, slotId: 6 });
//       expect(page.entries).toEqual([
//         { key: 1, value: { pageId: 4, slotId: 1 } },
//         { key: 3, value: { pageId: 4, slotId: 3 } },
//         { key: 5, value: { pageId: 4, slotId: 5 } },
//         { key: 10, value: { pageId: 4, slotId: 10 } },
//       ]);
//     });
//   });
// });
