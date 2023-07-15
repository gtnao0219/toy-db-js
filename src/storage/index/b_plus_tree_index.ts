import { BufferPoolManager } from "../../buffer/buffer_pool_manager";
import { RID } from "../../common/RID";
import { BPlusTreeLeafPage } from "../page/b_plus_tree_leaf_page";
import { INVALID_PAGE_ID } from "../page/page";

// export class BPlusTreeIndex {
//   constructor(
//     private _bufferPoolManager: BufferPoolManager,
//     private _rootPageId: number = INVALID_PAGE_ID
//   ) {}
//   isEmpty(): boolean {
//     return this._rootPageId === INVALID_PAGE_ID;
//   }
//   async getValues(key: number): Promise<RID[]> {
//     let leafPage = await this.findLeafPage(key);
//     const values: RID[] = [];
//     while (leafPage !== null) {
//       if (keyIndex < leafPage.getKeyCount()) {
//         const value = leafPage.getValue(keyIndex);
//         values.push(value);
//       }
//       leafPage = await leafPage.getNextPageId();
//     }
//   }
//   private async findLeafPage(key: number): Promise<BPlusTreeLeafPage> {
//     let pageId = this._rootPageId;
//     while (true) {
//       const page = this._bufferPoolManager.fetchPage(pageId);
//       if (page.isLeafPage()) {
//         return page;
//       }
//       pageId = page.findChildPageId(key);
//       this._bufferPoolManager.unpinPage(pageId);
//     }
//   }
// }
