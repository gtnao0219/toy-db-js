import { TablePage } from "../page/table_page";
import { TableHeap } from "./table_heap";

export class TableIterator {
  constructor(
    private tableHeap: TableHeap,
    private currentPageId: number | null
  ) {}
  // next(): TablePage | null {
  //   if (this.currentPageId === null) {
  //     return null;
  //   }
  //   const page = this.tableHeap.bufferPoolManager.fetchPage(this.currentPageId);
  //   if (page === null) {
  //     return null;
  //   }
  //   const tablePage = page as TablePage;
  //   this.currentPageId = tablePage.nextPageId;
  //   return tablePage;
  // }
}
