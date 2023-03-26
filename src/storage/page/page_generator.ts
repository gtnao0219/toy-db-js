import { HeaderPage } from "./header_page";
import { Page, PageType } from "./page";
import { TablePage } from "./table_page";

export function newEmptyPage(pageId: number, pageType: PageType): Page {
  switch (pageType) {
    case PageType.TABLE_PAGE:
      return new TablePage(pageId);
    case PageType.HEADER_PAGE:
      return new HeaderPage(pageId);
    default:
      throw new Error("invalid page type");
  }
}
