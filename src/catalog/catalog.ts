import { BufferPoolManager } from "../buffer/buffer_pool_manager";
import {
  HeaderPage,
  HeaderPageDeserializer,
  HeaderPageEntry,
} from "../storage/page/header_page";
import { INVALID_PAGE_ID, PageType } from "../storage/page/page";
import { TableHeap } from "../storage/table/table_heap";
import { Tuple } from "../storage/table/tuple";
import { IntegerValue } from "../type/integer_value";
import { Type } from "../type/type";
import { VarcharValue } from "../type/varchar_value";
import { Column } from "./column";
import { Schema } from "./schema";

const HEADER_PAGE_ID = 0;
const TABLE_INFORMATION_SCHEMA_OID = 0;
const COLUMN_INFORMATION_SCHEMA_OID = 1;
const TABLE_INFORMATION_SCHEMA_NAME = "table_information_schema";
const COLUMN_INFORMATION_SCHEMA_NAME = "column_information_schema";
const TABLE_INFORMATION_SCHEMA_SCHEMA = new Schema([
  new Column("oid", Type.INTEGER),
  new Column("name", Type.VARCHAR),
]);
const COLUMN_INFORMATION_SCHEMA_SCHEMA = new Schema([
  new Column("table_oid", Type.INTEGER),
  new Column("name", Type.VARCHAR),
  new Column("type", Type.INTEGER),
]);

export class Catalog {
  constructor(private _bufferPoolManager: BufferPoolManager) {}
  // TODO: Refactor
  nextOid(): number {
    const entries = this.headerPageEntries();
    let maxOid = 0;
    for (const entry of entries) {
      if (entry.oid > maxOid) {
        maxOid = entry.oid;
      }
    }
    return maxOid + 1;
  }
  initialize() {
    const headerPage = this._bufferPoolManager.newPage(PageType.HEADER_PAGE);
    this._bufferPoolManager.unpinPage(headerPage.pageId, true);
    const tableInformationSchemaTableHeap = TableHeap.create(
      this._bufferPoolManager,
      TABLE_INFORMATION_SCHEMA_OID,
      TABLE_INFORMATION_SCHEMA_SCHEMA
    );
    const columnInformationSchemaTableHeap = TableHeap.create(
      this._bufferPoolManager,
      COLUMN_INFORMATION_SCHEMA_OID,
      COLUMN_INFORMATION_SCHEMA_SCHEMA
    );
    this.insertHeaderPageEntry(
      TABLE_INFORMATION_SCHEMA_OID,
      tableInformationSchemaTableHeap.firstPageId
    );
    this.insertHeaderPageEntry(
      COLUMN_INFORMATION_SCHEMA_OID,
      columnInformationSchemaTableHeap.firstPageId
    );
    this.createTable(
      TABLE_INFORMATION_SCHEMA_NAME,
      TABLE_INFORMATION_SCHEMA_SCHEMA
    );
    this.createTable(
      COLUMN_INFORMATION_SCHEMA_NAME,
      COLUMN_INFORMATION_SCHEMA_SCHEMA
    );
  }
  createTable(tableName: string, schema: Schema): void {
    const oid = this.nextOid();
    const tableHeap = TableHeap.create(this._bufferPoolManager, oid, schema);
    this.insertHeaderPageEntry(oid, tableHeap.firstPageId);
    const tableInfoHeap = this.tableInformationSchemaTableHeap();
    tableInfoHeap.insertTuple(
      new Tuple(TABLE_INFORMATION_SCHEMA_SCHEMA, [
        new IntegerValue(oid),
        new VarcharValue(tableName),
      ])
    );
    const columnInfoHeap = this.columnInformationSchemaTableHeap();
    for (const column of schema.columns) {
      columnInfoHeap.insertTuple(
        new Tuple(COLUMN_INFORMATION_SCHEMA_SCHEMA, [
          new IntegerValue(oid),
          new VarcharValue(column.name),
          new IntegerValue(column.type),
        ])
      );
    }
  }
  getFirstPageIdByOid(oid: number): number {
    const entries = this.headerPageEntries();
    for (const entry of entries) {
      if (entry.oid === oid) {
        return entry.firstPageId;
      }
    }
    throw new Error("Table not found");
  }
  getOidByTableName(tableName: string): number {
    const heap = this.tableInformationSchemaTableHeap();
    for (const tuple of heap.scan()) {
      if (tuple.tuple.values[1].value === tableName) {
        return tuple.tuple.values[0].value;
      }
    }
    throw new Error("Table not found");
  }
  getFirstPageIdByTableName(tableName: string): number {
    const oid = this.getOidByTableName(tableName);
    return this.getFirstPageIdByOid(oid);
  }
  getSchemaByOid(tableOid: number): Schema {
    const heap = this.columnInformationSchemaTableHeap();
    const columns: Column[] = [];
    heap.scan().forEach((tuple) => {
      if (tuple.tuple.values[0].value === tableOid) {
        columns.push(
          new Column(tuple.tuple.values[1].value, tuple.tuple.values[2].value)
        );
      }
    });
    return new Schema(columns);
  }
  getSchemaByTableName(tableName: string): Schema {
    const tableOid = this.getOidByTableName(tableName);
    return this.getSchemaByOid(tableOid);
  }
  getTableHeapByOid(tableOid: number): TableHeap {
    const firstPageId = this.getFirstPageIdByOid(tableOid);
    const schema = this.getSchemaByOid(tableOid);
    return TableHeap.new(
      this._bufferPoolManager,
      tableOid,
      firstPageId,
      schema
    );
  }
  getTableHeapByTableName(tableName: string): TableHeap {
    const oid = this.getOidByTableName(tableName);
    return this.getTableHeapByOid(oid);
  }
  headerPageEntries(): HeaderPageEntry[] {
    const entries: HeaderPageEntry[] = [];
    let pageId = HEADER_PAGE_ID;
    while (true) {
      if (pageId == INVALID_PAGE_ID) {
        return entries;
      }
      const page = this._bufferPoolManager.fetchPage(
        pageId,
        new HeaderPageDeserializer()
      );
      if (!(page instanceof HeaderPage)) {
        throw new Error("invalid page type");
      }
      entries.push(...page.entries);
      const prevPageId = pageId;
      pageId = page.nextPageId;
      this._bufferPoolManager.unpinPage(prevPageId, false);
    }
  }
  insertHeaderPageEntry(oid: number, firstPageId: number): void {
    let prevPageId = INVALID_PAGE_ID;
    let pageId = HEADER_PAGE_ID;
    while (true) {
      if (pageId === INVALID_PAGE_ID) {
        const newPage = this._bufferPoolManager.newPage(PageType.HEADER_PAGE);
        if (!(newPage instanceof HeaderPage)) {
          throw new Error("invalid page type");
        }
        const prevPage = this._bufferPoolManager.fetchPage(
          prevPageId,
          new HeaderPageDeserializer()
        );
        if (!(prevPage instanceof HeaderPage)) {
          throw new Error("invalid page type");
        }
        prevPage.nextPageId = newPage.pageId;
        this._bufferPoolManager.unpinPage(prevPageId, true);
        newPage.insert(oid, firstPageId);
        this._bufferPoolManager.unpinPage(newPage.pageId, true);
        return;
      }

      const page = this._bufferPoolManager.fetchPage(
        pageId,
        new HeaderPageDeserializer()
      );
      if (!(page instanceof HeaderPage)) {
        throw new Error("invalid page type");
      }
      if (page.insert(oid, firstPageId)) {
        this._bufferPoolManager.unpinPage(pageId, true);
        return;
      }
      prevPageId = pageId;
      pageId = page.nextPageId;
      this._bufferPoolManager.unpinPage(prevPageId, false);
    }
  }
  tableInformationSchemaTableHeap(): TableHeap {
    const firstPageId = this.getFirstPageIdByOid(TABLE_INFORMATION_SCHEMA_OID);
    return TableHeap.new(
      this._bufferPoolManager,
      TABLE_INFORMATION_SCHEMA_OID,
      firstPageId,
      TABLE_INFORMATION_SCHEMA_SCHEMA
    );
  }
  columnInformationSchemaTableHeap(): TableHeap {
    const firstPageId = this.getFirstPageIdByOid(COLUMN_INFORMATION_SCHEMA_OID);
    return TableHeap.new(
      this._bufferPoolManager,
      COLUMN_INFORMATION_SCHEMA_OID,
      firstPageId,
      COLUMN_INFORMATION_SCHEMA_SCHEMA
    );
  }
}
