import { BufferPoolManager } from "../buffer/buffer_pool_manager";
import { Transaction } from "../concurrency/transaction";
import {
  HeaderPage,
  HeaderPageDeserializer,
  HeaderPageEntry,
  HeaderPageGenerator,
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
  new Column("ordinary_position", Type.INTEGER),
]);

export interface ICatalog {
  getOidByTableName(tableName: string): Promise<number>;
  getSchemaByOid(oid: number): Promise<Schema>;
}

export class Catalog implements ICatalog {
  constructor(private _bufferPoolManager: BufferPoolManager) {}
  // TODO: Refactor
  async nextOid(): Promise<number> {
    const entries = await this.headerPageEntries();
    let maxOid = 0;
    for (const entry of entries) {
      if (entry.oid > maxOid) {
        maxOid = entry.oid;
      }
    }
    return maxOid + 1;
  }
  async initialize(transaction: Transaction): Promise<void> {
    const headerPage = await this._bufferPoolManager.newPage(
      new HeaderPageGenerator()
    );
    this._bufferPoolManager.unpinPage(headerPage.pageId, true);
    const tableInformationSchemaTableHeap = await TableHeap.create(
      this._bufferPoolManager,
      TABLE_INFORMATION_SCHEMA_OID,
      TABLE_INFORMATION_SCHEMA_SCHEMA
    );
    const columnInformationSchemaTableHeap = await TableHeap.create(
      this._bufferPoolManager,
      COLUMN_INFORMATION_SCHEMA_OID,
      COLUMN_INFORMATION_SCHEMA_SCHEMA
    );
    await this.insertHeaderPageEntry(
      TABLE_INFORMATION_SCHEMA_OID,
      tableInformationSchemaTableHeap.firstPageId
    );
    await this.insertHeaderPageEntry(
      COLUMN_INFORMATION_SCHEMA_OID,
      columnInformationSchemaTableHeap.firstPageId
    );
    await this.createTable(
      TABLE_INFORMATION_SCHEMA_NAME,
      TABLE_INFORMATION_SCHEMA_SCHEMA,
      transaction
    );
    await this.createTable(
      COLUMN_INFORMATION_SCHEMA_NAME,
      COLUMN_INFORMATION_SCHEMA_SCHEMA,
      transaction
    );
  }
  async createTable(
    tableName: string,
    schema: Schema,
    transaction: Transaction
  ): Promise<void> {
    const oid = await this.nextOid();
    const tableHeap = await TableHeap.create(
      this._bufferPoolManager,
      oid,
      schema
    );
    await this.insertHeaderPageEntry(oid, tableHeap.firstPageId);
    const tableInfoHeap = await this.tableInformationSchemaTableHeap();
    await tableInfoHeap.insertTuple(
      new Tuple(TABLE_INFORMATION_SCHEMA_SCHEMA, [
        new IntegerValue(oid),
        new VarcharValue(tableName),
      ]),
      transaction
    );
    const columnInfoHeap = await this.columnInformationSchemaTableHeap();
    for (let i = 0; i < schema.columns.length; i++) {
      const column = schema.columns[i];
      await columnInfoHeap.insertTuple(
        new Tuple(COLUMN_INFORMATION_SCHEMA_SCHEMA, [
          new IntegerValue(oid),
          new VarcharValue(column.name),
          new IntegerValue(column.type),
          new IntegerValue(i),
        ]),
        transaction
      );
    }
  }
  async getFirstPageIdByOid(oid: number): Promise<number> {
    const entries = await this.headerPageEntries();
    for (const entry of entries) {
      if (entry.oid === oid) {
        return entry.firstPageId;
      }
    }
    throw new Error("Table not found");
  }
  async getOidByTableName(tableName: string): Promise<number> {
    const heap = await this.tableInformationSchemaTableHeap();
    const tuples = await heap.scan();
    for (const tuple of tuples) {
      if (tuple.tuple.values[1].value === tableName) {
        return tuple.tuple.values[0].value;
      }
    }
    throw new Error("Table not found");
  }
  async getFirstPageIdByTableName(tableName: string): Promise<number> {
    const oid = await this.getOidByTableName(tableName);
    return this.getFirstPageIdByOid(oid);
  }
  async getSchemaByOid(tableOid: number): Promise<Schema> {
    const heap = await this.columnInformationSchemaTableHeap();
    const columns: Column[] = [];
    const tuples = await heap.scan();
    // TODO: sort by execution
    tuples.sort((a, b) => {
      return a.tuple.values[3].value - b.tuple.values[3].value;
    });
    tuples.forEach((tuple) => {
      if (tuple.tuple.values[0].value === tableOid) {
        columns.push(
          new Column(tuple.tuple.values[1].value, tuple.tuple.values[2].value)
        );
      }
    });
    return new Schema(columns);
  }
  async getSchemaByTableName(tableName: string): Promise<Schema> {
    const tableOid = await this.getOidByTableName(tableName);
    return this.getSchemaByOid(tableOid);
  }
  async getTableHeapByOid(tableOid: number): Promise<TableHeap> {
    const firstPageId = await this.getFirstPageIdByOid(tableOid);
    const schema = await this.getSchemaByOid(tableOid);
    return TableHeap.new(
      this._bufferPoolManager,
      tableOid,
      firstPageId,
      schema
    );
  }
  async getTableHeapByTableName(tableName: string): Promise<TableHeap> {
    const oid = await this.getOidByTableName(tableName);
    return this.getTableHeapByOid(oid);
  }
  async headerPageEntries(): Promise<HeaderPageEntry[]> {
    const entries: HeaderPageEntry[] = [];
    let pageId = HEADER_PAGE_ID;
    while (true) {
      if (pageId == INVALID_PAGE_ID) {
        return entries;
      }
      const page = await this._bufferPoolManager.fetchPage(
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
  async insertHeaderPageEntry(oid: number, firstPageId: number): Promise<void> {
    let prevPageId = INVALID_PAGE_ID;
    let pageId = HEADER_PAGE_ID;
    while (true) {
      if (pageId === INVALID_PAGE_ID) {
        const newPage = await this._bufferPoolManager.newPage(
          new HeaderPageGenerator()
        );
        if (!(newPage instanceof HeaderPage)) {
          throw new Error("invalid page type");
        }
        const prevPage = await this._bufferPoolManager.fetchPage(
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

      const page = await this._bufferPoolManager.fetchPage(
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
  async tableInformationSchemaTableHeap(): Promise<TableHeap> {
    const firstPageId = await this.getFirstPageIdByOid(
      TABLE_INFORMATION_SCHEMA_OID
    );
    return TableHeap.new(
      this._bufferPoolManager,
      TABLE_INFORMATION_SCHEMA_OID,
      firstPageId,
      TABLE_INFORMATION_SCHEMA_SCHEMA
    );
  }
  async columnInformationSchemaTableHeap(): Promise<TableHeap> {
    const firstPageId = await this.getFirstPageIdByOid(
      COLUMN_INFORMATION_SCHEMA_OID
    );
    return TableHeap.new(
      this._bufferPoolManager,
      COLUMN_INFORMATION_SCHEMA_OID,
      firstPageId,
      COLUMN_INFORMATION_SCHEMA_SCHEMA
    );
  }
}
