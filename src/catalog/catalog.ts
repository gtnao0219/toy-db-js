import { BufferPoolManager } from "../buffer/buffer_pool_manager";
import { Transaction } from "../concurrency/transaction";
import { TransactionManager } from "../concurrency/transaction_manager";
import { LogManager } from "../recovery/log_manager";
import {
  HeaderPage,
  HeaderPageDeserializer,
  HeaderPageEntry,
  HeaderPageGenerator,
} from "../storage/page/header_page";
import { INVALID_PAGE_ID } from "../storage/page/page";
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
const INDEX_INFORMATION_SCHEMA_OID = 2;
const TABLE_INFORMATION_SCHEMA_NAME = "table_information_schema";
const COLUMN_INFORMATION_SCHEMA_NAME = "column_information_schema";
const INDEX_INFORMATION_SCHEMA_NAME = "index_information_schema";
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
const INDEX_INFORMATION_SCHEMA_SCHEMA = new Schema([
  new Column("oid", Type.INTEGER),
  new Column("name", Type.VARCHAR),
  new Column("table_oid", Type.INTEGER),
  new Column("column_name", Type.VARCHAR),
]);

export type IndexInfo = {
  oid: number;
  name: string;
  tableOid: number;
  columnName: string;
  // index: Index
};

export interface Catalog {
  bootstrap(isEmpty: boolean): Promise<void>;
  createTable(
    tableName: string,
    schema: Schema,
    transaction: Transaction
  ): Promise<void>;
  getOidByTableName(tableName: string): Promise<number>;
  getSchemaByOid(oid: number): Promise<Schema>;
  getTableHeapByOid(oid: number): Promise<TableHeap>;
  getIndexesByOid(oid: number): Promise<IndexInfo[]>;
}

export class CatalogImpl implements Catalog {
  private _nextOid: number = 0;
  constructor(
    private _bufferPoolManager: BufferPoolManager,
    private _logManager: LogManager,
    private _transactionManager: TransactionManager
  ) {}
  async bootstrap(isEmpty: boolean): Promise<void> {
    if (isEmpty) {
      await this.createSystemPages();
    } else {
      await this.setupNextOid();
    }
  }
  async createSystemPages(): Promise<void> {
    const headerPage = await this._bufferPoolManager.newPage(
      new HeaderPageGenerator()
    );
    this._bufferPoolManager.unpinPage(headerPage.pageId, true);
    const tableInformationSchemaTableHeap = await TableHeap.create(
      this._bufferPoolManager,
      this._logManager,
      TABLE_INFORMATION_SCHEMA_OID,
      TABLE_INFORMATION_SCHEMA_SCHEMA
    );
    const columnInformationSchemaTableHeap = await TableHeap.create(
      this._bufferPoolManager,
      this._logManager,
      COLUMN_INFORMATION_SCHEMA_OID,
      COLUMN_INFORMATION_SCHEMA_SCHEMA
    );
    const indexInformationSchemaTableHeap = await TableHeap.create(
      this._bufferPoolManager,
      this._logManager,
      INDEX_INFORMATION_SCHEMA_OID,
      INDEX_INFORMATION_SCHEMA_SCHEMA
    );
    await this.insertHeaderPageEntry(
      TABLE_INFORMATION_SCHEMA_OID,
      tableInformationSchemaTableHeap.firstPageId
    );
    await this.insertHeaderPageEntry(
      COLUMN_INFORMATION_SCHEMA_OID,
      columnInformationSchemaTableHeap.firstPageId
    );
    await this.insertHeaderPageEntry(
      INDEX_INFORMATION_SCHEMA_OID,
      indexInformationSchemaTableHeap.firstPageId
    );
    await this.setupNextOid();

    const transaction = await this._transactionManager.begin();
    await this.createTable(
      TABLE_INFORMATION_SCHEMA_NAME,
      TABLE_INFORMATION_SCHEMA_SCHEMA,
      transaction,
      TABLE_INFORMATION_SCHEMA_OID
    );
    await this.createTable(
      COLUMN_INFORMATION_SCHEMA_NAME,
      COLUMN_INFORMATION_SCHEMA_SCHEMA,
      transaction,
      COLUMN_INFORMATION_SCHEMA_OID
    );
    await this.createTable(
      INDEX_INFORMATION_SCHEMA_NAME,
      INDEX_INFORMATION_SCHEMA_SCHEMA,
      transaction,
      INDEX_INFORMATION_SCHEMA_OID
    );
    await this._transactionManager.commit(transaction);
  }
  private async setupNextOid(): Promise<void> {
    const entries = await this.headerPageEntries();
    let maxOid = -1;
    for (const entry of entries) {
      if (entry.oid > maxOid) {
        maxOid = entry.oid;
      }
    }
    this._nextOid = maxOid + 1;
  }
  async createTable(
    tableName: string,
    schema: Schema,
    transaction: Transaction,
    systemOid: number | null = null
  ): Promise<void> {
    const oid = systemOid == null ? this.iterateOid() : systemOid;
    const tableHeap = await TableHeap.create(
      this._bufferPoolManager,
      this._logManager,
      oid,
      schema
    );
    if (systemOid == null) {
      await this.insertHeaderPageEntry(oid, tableHeap.firstPageId);
    }
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
  async createIndex(
    indexName: string,
    tableName: string,
    columnName: string,
    transaction: Transaction
  ): Promise<void> {
    const tableOid = await this.getOidByTableName(tableName);
    const schema = await this.getSchemaByOid(tableOid);
    const column = schema.columns.find((c) => c.name === columnName);
    if (column == null) {
      throw new Error(`column ${columnName} not found`);
    }
    const oid = this.iterateOid();
    const indexInfoHeap = await this.indexInformationSchemaTableHeap();
    await indexInfoHeap.insertTuple(
      new Tuple(INDEX_INFORMATION_SCHEMA_SCHEMA, [
        new IntegerValue(oid),
        new VarcharValue(indexName),
        new IntegerValue(tableOid),
        new VarcharValue(columnName),
      ]),
      transaction
    );
    // TODO: rows have been already inserted into the table, so we need to build the index
  }
  private iterateOid(): number {
    return this._nextOid++;
  }
  private async getFirstPageIdByOid(oid: number): Promise<number> {
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
  private async getFirstPageIdByTableName(tableName: string): Promise<number> {
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
  private async getSchemaByTableName(tableName: string): Promise<Schema> {
    const tableOid = await this.getOidByTableName(tableName);
    return this.getSchemaByOid(tableOid);
  }
  async getTableHeapByOid(tableOid: number): Promise<TableHeap> {
    const firstPageId = await this.getFirstPageIdByOid(tableOid);
    const schema = await this.getSchemaByOid(tableOid);
    return TableHeap.new(
      this._bufferPoolManager,
      this._logManager,
      tableOid,
      firstPageId,
      schema
    );
  }
  private async getTableHeapByTableName(tableName: string): Promise<TableHeap> {
    const oid = await this.getOidByTableName(tableName);
    return this.getTableHeapByOid(oid);
  }
  async getIndexesByOid(tableOid: number): Promise<IndexInfo[]> {
    const heap = await this.indexInformationSchemaTableHeap();
    const tuples = await heap.scan();
    return tuples
      .filter((tuple) => {
        return tuple.tuple.values[2].value === tableOid;
      })
      .map((tuple) => {
        return {
          oid: tuple.tuple.values[0].value,
          name: tuple.tuple.values[1].value,
          tableOid: tuple.tuple.values[2].value,
          columnName: tuple.tuple.values[3].value,
        };
      });
  }
  private async headerPageEntries(): Promise<HeaderPageEntry[]> {
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
  private async insertHeaderPageEntry(
    oid: number,
    firstPageId: number
  ): Promise<void> {
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
  private async tableInformationSchemaTableHeap(): Promise<TableHeap> {
    const firstPageId = await this.getFirstPageIdByOid(
      TABLE_INFORMATION_SCHEMA_OID
    );
    return TableHeap.new(
      this._bufferPoolManager,
      this._logManager,
      TABLE_INFORMATION_SCHEMA_OID,
      firstPageId,
      TABLE_INFORMATION_SCHEMA_SCHEMA
    );
  }
  private async columnInformationSchemaTableHeap(): Promise<TableHeap> {
    const firstPageId = await this.getFirstPageIdByOid(
      COLUMN_INFORMATION_SCHEMA_OID
    );
    return TableHeap.new(
      this._bufferPoolManager,
      this._logManager,
      COLUMN_INFORMATION_SCHEMA_OID,
      firstPageId,
      COLUMN_INFORMATION_SCHEMA_SCHEMA
    );
  }
  private async indexInformationSchemaTableHeap(): Promise<TableHeap> {
    const firstPageId = await this.getFirstPageIdByOid(
      INDEX_INFORMATION_SCHEMA_OID
    );
    return TableHeap.new(
      this._bufferPoolManager,
      this._logManager,
      INDEX_INFORMATION_SCHEMA_OID,
      firstPageId,
      INDEX_INFORMATION_SCHEMA_SCHEMA
    );
  }
}
