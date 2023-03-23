import { BufferPoolManager } from "../buffer/buffer_pool_manager";
import { TableHeap } from "../storage/table/table_heap";
import { Schema } from "./schema";

export class TableMetadata {
  constructor(
    private _tableOid: number,
    private _tableName: string,
    private _schema: Schema,
    private _tableHeap: TableHeap
  ) {}
  get tableOid(): number {
    return this._tableOid;
  }
  get tableName(): string {
    return this._tableName;
  }
  get schema(): Schema {
    return this._schema;
  }
  get tableHeap(): TableHeap {
    return this._tableHeap;
  }
}

export class Catalog {
  constructor(private _bufferPoolManager: BufferPoolManager) {}
  createTable(tableName: string, schema: Schema): void {}
  getTableMetadata(tableName: string): TableMetadata | null {
    return null;
  }
}
