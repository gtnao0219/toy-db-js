import { BufferPoolManager } from "../../buffer/buffer_pool_manager";
import { Catalog } from "../../catalog/catalog";
import { TupleWithRID } from "../../storage/table/table_heap";

export enum ExecutorType {
  INSERT,
  DELETE,
  UPDATE,
  SEQ_SCAN,
  PROJECTION,
  FILTER,
}
export abstract class Executor {
  constructor(
    protected _catalog: Catalog,
    protected _bufferPoolManager: BufferPoolManager,
    protected _executorType: ExecutorType
  ) {}
  get executorType(): ExecutorType {
    return this._executorType;
  }
  abstract next(): TupleWithRID | null;
}
