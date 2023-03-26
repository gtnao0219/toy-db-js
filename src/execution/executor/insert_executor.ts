import { BufferPoolManager } from "../../buffer/buffer_pool_manager";
import { Catalog } from "../../catalog/catalog";
import { Tuple } from "../../storage/table/tuple";
import { InsertPlanNode } from "../plan/insert_plan_node";
import { Executor, ExecutorType } from "./executor";

export class InsertExecutor extends Executor {
  constructor(
    protected _catalog: Catalog,
    protected _bufferPoolManager: BufferPoolManager,
    private _planNode: InsertPlanNode
  ) {
    super(_catalog, _bufferPoolManager, ExecutorType.INSERT);
  }
  next(): any[][] {
    const tableHeap = this._catalog.getTableHeapByOid(this._planNode.tableOid);
    tableHeap.insertTuple(new Tuple(tableHeap.schema, this._planNode.values));
    return [];
  }
}
