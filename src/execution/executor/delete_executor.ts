import { BufferPoolManager } from "../../buffer/buffer_pool_manager";
import { Catalog } from "../../catalog/catalog";
import { DeletePlanNode } from "../plan/delete_plan_node";
import { Executor, ExecutorType } from "./executor";

export class DeleteExecutor extends Executor {
  constructor(
    protected _catalog: Catalog,
    protected _bufferPoolManager: BufferPoolManager,
    private _planNode: DeletePlanNode
  ) {
    super(_catalog, _bufferPoolManager, ExecutorType.INSERT);
  }
  next(): any[][] {
    const tableHeap = this._catalog.getTableHeapByOid(this._planNode.tableOid);
    tableHeap.scan().forEach((tuple, i) => {
      tableHeap.deleteTuple(tuple.rid);
    });
    return [];
  }
}
