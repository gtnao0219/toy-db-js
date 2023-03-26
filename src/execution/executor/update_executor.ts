import { BufferPoolManager } from "../../buffer/buffer_pool_manager";
import { Catalog } from "../../catalog/catalog";
import { UpdatePlanNode } from "../plan/update_plan_node";
import { Executor, ExecutorType } from "./executor";

export class UpdateExecutor extends Executor {
  constructor(
    protected _catalog: Catalog,
    protected _bufferPoolManager: BufferPoolManager,
    private _planNode: UpdatePlanNode
  ) {
    super(_catalog, _bufferPoolManager, ExecutorType.UPDATE);
  }
  next(): any[][] {
    const tableHeap = this._catalog.getTableHeapByOid(this._planNode.tableOid);
    tableHeap.scan().forEach((tuple, i) => {
      const newTuple = tuple.tuple;
      this._planNode.assignments.forEach((assignment) => {
        newTuple.values[assignment.columnIndex] = assignment.value;
      });
      tableHeap.updateTuple(tuple.rid, newTuple);
    });
    return [];
  }
}
