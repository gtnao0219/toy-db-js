import { BufferPoolManager } from "../../buffer/buffer_pool_manager";
import { Catalog } from "../../catalog/catalog";
import { TableHeap, TupleWithRID } from "../../storage/table/table_heap";
import { UpdatePlanNode } from "../plan/update_plan_node";
import { Executor, ExecutorType } from "./executor";

export class UpdateExecutor extends Executor {
  private tableHeap: TableHeap;
  constructor(
    protected _catalog: Catalog,
    protected _bufferPoolManager: BufferPoolManager,
    private _planNode: UpdatePlanNode,
    private _child: Executor
  ) {
    super(_catalog, _bufferPoolManager, ExecutorType.UPDATE);
    this.tableHeap = this._catalog.getTableHeapByOid(
      this._planNode.table.tableOid
    );
  }
  next(): TupleWithRID | null {
    let tuple: TupleWithRID | null = null;
    while ((tuple = this._child.next()) !== null) {
      const newTuple = tuple.tuple;
      for (const assignment of this._planNode.assignments) {
        newTuple.values[assignment.columnIndex] = assignment.value;
      }
      this.tableHeap.updateTuple(tuple.rid, newTuple);
    }
    return null;
  }
}
