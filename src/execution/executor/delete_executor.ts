import { LockMode } from "../../concurrency/lock_manager";
import { TableHeap, TupleWithRID } from "../../storage/table/table_heap";
import { ExecutorContext } from "../executor_context";
import { DeletePlanNode } from "../plan/delete_plan_node";
import { Executor, ExecutorType } from "./executor";

export class DeleteExecutor extends Executor {
  private tableHeap: TableHeap;
  constructor(
    protected _executorContext: ExecutorContext,
    private _planNode: DeletePlanNode,
    private _child: Executor
  ) {
    super(_executorContext, ExecutorType.INSERT);
    this.tableHeap = this._executorContext.catalog.getTableHeapByOid(
      this._planNode.table.tableOid
    );
  }
  init(): void {
    this._child.init();
    this._executorContext.lockManager.lockTable(
      this._executorContext.transaction,
      LockMode.INTENTION_EXCLUSIVE,
      this._planNode.table.tableOid
    );
  }
  next(): TupleWithRID | null {
    let tuple: TupleWithRID | null = null;
    while ((tuple = this._child.next()) !== null) {
      this.tableHeap.markDelete(tuple.rid, this._executorContext.transaction);
    }
    return null;
  }
}
