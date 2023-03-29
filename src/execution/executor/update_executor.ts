import { LockMode } from "../../concurrency/lock_manager";
import { TableHeap, TupleWithRID } from "../../storage/table/table_heap";
import { ExecutorContext } from "../executor_context";
import { UpdatePlanNode } from "../plan/update_plan_node";
import { Executor, ExecutorType } from "./executor";

export class UpdateExecutor extends Executor {
  private tableHeap: TableHeap;
  constructor(
    protected _executorContext: ExecutorContext,
    private _planNode: UpdatePlanNode,
    private _child: Executor
  ) {
    super(_executorContext, ExecutorType.UPDATE);
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
      const newTuple = tuple.tuple;
      for (const assignment of this._planNode.assignments) {
        newTuple.values[assignment.columnIndex] = assignment.value;
      }
      this.tableHeap.updateTuple(
        tuple.rid,
        newTuple,
        this._executorContext.transaction
      );
    }
    return null;
  }
}