import { LockMode } from "../../concurrency/lock_manager";
import { TupleWithRID } from "../../storage/table/table_heap";
import { Tuple } from "../../storage/table/tuple";
import { ExecutorContext } from "../executor_context";
import { InsertPlanNode } from "../plan/insert_plan_node";
import { Executor, ExecutorType } from "./executor";

export class InsertExecutor extends Executor {
  constructor(
    protected _executorContext: ExecutorContext,
    private _planNode: InsertPlanNode
  ) {
    super(_executorContext, ExecutorType.INSERT);
  }
  init(): void {
    // this._child.init();
    this._executorContext.lockManager.lockTable(
      this._executorContext.transaction,
      LockMode.INTENTION_EXCLUSIVE,
      this._planNode.table.tableOid
    );
  }
  next(): TupleWithRID | null {
    const tableHeap = this._executorContext.catalog.getTableHeapByOid(
      this._planNode.table.tableOid
    );
    tableHeap.insertTuple(
      new Tuple(tableHeap.schema, this._planNode.values),
      this._executorContext.transaction
    );
    return null;
  }
}
