import { LockMode } from "../../concurrency/lock_manager";
import { TupleWithRID } from "../../storage/table/table_heap";
import { Tuple } from "../../storage/table/tuple";
import { ExecutorContext } from "../executor_context";
import { InsertPlanNode } from "../plan";
import { Executor, ExecutorType } from "./executor";

export class InsertExecutor extends Executor {
  constructor(
    protected _executorContext: ExecutorContext,
    private _planNode: InsertPlanNode
  ) {
    super(_executorContext, ExecutorType.INSERT);
  }
  async init(): Promise<void> {
    // this._child.init();
    this._executorContext.lockManager.lockTable(
      this._executorContext.transaction,
      LockMode.INTENTION_EXCLUSIVE,
      this._planNode.tableOid
    );
  }
  async next(): Promise<TupleWithRID | null> {
    const tableHeap = await this._executorContext.catalog.getTableHeapByOid(
      this._planNode.tableOid
    );
    const rid = await tableHeap.insertTuple(
      new Tuple(tableHeap.schema, this._planNode.values),
      this._executorContext.transaction
    );
    await this._executorContext.lockManager.lockRow(
      this._executorContext.transaction,
      LockMode.EXCLUSIVE,
      this._planNode.tableOid,
      rid
    );
    return null;
  }
}
