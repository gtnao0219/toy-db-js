import { LockMode } from "../../concurrency/lock_manager";
import { TableHeap, TupleWithRID } from "../../storage/table/table_heap";
import { ExecutorContext } from "../executor_context";
import { DeletePlanNode } from "../plan";
import { Executor, ExecutorType } from "./executor";

export class DeleteExecutor extends Executor {
  private tableHeap: TableHeap | null = null;
  constructor(
    protected _executorContext: ExecutorContext,
    protected _planNode: DeletePlanNode,
    private _child: Executor
  ) {
    super(_executorContext, _planNode, ExecutorType.INSERT);
  }
  async init(): Promise<void> {
    await this._child.init();
    this._executorContext.lockManager.lockTable(
      this._executorContext.transaction,
      LockMode.INTENTION_EXCLUSIVE,
      this._planNode.tableOid
    );
    this.tableHeap = await this._executorContext.catalog.getTableHeapByOid(
      this._planNode.tableOid
    );
  }
  async next(): Promise<TupleWithRID | null> {
    if (this.tableHeap === null) {
      throw new Error("table heap is null");
    }
    let tuple: TupleWithRID | null = null;
    while ((tuple = await this._child.next()) !== null) {
      await this._executorContext.lockManager.lockRow(
        this._executorContext.transaction,
        LockMode.EXCLUSIVE,
        this._planNode.tableOid,
        tuple.rid
      );
      await this.tableHeap.markDelete(
        tuple.rid,
        this._executorContext.transaction
      );
    }
    return null;
  }
}
