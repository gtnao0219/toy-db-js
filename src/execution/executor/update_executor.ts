import { LockMode } from "../../concurrency/lock_manager";
import { TableHeap, TupleWithRID } from "../../storage/table/table_heap";
import { ExecutorContext } from "../executor_context";
import { UpdatePlanNode } from "../plan";
import { Executor, ExecutorType } from "./executor";

export class UpdateExecutor extends Executor {
  private tableHeap: TableHeap | null = null;
  constructor(
    protected _executorContext: ExecutorContext,
    private _planNode: UpdatePlanNode,
    private _child: Executor
  ) {
    super(_executorContext, ExecutorType.UPDATE);
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
    if (this.tableHeap == null) {
      throw new Error("tableHeap is null");
    }
    let tuple: TupleWithRID | null = null;
    while ((tuple = await this._child.next()) !== null) {
      const newTuple = tuple.tuple;
      for (const assignment of this._planNode.assignments) {
        newTuple.values[assignment.columnIndex] = assignment.value;
      }
      await this._executorContext.lockManager.lockRow(
        this._executorContext.transaction,
        LockMode.EXCLUSIVE,
        this._planNode.tableOid,
        tuple.rid
      );
      this.tableHeap.updateTuple(
        tuple.rid,
        newTuple,
        this._executorContext.transaction
      );
    }
    return null;
  }
}
