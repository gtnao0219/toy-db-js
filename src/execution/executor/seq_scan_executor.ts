import { LockMode } from "../../concurrency/lock_manager";
import { TupleWithRID } from "../../storage/table/table_heap";
import { ExecutorContext } from "../executor_context";
import { SeqScanPlanNode } from "../plan/seq_scan_plan_node";
import { Executor, ExecutorType } from "./executor";

export class SeqScanExecutor extends Executor {
  // TODO: implement iterator
  private _tuples: TupleWithRID[];
  private _cursor: number = 0;

  constructor(
    protected _executorContext: ExecutorContext,
    private _planNode: SeqScanPlanNode
  ) {
    super(_executorContext, ExecutorType.SEQ_SCAN);
    const tableHeap = this._executorContext.catalog.getTableHeapByOid(
      this._planNode.table.tableOid
    );
    this._tuples = tableHeap.scan();
  }
  init(): void {
    this._executorContext.lockManager.lockTable(
      this._executorContext.transaction,
      LockMode.INTENTION_SHARED,
      this._planNode.table.tableOid
    );
  }
  next(): TupleWithRID | null {
    if (this._cursor >= this._tuples.length) {
      return null;
    }
    return this._tuples[this._cursor++];
  }
}
