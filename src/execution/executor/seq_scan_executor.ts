import { LockMode } from "../../concurrency/lock_manager";
import { IsolationLevel } from "../../concurrency/transaction";
import { TupleWithRID } from "../../storage/table/table_heap";
import { ExecutorContext } from "../executor_context";
import { SeqScanPlanNode } from "../plan/seq_scan_plan_node";
import { Executor, ExecutorType } from "./executor";

export class SeqScanExecutor extends Executor {
  // TODO: implement iterator
  private _tuples: TupleWithRID[] = [];
  private _cursor: number = 0;

  constructor(
    protected _executorContext: ExecutorContext,
    private _planNode: SeqScanPlanNode
  ) {
    super(_executorContext, ExecutorType.SEQ_SCAN);
  }
  async init(): Promise<void> {
    this._executorContext.lockManager.lockTable(
      this._executorContext.transaction,
      LockMode.INTENTION_SHARED,
      this._planNode.table.tableOid
    );
    const tableHeap = await this._executorContext.catalog.getTableHeapByOid(
      this._planNode.table.tableOid
    );
    this._tuples = await tableHeap.scan();
  }
  async next(): Promise<TupleWithRID | null> {
    if (this._cursor >= this._tuples.length) {
      if (
        this._executorContext.transaction.isolationLevel ===
        IsolationLevel.READ_COMMITTED
      ) {
        this._executorContext.transaction.locks.sharedRowLock.forEach(
          ([oid, rid]) => {
            this._executorContext.lockManager.unlockRow(
              this._executorContext.transaction,
              oid,
              rid
            );
          }
        );
        this._executorContext.lockManager.unlockTable(
          this._executorContext.transaction,
          this._planNode.table.tableOid
        );
      }
      return null;
    }
    const tuple = this._tuples[this._cursor++];
    if (
      this._executorContext.transaction.isolationLevel !==
      IsolationLevel.READ_UNCOMMITTED
    ) {
      await this._executorContext.lockManager.lockRow(
        this._executorContext.transaction,
        LockMode.SHARED,
        this._planNode.table.tableOid,
        tuple.rid
      );
    }
    return tuple;
  }
}
