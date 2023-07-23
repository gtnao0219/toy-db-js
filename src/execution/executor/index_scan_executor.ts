import { IndexInfo } from "../../catalog/catalog";
import { RID } from "../../common/RID";
import { LockMode } from "../../concurrency/lock_manager";
import { IsolationLevel } from "../../concurrency/transaction";
import { TableHeap, TupleWithRID } from "../../storage/table/table_heap";
import { ExecutorContext } from "../executor_context";
import {
  BinaryOperationExpressionPlanNode,
  LiteralExpressionPlanNode,
} from "../expression_plan";
import { IndexScanPlanNode } from "../plan";
import { Executor, ExecutorType } from "./executor";

export class IndexScanExecutor extends Executor {
  private _rids: RID[] = [];
  private _cursor: number = 0;
  private _tableHeap: TableHeap | null = null;
  private _index: IndexInfo | null = null;

  constructor(
    protected _executorContext: ExecutorContext,
    protected _planNode: IndexScanPlanNode
  ) {
    super(_executorContext, _planNode, ExecutorType.INDEX_SCAN);
  }
  async init(): Promise<void> {
    this._executorContext.lockManager.lockTable(
      this._executorContext.transaction,
      LockMode.INTENTION_SHARED,
      this._planNode.tableOid
    );
    this._index = await this._executorContext.catalog.getIndexByOid(
      this._planNode.indexOid
    );
    const right = (
      this._planNode.condition as BinaryOperationExpressionPlanNode
    ).right as LiteralExpressionPlanNode;
    this._rids = await this._index.index.getValues(right.value as number);
    this._tableHeap = await this._executorContext.catalog.getTableHeapByOid(
      this._planNode.tableOid
    );
  }
  async next(): Promise<TupleWithRID | null> {
    if (this._cursor >= this._rids.length) {
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
          this._planNode.tableOid
        );
      }
      return null;
    }
    const rid = this._rids[this._cursor++];
    if (this._tableHeap === null) {
      throw new Error("table heap is null");
    }
    const tuple = await this._tableHeap.getTuple(rid);
    if (
      this._executorContext.transaction.isolationLevel !==
      IsolationLevel.READ_UNCOMMITTED
    ) {
      await this._executorContext.lockManager.lockRow(
        this._executorContext.transaction,
        LockMode.SHARED,
        this._planNode.tableOid,
        rid
      );
    }
    if (tuple == null) {
      throw new Error(`Tuple not found: ${rid}`);
    }
    return {
      tuple,
      rid,
    };
  }
}
