import { Schema } from "../../catalog/schema";
import { LockMode } from "../../concurrency/lock_manager";
import { TupleWithRID } from "../../storage/table/table_heap";
import { Tuple } from "../../storage/table/tuple";
import { ExecutorContext } from "../executor_context";
import { evaluate } from "../expression_plan";
import { InsertPlanNode } from "../plan";
import { Executor, ExecutorType } from "./executor";

export class InsertExecutor extends Executor {
  constructor(
    protected _executorContext: ExecutorContext,
    protected _planNode: InsertPlanNode
  ) {
    super(_executorContext, _planNode, ExecutorType.INSERT);
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
      new Tuple(
        tableHeap.schema,
        this._planNode.values.map((v) => {
          // TODO:
          const emptySchema = new Schema([]);
          const emptyTuple = new Tuple(emptySchema, []);
          const evaluated = evaluate(v, emptyTuple, emptySchema);
          return evaluated;
        })
      ),
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
