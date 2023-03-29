import { BufferPoolManager } from "../buffer/buffer_pool_manager";
import { Catalog } from "../catalog/catalog";
import { TransactionManager } from "../concurrency/transaction_manager";
import { TupleWithRID } from "../storage/table/table_heap";
import { Tuple } from "../storage/table/tuple";
import { ExecutorContext } from "./executor_context";
import { createExecutor } from "./executor_factory";
import { PlanNode } from "./plan/plan_node";

export class ExecutorEngine {
  constructor(
    private _bufferPoolManager: BufferPoolManager,
    private _transactionManager: TransactionManager,
    private _catalog: Catalog
  ) {}
  get bufferPoolManager(): BufferPoolManager {
    return this._bufferPoolManager;
  }
  get transactionManager(): TransactionManager {
    return this._transactionManager;
  }
  get catalog(): Catalog {
    return this._catalog;
  }
  execute(executorContext: ExecutorContext, plan: PlanNode): Tuple[] {
    const executor = createExecutor(executorContext, plan);
    executor.init();
    const tuples: Tuple[] = [];
    let tuple: TupleWithRID | null = null;
    while ((tuple = executor.next()) !== null) {
      tuples.push(tuple.tuple);
    }
    return tuples;
  }
}
