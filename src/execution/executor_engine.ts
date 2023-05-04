import { BufferPoolManager } from "../buffer/buffer_pool_manager";
import { Catalog } from "../catalog/catalog";
import { TransactionManager } from "../concurrency/transaction_manager";
import { TupleWithRID } from "../storage/table/table_heap";
import { Tuple } from "../storage/table/tuple";
import { ExecutorContext } from "./executor_context";
import { createExecutor } from "./executor_factory";
import { PlanNode } from "./plan";

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
  async execute(
    executorContext: ExecutorContext,
    plan: PlanNode
  ): Promise<Tuple[]> {
    const executor = createExecutor(executorContext, plan);
    await executor.init();
    const tuples: Tuple[] = [];
    let tuple: TupleWithRID | null = null;
    while ((tuple = await executor.next()) !== null) {
      tuples.push(tuple.tuple);
    }
    return tuples;
  }
}
