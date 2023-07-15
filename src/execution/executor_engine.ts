import { BufferPoolManager } from "../buffer/buffer_pool_manager";
import { Catalog } from "../catalog/catalog";
import { LockManager } from "../concurrency/lock_manager";
import { TransactionManager } from "../concurrency/transaction_manager";
import { TupleWithRID } from "../storage/table/table_heap";
import { Tuple } from "../storage/table/tuple";
import { ExecutorContext } from "./executor_context";
import { createExecutor } from "./executor_factory";
import { PlanNode } from "./plan";

export class ExecutorEngine {
  constructor(
    private _bufferPoolManager: BufferPoolManager,
    private _catalog: Catalog,
    private _lockManager: LockManager,
    private _transactionManager: TransactionManager
  ) {}
  get bufferPoolManager(): BufferPoolManager {
    return this._bufferPoolManager;
  }
  get catalog(): Catalog {
    return this._catalog;
  }
  get lockManager(): LockManager {
    return this._lockManager;
  }
  get transactionManager(): TransactionManager {
    return this._transactionManager;
  }
  async execute(transactionId: number, plan: PlanNode): Promise<Tuple[]> {
    const transaction = this._transactionManager.getTransaction(transactionId);
    const executorContext: ExecutorContext = {
      transaction,
      bufferPoolManager: this._bufferPoolManager,
      catalog: this._catalog,
      lockManager: this._lockManager,
      transactionManager: this._transactionManager,
    };
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
