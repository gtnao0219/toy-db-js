import { BufferPoolManager } from "../buffer/buffer_pool_manager";
import { Catalog } from "../catalog/catalog";
import { LockManager } from "../concurrency/lock_manager";
import { Transaction } from "../concurrency/transaction";
import { TransactionManager } from "../concurrency/transaction_manager";

export class ExecutorContext {
  constructor(
    private _transaction: Transaction,
    private _bufferPoolManager: BufferPoolManager,
    private _lockManager: LockManager,
    private _transactionManager: TransactionManager,
    private _catalog: Catalog
  ) {}
  get transaction(): Transaction {
    return this._transaction;
  }
  get bufferPoolManager(): BufferPoolManager {
    return this._bufferPoolManager;
  }
  get lockManager(): LockManager {
    return this._lockManager;
  }
  get transactionManager(): TransactionManager {
    return this._transactionManager;
  }
  get catalog(): Catalog {
    return this._catalog;
  }
}
