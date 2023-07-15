import { BufferPoolManager } from "../buffer/buffer_pool_manager";
import { Catalog } from "../catalog/catalog";
import { LockManager } from "../concurrency/lock_manager";
import { Transaction } from "../concurrency/transaction";
import { TransactionManager } from "../concurrency/transaction_manager";

export type ExecutorContext = {
  transaction: Transaction;
  bufferPoolManager: BufferPoolManager;
  catalog: Catalog;
  lockManager: LockManager;
  transactionManager: TransactionManager;
};
