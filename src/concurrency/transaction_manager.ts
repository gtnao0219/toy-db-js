import { Tuple } from "../storage/table/tuple";
import { LockManager } from "./lock_manager";
import { Transaction, WriteType } from "./transaction";

export class TransactionManager {
  constructor(
    private _lockManager: LockManager,
    private _nextTransactionId: number = 0,
    private _transactionMap: Map<number, Transaction> = new Map()
  ) {}
  begin(): Transaction {
    const transaction = new Transaction(this._nextTransactionId++);
    this._transactionMap.set(transaction.transactionId, transaction);
    return transaction;
  }
  commit(transaction: Transaction) {
    transaction.writeSet.forEach((writeRecord) => {
      switch (writeRecord.writeType) {
        case WriteType.DELETE:
          writeRecord.table.applyDelete(writeRecord.rid, transaction);
          break;
      }
    });
    this.releaseLocks(transaction);
  }
  abort(transaction: Transaction) {
    transaction.writeSet.forEach((writeRecord) => {
      switch (writeRecord.writeType) {
        case WriteType.INSERT:
          writeRecord.table.applyDelete(writeRecord.rid, transaction);
          break;
        case WriteType.DELETE:
          writeRecord.table.rollbackDelete(writeRecord.rid, transaction);
          break;
        case WriteType.UPDATE:
          writeRecord.table.updateTuple(
            writeRecord.rid,
            writeRecord.tuple as Tuple,
            transaction
          );
          break;
      }
    });

    this.releaseLocks(transaction);
  }
  private releaseLocks(transaction: Transaction) {}
}
