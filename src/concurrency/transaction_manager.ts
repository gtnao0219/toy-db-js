import { RID } from "../common/RID";
import { LockManager } from "./lock_manager";
import { Transaction, TransactionState, WriteType } from "./transaction";

export class TransactionManager {
  // TODO: read from storage
  private _nextTransactionId: number = 0;
  private _transactionMap: Map<number, Transaction> = new Map();
  constructor(private _lockManager: LockManager) {}
  getTransaction(transactionId: number): Transaction | null {
    const transaction = this._transactionMap.get(transactionId);
    if (transaction === undefined) {
      return null;
    }
    return transaction;
  }
  begin(): Transaction {
    const transaction = new Transaction(this._nextTransactionId++);
    this._transactionMap.set(transaction.transactionId, transaction);
    return transaction;
  }
  commit(transaction: Transaction) {
    transaction.state = TransactionState.COMMITTED;
    transaction.writeRecords.forEach((writeRecord) => {
      switch (writeRecord.writeType) {
        case WriteType.DELETE:
          writeRecord.tableHeap.applyDelete(writeRecord.rid, transaction);
          break;
      }
    });
    this.releaseLocks(transaction);
  }
  abort(transaction: Transaction) {
    transaction.state = TransactionState.ABORTED;
    transaction.writeRecords.forEach((writeRecord) => {
      switch (writeRecord.writeType) {
        case WriteType.INSERT:
          writeRecord.tableHeap.applyDelete(writeRecord.rid, transaction);
          break;
        case WriteType.DELETE:
          writeRecord.tableHeap.rollbackDelete(writeRecord.rid, transaction);
          break;
        case WriteType.UPDATE:
          if (writeRecord.oldTuple == null) {
            throw new Error("old tuple is null");
          }
          writeRecord.tableHeap.updateTuple(
            writeRecord.rid,
            writeRecord.oldTuple,
            transaction
          );
          break;
      }
    });
    this.releaseLocks(transaction);
  }
  private releaseLocks(transaction: Transaction) {
    // tables
    const tableOids: number[] = [];
    transaction.locks.sharedTableLock.forEach((oid) => {
      tableOids.push(oid);
    });
    transaction.locks.exclusiveTableLock.forEach((oid) => {
      tableOids.push(oid);
    });
    transaction.locks.intentionSharedTableLock.forEach((oid) => {
      tableOids.push(oid);
    });
    transaction.locks.intentionExclusiveTableLock.forEach((oid) => {
      tableOids.push(oid);
    });
    transaction.locks.sharedIntentionExclusiveTableLock.forEach((oid) => {
      tableOids.push(oid);
    });
    // rows
    const rowOidRids: [number, RID][] = [];
    transaction.locks.sharedRowLock.forEach((oidRid) => {
      rowOidRids.push(oidRid);
    });
    transaction.locks.exclusiveRowLock.forEach((oidRid) => {
      rowOidRids.push(oidRid);
    });

    rowOidRids.forEach(([oid, rid]) => {
      this._lockManager.unlockRow(transaction, oid, rid);
    });
    tableOids.forEach((oid) => {
      this._lockManager.unlockTable(transaction, oid);
    });
  }
}
