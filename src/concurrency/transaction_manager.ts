import { RID } from "../common/RID";
import { LogManager } from "../recovery/log_manager";
import {
  AbortLogRecord,
  BeginLogRecord,
  CommitLogRecord,
} from "../recovery/log_record";
import { LockManager } from "./lock_manager";
import { Transaction, TransactionState, WriteType } from "./transaction";

export class TransactionManager {
  // TODO: read from storage
  private _transactionMap: Map<number, Transaction> = new Map();
  constructor(
    private _lockManager: LockManager,
    private _logManager: LogManager,
    private _nextTransactionId: number = 0
  ) {}
  set nextTransactionId(nextTransactionId: number) {
    this._nextTransactionId = nextTransactionId;
  }
  getTransaction(transactionId: number): Transaction | null {
    const transaction = this._transactionMap.get(transactionId);
    if (transaction === undefined) {
      return null;
    }
    return transaction;
  }
  async begin(): Promise<Transaction> {
    const transaction = new Transaction(this._nextTransactionId++);
    this._transactionMap.set(transaction.transactionId, transaction);

    const lsn = await this._logManager.appendLogRecord(
      new BeginLogRecord(-1, transaction.prevLsn, transaction.transactionId)
    );
    transaction.prevLsn = lsn;

    return transaction;
  }
  async commit(transaction: Transaction) {
    console.log("commit", transaction.transactionId);
    transaction.state = TransactionState.COMMITTED;
    transaction.writeRecords.forEach((writeRecord) => {
      switch (writeRecord.writeType) {
        case WriteType.DELETE:
          writeRecord.tableHeap.applyDelete(writeRecord.rid, transaction);
          break;
      }
    });

    const lsn = await this._logManager.appendLogRecord(
      new CommitLogRecord(-1, transaction.prevLsn, transaction.transactionId)
    );
    transaction.prevLsn = lsn;
    this._logManager.flush();

    this.releaseLocks(transaction);
  }
  async abort(transaction: Transaction) {
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

    const lsn = await this._logManager.appendLogRecord(
      new AbortLogRecord(-1, transaction.prevLsn, transaction.transactionId)
    );
    transaction.prevLsn = lsn;

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
    console.log("releaseLocks", tableOids, rowOidRids);

    rowOidRids.forEach(([oid, rid]) => {
      this._lockManager.unlockRow(transaction, oid, rid);
    });
    tableOids.forEach((oid) => {
      this._lockManager.unlockTable(transaction, oid);
    });
  }
  toJSON() {
    return {
      nextTransactionId: this._nextTransactionId,
      transactionMap: Array.from(this._transactionMap.entries()).map(
        ([_, transaction]) => {
          return transaction;
        }
      ),
    };
  }
}
