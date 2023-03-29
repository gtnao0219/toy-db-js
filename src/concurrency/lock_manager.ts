import { RID } from "../common/RID";
import { Transaction } from "./transaction";

export class TableLockRequest {
  constructor(transaction: Transaction, tableOid: number) {}
}
export class RowLockRequest {
  constructor(transaction: Transaction, tableOid: number, rid: RID) {}
}

export enum LockMode {
  SHARED,
  EXCLUSIVE,
  INTENTION_SHARED,
  INTENTION_EXCLUSIVE,
}

export class LockManager {
  lockTable(transaction: Transaction, lockMode: LockMode, tableOid: number) {}
  unlockTable(transaction: Transaction, lockMode: LockMode, tableOid: number) {}
  lockRow(
    transaction: Transaction,
    lockMode: LockMode,
    tableOid: number,
    rid: RID
  ) {}
  unlockRow(
    transaction: Transaction,
    lockMode: LockMode,
    tableOid: number,
    rid: RID
  ) {}
  grantLock() {}
}
