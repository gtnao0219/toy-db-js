import { RID } from "../common/RID";
import { IsolationLevel, Transaction, TransactionState } from "./transaction";

export abstract class LockRequest {
  private _granted = false;
  constructor(
    protected _transaction: Transaction,
    protected _lockMode: LockMode
  ) {}
  get transaction(): Transaction {
    return this._transaction;
  }
  get lockMode(): LockMode {
    return this._lockMode;
  }
  get granted(): boolean {
    return this._granted;
  }
  set granted(granted: boolean) {
    this._granted = granted;
  }
}
export class TableLockRequest extends LockRequest {
  constructor(
    protected _transaction: Transaction,
    protected _lockMode: LockMode,
    private _tableOid: number
  ) {
    super(_transaction, _lockMode);
  }
}
export class RowLockRequest extends LockRequest {
  constructor(
    protected _transaction: Transaction,
    protected _lockMode: LockMode,
    private _tableOid: number,
    private _rid: RID
  ) {
    super(_transaction, _lockMode);
  }
}

export abstract class LockRequestQueue {
  private _cv = new Int32Array(new SharedArrayBuffer(4));
  private _upgrading: boolean = false;
  protected _requests: Array<LockRequest> = [];
  async wait(): Promise<void> {
    await Atomics.waitAsync(this._cv, 0, 0).value;
  }
  notifyAll(): void {
    Atomics.notify(this._cv, 0);
  }
  get upgrading(): boolean {
    return this._upgrading;
  }
  set upgrading(upgrading: boolean) {
    this._upgrading = upgrading;
  }
  get requests(): Array<LockRequest> {
    return this._requests;
  }
}
export class TableLockRequestQueue extends LockRequestQueue {
  protected _requests: Array<TableLockRequest> = [];
  get requests(): Array<TableLockRequest> {
    return this._requests;
  }
}
export class RowLockRequestQueue extends LockRequestQueue {
  protected _requests: Array<RowLockRequest> = [];
  get requests(): Array<RowLockRequest> {
    return this._requests;
  }
}

export enum LockMode {
  SHARED,
  EXCLUSIVE,
  INTENTION_SHARED,
  INTENTION_EXCLUSIVE,
  SHARED_INTENTION_EXCLUSIVE,
}

export class LockManager {
  private tableLockMap = new Map<number, TableLockRequestQueue>();
  private rowLockMap = new Map<number, Map<number, RowLockRequestQueue>>();
  async lockTable(
    transaction: Transaction,
    lockMode: LockMode,
    tableOid: number
  ) {
    validateLockMode(transaction, lockMode);
    const queue = this.findOrCreateTableLockRequestQueue(tableOid);

    for (let i = 0; i < queue.requests.length; ++i) {
      const request = queue.requests[i];
      if (request.transaction.transactionId !== transaction.transactionId) {
        continue;
      }
      // already locked
      if (request.lockMode === lockMode) {
        return;
      }
      // upgrade conflict
      if (queue.upgrading) {
        transaction.state = TransactionState.ABORTED;
        throw new Error("UPGRADE_CONFLICT");
      }
      if (!canUpgrade(request.lockMode, lockMode)) {
        transaction.state = TransactionState.ABORTED;
        throw new Error("INCOMPATIBLE_UPGRADE");
      }
      // TODO:
      queue.requests.splice(i, 1);
      request.transaction.removeTableLock(tableOid, request.lockMode);
      const lockRequest = new TableLockRequest(transaction, lockMode, tableOid);
      queue.requests.splice(i, 0, lockRequest);
      queue.upgrading = true;
      while (!canGrantLock(queue, lockRequest)) {
        await queue.wait();
        if (transaction.state === TransactionState.ABORTED) {
          queue.upgrading = false;
          queue.requests.splice(i, 1);
          queue.notifyAll();
        }
      }
      queue.upgrading = false;
      lockRequest.granted = true;
      transaction.addTableLock(tableOid, lockMode);
      if (lockMode !== LockMode.EXCLUSIVE) {
        queue.notifyAll();
      }
    }
    const lockRequest = new TableLockRequest(transaction, lockMode, tableOid);
    queue.requests.push(lockRequest);
    while (!canGrantLock(queue, lockRequest)) {
      await queue.wait();
      if (transaction.state === TransactionState.ABORTED) {
        queue.upgrading = false;
        queue.requests.splice(queue.requests.length - 1, 1);
        queue.notifyAll();
      }
    }
    queue.upgrading = false;
    lockRequest.granted = true;
    transaction.addTableLock(tableOid, lockMode);
    if (lockMode !== LockMode.EXCLUSIVE) {
      queue.notifyAll();
    }
  }
  unlockTable(transaction: Transaction, tableOid: number) {
    const queue = this.tableLockMap.get(tableOid);
    if (queue == null) {
      transaction.state = TransactionState.ABORTED;
      throw new Error("ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD");
    }
    transaction.locks.sharedRowLock.forEach(([oid, _]) => {
      if (oid === tableOid) {
        transaction.state = TransactionState.ABORTED;
        throw new Error("TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS");
      }
    });
    transaction.locks.exclusiveRowLock.forEach(([oid, _]) => {
      if (oid === tableOid) {
        transaction.state = TransactionState.ABORTED;
        throw new Error("TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS");
      }
    });
    for (let i = 0; i < queue.requests.length; ++i) {
      const request = queue.requests[i];
      if (
        request.transaction.transactionId === transaction.transactionId &&
        request.granted
      ) {
        queue.requests.splice(i);
        queue.notifyAll();
        if (mustShrink(transaction, request.lockMode)) {
          transaction.state = TransactionState.SHRINKING;
        }
        request.transaction.removeTableLock(tableOid, request.lockMode);
      }
    }
  }
  async lockRow(
    transaction: Transaction,
    lockMode: LockMode,
    tableOid: number,
    rid: RID
  ) {
    validateLockMode(transaction, lockMode);
    if (
      lockMode === LockMode.EXCLUSIVE &&
      !transaction.isTableIntentionLocked(tableOid)
    ) {
      transaction.state = TransactionState.ABORTED;
      throw new Error("TABLE_LOCK_NOT_PRESENT");
    }
    const queue = this.findOrCreateRowLockRequestQueue(rid);
    for (let i = 0; i < queue.requests.length; ++i) {
      const request = queue.requests[i];
      if (request.transaction.transactionId !== transaction.transactionId) {
        continue;
      }
      // already locked
      if (request.lockMode === lockMode) {
        return;
      }
      // upgrade conflict
      if (queue.upgrading) {
        transaction.state = TransactionState.ABORTED;
        throw new Error("UPGRADE_CONFLICT");
      }
      if (!canUpgrade(request.lockMode, lockMode)) {
        transaction.state = TransactionState.ABORTED;
        throw new Error("INCOMPATIBLE_UPGRADE");
      }
      // TODO:
      queue.requests.splice(i, 1);
      request.transaction.removeTableLock(tableOid, request.lockMode);
      const lockRequest = new RowLockRequest(
        transaction,
        lockMode,
        tableOid,
        rid
      );
      queue.requests.splice(i, 0, lockRequest);
      queue.upgrading = true;
      while (!canGrantLock(queue, lockRequest)) {
        await queue.wait();
        if (transaction.state === TransactionState.ABORTED) {
          queue.upgrading = false;
          queue.requests.splice(i, 1);
          queue.notifyAll();
        }
      }
      queue.upgrading = false;
      lockRequest.granted = true;
      transaction.addTableLock(tableOid, lockMode);
      if (lockMode !== LockMode.EXCLUSIVE) {
        queue.notifyAll();
      }
    }
    const lockRequest = new RowLockRequest(
      transaction,
      lockMode,
      tableOid,
      rid
    );
    queue.requests.push(lockRequest);
    while (!canGrantLock(queue, lockRequest)) {
      await queue.wait();
      if (transaction.state === TransactionState.ABORTED) {
        queue.upgrading = false;
        queue.requests.splice(queue.requests.length - 1, 1);
        queue.notifyAll();
      }
    }
    queue.upgrading = false;
    lockRequest.granted = true;
    transaction.addTableLock(tableOid, lockMode);
    if (lockMode !== LockMode.EXCLUSIVE) {
      queue.notifyAll();
    }
  }
  unlockRow(transaction: Transaction, tableOid: number, rid: RID) {
    const slotIdMap = this.rowLockMap.get(tableOid);
    if (slotIdMap == null) {
      transaction.state = TransactionState.ABORTED;
      throw new Error("ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD");
    }
    const queue = slotIdMap.get(rid.slotId);
    if (queue == null) {
      transaction.state = TransactionState.ABORTED;
      throw new Error("ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD");
    }
    for (let i = 0; i < queue.requests.length; ++i) {
      const request = queue.requests[i];
      if (
        request.transaction.transactionId === transaction.transactionId &&
        request.granted
      ) {
        queue.requests.splice(i);
        queue.notifyAll();
        if (mustShrink(transaction, request.lockMode)) {
          transaction.state = TransactionState.SHRINKING;
        }
        request.transaction.removeTableLock(tableOid, request.lockMode);
      }
    }
  }
  private findOrCreateTableLockRequestQueue(
    tableOid: number
  ): TableLockRequestQueue {
    if (this.tableLockMap.get(tableOid) === undefined) {
      this.tableLockMap.set(tableOid, new TableLockRequestQueue());
    }
    return this.tableLockMap.get(tableOid)!;
  }
  private findOrCreateRowLockRequestQueue(rid: RID): RowLockRequestQueue {
    if (this.rowLockMap.get(rid.pageId) === undefined) {
      this.rowLockMap.set(rid.pageId, new Map());
    }
    const slotIdMap = this.rowLockMap.get(rid.pageId)!;
    if (slotIdMap.get(rid.slotId) === undefined) {
      slotIdMap.set(rid.slotId, new RowLockRequestQueue());
    }
    return slotIdMap.get(rid.slotId)!;
  }
}

function isUnsupportedLockMode(
  isRowLock: boolean,
  lockMode: LockMode,
  isolationLevel: IsolationLevel
): boolean {
  if (isRowLock) {
    switch (lockMode) {
      case LockMode.INTENTION_SHARED:
      case LockMode.INTENTION_EXCLUSIVE:
      case LockMode.SHARED_INTENTION_EXCLUSIVE:
        return true;
    }
  }
  switch (isolationLevel) {
    case IsolationLevel.READ_UNCOMMITTED:
      return (
        lockMode === LockMode.SHARED ||
        lockMode === LockMode.INTENTION_SHARED ||
        lockMode === LockMode.SHARED_INTENTION_EXCLUSIVE
      );
  }
  return false;
}
function isUnsupportedLockModeInShrinking(
  lockMode: LockMode,
  isolationLevel: IsolationLevel
) {
  switch (isolationLevel) {
    case IsolationLevel.READ_UNCOMMITTED:
      return true;
    case IsolationLevel.READ_COMMITTED:
      return (
        lockMode === LockMode.SHARED || lockMode === LockMode.INTENTION_SHARED
      );
    case IsolationLevel.REPEATABLE_READ:
      return true;
  }
}
function validateLockMode(transaction: Transaction, lockMode: LockMode): void {
  // check unsupported lock mode
  if (isUnsupportedLockMode(false, lockMode, transaction.isolationLevel)) {
    transaction.state = TransactionState.ABORTED;
    throw new Error("UNSUPPORTED_LOCK_MODE");
  }
  // check shrinking
  if (
    transaction.state === TransactionState.SHRINKING &&
    isUnsupportedLockModeInShrinking(lockMode, transaction.isolationLevel)
  ) {
    transaction.state = TransactionState.ABORTED;
    throw new Error("SHRINKING");
  }
}

function canUpgrade(current: LockMode, next: LockMode) {
  if (current === next) {
    return true;
  }
  switch (current) {
    case LockMode.INTENTION_SHARED:
      return (
        next === LockMode.SHARED ||
        next === LockMode.INTENTION_EXCLUSIVE ||
        next === LockMode.SHARED_INTENTION_EXCLUSIVE ||
        next === LockMode.EXCLUSIVE
      );
    case LockMode.SHARED:
      return (
        next === LockMode.EXCLUSIVE ||
        next === LockMode.SHARED_INTENTION_EXCLUSIVE
      );
    case LockMode.INTENTION_EXCLUSIVE:
      return (
        next === LockMode.SHARED_INTENTION_EXCLUSIVE ||
        next === LockMode.EXCLUSIVE
      );
    case LockMode.SHARED_INTENTION_EXCLUSIVE:
      return next === LockMode.EXCLUSIVE;
  }
}
function canGrantLock(
  queue: LockRequestQueue,
  lockRequest: LockRequest
): boolean {
  for (let i = 0; i < queue.requests.length; ++i) {
    const currentRequest = queue.requests[i];
    if (currentRequest.granted) {
      switch (currentRequest.lockMode) {
        case LockMode.SHARED:
          if (
            lockRequest.lockMode === LockMode.INTENTION_EXCLUSIVE ||
            lockRequest.lockMode === LockMode.SHARED_INTENTION_EXCLUSIVE ||
            lockRequest.lockMode === LockMode.EXCLUSIVE
          ) {
            return false;
          }
          break;
        case LockMode.EXCLUSIVE:
          return false;
        case LockMode.INTENTION_SHARED:
          if (lockRequest.lockMode === LockMode.EXCLUSIVE) {
            return false;
          }
          break;
        case LockMode.INTENTION_EXCLUSIVE:
          if (
            lockRequest.lockMode === LockMode.SHARED ||
            lockRequest.lockMode === LockMode.SHARED_INTENTION_EXCLUSIVE ||
            lockRequest.lockMode === LockMode.EXCLUSIVE
          ) {
            return false;
          }
          break;
        case LockMode.SHARED_INTENTION_EXCLUSIVE:
          if (
            lockRequest.lockMode === LockMode.SHARED ||
            lockRequest.lockMode === LockMode.EXCLUSIVE ||
            lockRequest.lockMode === LockMode.INTENTION_EXCLUSIVE ||
            lockRequest.lockMode === LockMode.SHARED_INTENTION_EXCLUSIVE
          ) {
            return false;
          }
          break;
      }
    }
  }
  return true;
}
export function mustShrink(
  transaction: Transaction,
  lockMode: LockMode
): boolean {
  if (
    transaction.state === TransactionState.COMMITTED ||
    transaction.state === TransactionState.ABORTED
  ) {
    return false;
  }
  switch (transaction.isolationLevel) {
    case IsolationLevel.READ_UNCOMMITTED:
      return lockMode === LockMode.EXCLUSIVE;
    case IsolationLevel.READ_COMMITTED:
      return lockMode === LockMode.EXCLUSIVE;
    case IsolationLevel.REPEATABLE_READ:
      return lockMode === LockMode.SHARED || lockMode === LockMode.EXCLUSIVE;
  }
}
