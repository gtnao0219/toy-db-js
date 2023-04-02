import { RID } from "../common/RID";
import { Debuggable } from "../common/common";
import { TableHeap } from "../storage/table/table_heap";
import { Tuple } from "../storage/table/tuple";
import { LockMode } from "./lock_manager";

export enum TransactionState {
  GROWING,
  SHRINKING,
  COMMITTED,
  ABORTED,
}
export enum IsolationLevel {
  READ_UNCOMMITTED,
  READ_COMMITTED,
  REPEATABLE_READ,
}
export enum WriteType {
  INSERT,
  DELETE,
  UPDATE,
}
export class TransactionWriteRecord implements Debuggable {
  constructor(
    private _writeType: WriteType,
    private _rid: RID,
    private _oldTuple: Tuple | null,
    private _tableHeap: TableHeap
  ) {}
  get writeType(): WriteType {
    return this._writeType;
  }
  get rid(): RID {
    return this._rid;
  }
  get oldTuple(): Tuple | null {
    return this._oldTuple;
  }
  get tableHeap(): TableHeap {
    return this._tableHeap;
  }
  debug(): object {
    return {
      writeType: this._writeType,
      rid: this._rid,
      oldTuple: this._oldTuple,
    };
  }
}

type TransactionLocks = {
  sharedRowLock: [number, RID][];
  exclusiveRowLock: [number, RID][];
  sharedTableLock: number[];
  exclusiveTableLock: number[];
  intentionSharedTableLock: number[];
  intentionExclusiveTableLock: number[];
  sharedIntentionExclusiveTableLock: number[];
};

export class Transaction implements Debuggable {
  private _state: TransactionState = TransactionState.GROWING;
  private _writeRecords: TransactionWriteRecord[] = [];
  private _locks: TransactionLocks = {
    sharedRowLock: [],
    exclusiveRowLock: [],
    sharedTableLock: [],
    exclusiveTableLock: [],
    intentionSharedTableLock: [],
    intentionExclusiveTableLock: [],
    sharedIntentionExclusiveTableLock: [],
  };
  constructor(
    private _transactionId: number,
    private _isolationLevel: IsolationLevel = IsolationLevel.REPEATABLE_READ
  ) {}
  get transactionId(): number {
    return this._transactionId;
  }
  get isolationLevel(): IsolationLevel {
    return this._isolationLevel;
  }
  get state(): TransactionState {
    return this._state;
  }
  set state(state: TransactionState) {
    this._state = state;
  }
  get writeRecords(): TransactionWriteRecord[] {
    return this._writeRecords;
  }
  get locks(): TransactionLocks {
    return this._locks;
  }
  addWriteRecord(
    writeType: WriteType,
    rid: RID,
    tuple: Tuple | null,
    table: TableHeap
  ): void {
    this._writeRecords.push(
      new TransactionWriteRecord(writeType, rid, tuple, table)
    );
  }
  addTableLock(tableOid: number, lockMode: LockMode): void {
    console.log("add table lock", tableOid, lockMode);
    switch (lockMode) {
      case LockMode.SHARED:
        if (this.locks.sharedTableLock.includes(tableOid)) {
          return;
        }
        this.locks.sharedTableLock.push(tableOid);
        return;
      case LockMode.EXCLUSIVE:
        if (this.locks.exclusiveTableLock.includes(tableOid)) {
          return;
        }
        this.locks.exclusiveTableLock.push(tableOid);
        return;
      case LockMode.INTENTION_SHARED:
        if (this.locks.intentionSharedTableLock.includes(tableOid)) {
          return;
        }
        this.locks.intentionSharedTableLock.push(tableOid);
        return;
      case LockMode.INTENTION_EXCLUSIVE:
        if (this.locks.intentionExclusiveTableLock.includes(tableOid)) {
          return;
        }
        this.locks.intentionExclusiveTableLock.push(tableOid);
        return;
      case LockMode.SHARED_INTENTION_EXCLUSIVE:
        if (this.locks.sharedIntentionExclusiveTableLock.includes(tableOid)) {
          return;
        }
        this.locks.sharedIntentionExclusiveTableLock.push(tableOid);
        return;
    }
  }
  removeTableLock(tableOid: number, lockMode: LockMode): void {
    console.log("remove table lock", tableOid, lockMode);
    switch (lockMode) {
      case LockMode.SHARED:
        this.locks.sharedTableLock = this.locks.sharedTableLock.filter(
          (oid) => oid !== tableOid
        );
        return;
      case LockMode.EXCLUSIVE:
        this.locks.exclusiveTableLock = this.locks.exclusiveTableLock.filter(
          (oid) => oid !== tableOid
        );
        return;
      case LockMode.INTENTION_SHARED:
        this.locks.intentionSharedTableLock =
          this.locks.intentionSharedTableLock.filter((oid) => oid !== tableOid);
        return;
      case LockMode.INTENTION_EXCLUSIVE:
        this.locks.intentionExclusiveTableLock =
          this.locks.intentionExclusiveTableLock.filter(
            (oid) => oid !== tableOid
          );
        return;
      case LockMode.SHARED_INTENTION_EXCLUSIVE:
        this.locks.sharedIntentionExclusiveTableLock =
          this.locks.sharedIntentionExclusiveTableLock.filter(
            (oid) => oid !== tableOid
          );
        return;
    }
  }
  addRowLock(tableOid: number, rid: RID, lockMode: LockMode): void {
    console.log("add row lock", tableOid, rid, lockMode);
    switch (lockMode) {
      case LockMode.SHARED:
        if (
          this.locks.sharedRowLock.find(
            (lock) =>
              lock[0] === tableOid &&
              lock[1].pageId === rid.pageId &&
              lock[1].slotId === rid.slotId
          )
        ) {
          return;
        }
        this.locks.sharedRowLock.push([tableOid, rid]);
        return;
      case LockMode.EXCLUSIVE:
        if (
          this.locks.exclusiveRowLock.find(
            (lock) =>
              lock[0] === tableOid &&
              lock[1].pageId === rid.pageId &&
              lock[1].slotId === rid.slotId
          )
        ) {
          return;
        }
        this.locks.exclusiveRowLock.push([tableOid, rid]);
        return;
    }
  }
  removeRowLock(tableOid: number, rid: RID, lockMode: LockMode): void {
    console.log("remove row lock", tableOid, rid, lockMode);
    switch (lockMode) {
      case LockMode.SHARED:
        this.locks.sharedRowLock = this.locks.sharedRowLock.filter(
          ([oid, r]) =>
            oid !== tableOid ||
            r.pageId !== rid.pageId ||
            r.slotId !== rid.slotId
        );
        return;
      case LockMode.EXCLUSIVE:
        this.locks.exclusiveRowLock = this.locks.exclusiveRowLock.filter(
          ([oid, r]) =>
            oid !== tableOid ||
            r.pageId !== rid.pageId ||
            r.slotId !== rid.slotId
        );
        return;
    }
  }
  isTableIntentionLocked(tableOid: number): boolean {
    return (
      this.locks.intentionSharedTableLock.includes(tableOid) ||
      this.locks.intentionExclusiveTableLock.includes(tableOid) ||
      this.locks.sharedIntentionExclusiveTableLock.includes(tableOid)
    );
  }
  debug(): object {
    return {
      transactionId: this.transactionId,
      isolationLevel: this.isolationLevel,
      state: this.state,
      writeRecords: this.writeRecords.map((wr) => wr.debug()),
      locks: this.locks,
    };
  }
}
