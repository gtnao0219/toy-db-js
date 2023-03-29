import { RID } from "../common/RID";
import { TableHeap } from "../storage/table/table_heap";
import { Tuple } from "../storage/table/tuple";

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
  SERIALIZABLE,
}
export enum WriteType {
  INSERT,
  DELETE,
  UPDATE,
}
export class TransactionWriteRecord {
  constructor(
    private _writeType: WriteType,
    private _rid: RID,
    private _tuple: Tuple | null,
    private _table: TableHeap
  ) {}
  get writeType(): WriteType {
    return this._writeType;
  }
  get rid(): RID {
    return this._rid;
  }
  get tuple(): Tuple | null {
    return this._tuple;
  }
  get table(): TableHeap {
    return this._table;
  }
}

export class Transaction {
  constructor(
    private _transactionId: number,
    private _isolationLevel: IsolationLevel = IsolationLevel.REPEATABLE_READ,
    private _writeSet: TransactionWriteRecord[] = []
  ) {}
  get transactionId(): number {
    return this._transactionId;
  }
  get writeSet(): TransactionWriteRecord[] {
    return this._writeSet;
  }
  addWriteSet(
    writeType: WriteType,
    rid: RID,
    tuple: Tuple | null,
    table: TableHeap
  ): void {
    this._writeSet.push(
      new TransactionWriteRecord(writeType, rid, tuple, table)
    );
  }
}
