import { RID } from "../common/RID";

const HEADER_SIZE = 20;

export enum LogRecordType {
  INSERT,
  MARK_DELETE,
  APPLY_DELETE,
  ROLLBACK_DELETE,
  UPDATE,
  BEGIN,
  COMMIT,
  ABORT,
}

export abstract class LogRecord {
  constructor(
    protected _lsn: number,
    protected _prevLsn: number,
    protected _transactionId: number,
    protected _type: LogRecordType
  ) {}
  get lsn(): number {
    return this._lsn;
  }
  set lsn(lsn: number) {
    this._lsn = lsn;
  }
  get prevLsn(): number {
    return this._prevLsn;
  }
  get transactionId(): number {
    return this._transactionId;
  }
  get type(): LogRecordType {
    return this._type;
  }
  serialize(): ArrayBuffer {
    const buffer = new ArrayBuffer(HEADER_SIZE);
    const view = new DataView(buffer);
    view.setInt32(0, HEADER_SIZE);
    view.setInt32(4, this._lsn);
    view.setInt32(8, this._prevLsn);
    view.setInt32(12, this._transactionId);
    view.setInt32(16, this._type);
    return buffer;
  }
  static deserialize(buffer: ArrayBuffer): LogRecord {
    const view = new DataView(buffer);
    const lsn = view.getInt32(4);
    const prevLsn = view.getInt32(8);
    const transactionId = view.getInt32(12);
    const type = view.getInt32(16);

    switch (type) {
      case LogRecordType.BEGIN:
        return new BeginLogRecord(lsn, prevLsn, transactionId);
      case LogRecordType.COMMIT:
        return new CommitLogRecord(lsn, prevLsn, transactionId);
      case LogRecordType.ABORT:
        return new AbortLogRecord(lsn, prevLsn, transactionId);
      case LogRecordType.INSERT:
        return InsertLogRecord.deserialize(buffer);
      case LogRecordType.UPDATE:
        return UpdateLogRecord.deserialize(buffer);
      case LogRecordType.MARK_DELETE:
        return MarkDeleteLogRecord.deserialize(buffer);
      case LogRecordType.ROLLBACK_DELETE:
        return RollbackDeleteLogRecord.deserialize(buffer);
      case LogRecordType.APPLY_DELETE:
        return ApplyDeleteLogRecord.deserialize(buffer);
      default:
        throw new Error("Unknown log record type");
    }
  }
}

export class InsertLogRecord extends LogRecord {
  constructor(
    protected _lsn: number,
    protected _prevLsn: number,
    protected _transactionId: number,
    private _rid: RID,
    private _tupleData: ArrayBuffer
  ) {
    super(_lsn, _prevLsn, _transactionId, LogRecordType.INSERT);
  }
  get rid(): RID {
    return this._rid;
  }
  get tupleData(): ArrayBuffer {
    return this._tupleData;
  }
  serialize(): ArrayBuffer {
    const buffer = new ArrayBuffer(
      HEADER_SIZE + 8 + this._tupleData.byteLength
    );
    const view = new DataView(buffer);
    view.setInt32(0, HEADER_SIZE + 8 + this._tupleData.byteLength);
    view.setInt32(4, this._lsn);
    view.setInt32(8, this._prevLsn);
    view.setInt32(12, this._transactionId);
    view.setInt32(16, this._type);
    view.setInt32(20, this._rid.pageId);
    view.setInt32(24, this._rid.slotId);
    const data = new Uint8Array(this._tupleData);
    const dataView = new DataView(buffer, 28);
    data.forEach((byte, i) => dataView.setUint8(i, byte));
    return buffer;
  }
  static deserialize(buffer: ArrayBuffer): InsertLogRecord {
    const view = new DataView(buffer);
    const lsn = view.getInt32(4);
    const prevLsn = view.getInt32(8);
    const transactionId = view.getInt32(12);
    const pageId = view.getInt32(20);
    const slotId = view.getInt32(24);
    const tupleSize = view.byteLength - 28;
    const tupleData = new Uint8Array(tupleSize);
    const tupleDataView = new DataView(buffer, 28);
    tupleData.forEach((_, i) => (tupleData[i] = tupleDataView.getUint8(i)));
    return new InsertLogRecord(
      lsn,
      prevLsn,
      transactionId,
      { pageId, slotId },
      tupleData.buffer
    );
  }
  toJSON() {
    return {
      lsn: this._lsn,
      prevLsn: this._prevLsn,
      transactionId: this._transactionId,
      type: this._type,
      rid: this._rid,
      tupleData: this._tupleData,
    };
  }
}
export class UpdateLogRecord extends LogRecord {
  constructor(
    protected _lsn: number,
    protected _prevLsn: number,
    protected _transactionId: number,
    private _rid: RID,
    private _oldTupleData: ArrayBuffer,
    private _newTupleData: ArrayBuffer
  ) {
    super(_lsn, _prevLsn, _transactionId, LogRecordType.UPDATE);
  }
  get rid(): RID {
    return this._rid;
  }
  get oldTupleData(): ArrayBuffer {
    return this._oldTupleData;
  }
  get newTupleData(): ArrayBuffer {
    return this._newTupleData;
  }
  serialize(): ArrayBuffer {
    const buffer = new ArrayBuffer(
      HEADER_SIZE +
        8 +
        this._oldTupleData.byteLength +
        this._newTupleData.byteLength
    );
    const view = new DataView(buffer);
    view.setInt32(
      0,
      HEADER_SIZE +
        8 +
        this._oldTupleData.byteLength +
        this._newTupleData.byteLength
    );
    view.setInt32(4, this._lsn);
    view.setInt32(8, this._prevLsn);
    view.setInt32(12, this._transactionId);
    view.setInt32(16, this._type);
    view.setInt32(20, this._rid.pageId);
    view.setInt32(24, this._rid.slotId);
    const oldTupleData = new Uint8Array(this._oldTupleData);
    const oldTupleDataView = new DataView(buffer, 28);
    oldTupleData.forEach((byte, i) => oldTupleDataView.setUint8(i, byte));
    const newTupleData = new Uint8Array(this._newTupleData);
    const newTupleDataView = new DataView(buffer, 28 + oldTupleData.byteLength);
    newTupleData.forEach((byte, i) => newTupleDataView.setUint8(i, byte));
    return buffer;
  }
  static deserialize(buffer: ArrayBuffer): UpdateLogRecord {
    const view = new DataView(buffer);
    const lsn = view.getInt32(4);
    const prevLsn = view.getInt32(8);
    const transactionId = view.getInt32(12);
    const pageId = view.getInt32(20);
    const slotId = view.getInt32(24);
    const oldTupleSize = view.byteLength - 28;
    const oldTupleData = new Uint8Array(oldTupleSize);
    const oldTupleDataView = new DataView(buffer, 28);
    oldTupleData.forEach(
      (_, i) => (oldTupleData[i] = oldTupleDataView.getUint8(i))
    );
    const newTupleSize = view.byteLength - 28 - oldTupleSize;
    const newTupleData = new Uint8Array(newTupleSize);
    const newTupleDataView = new DataView(buffer, 28 + oldTupleSize);
    newTupleData.forEach(
      (_, i) => (newTupleData[i] = newTupleDataView.getUint8(i))
    );
    return new UpdateLogRecord(
      lsn,
      prevLsn,
      transactionId,
      { pageId, slotId },
      oldTupleData.buffer,
      newTupleData.buffer
    );
  }
  toJSON() {
    return {
      lsn: this._lsn,
      prevLsn: this._prevLsn,
      transactionId: this._transactionId,
      type: this._type,
      rid: this._rid,
      oldTupleData: this._oldTupleData,
      newTupleData: this._newTupleData,
    };
  }
}
export class MarkDeleteLogRecord extends LogRecord {
  constructor(
    protected _lsn: number,
    protected _prevLsn: number,
    protected _transactionId: number,
    private _rid: RID,
    private _tupleData: ArrayBuffer
  ) {
    super(_lsn, _prevLsn, _transactionId, LogRecordType.MARK_DELETE);
  }
  get rid(): RID {
    return this._rid;
  }
  get tupleData(): ArrayBuffer {
    return this._tupleData;
  }
  serialize(): ArrayBuffer {
    const buffer = new ArrayBuffer(
      HEADER_SIZE + 8 + this._tupleData.byteLength
    );
    const view = new DataView(buffer);
    view.setInt32(0, HEADER_SIZE + 8 + this._tupleData.byteLength);
    view.setInt32(4, this._lsn);
    view.setInt32(8, this._prevLsn);
    view.setInt32(12, this._transactionId);
    view.setInt32(16, this._type);
    view.setInt32(20, this._rid.pageId);
    view.setInt32(24, this._rid.slotId);
    const data = new Uint8Array(this._tupleData);
    const dataView = new DataView(buffer, 28);
    data.forEach((byte, i) => dataView.setUint8(i, byte));
    return buffer;
  }
  static deserialize(buffer: ArrayBuffer): MarkDeleteLogRecord {
    const view = new DataView(buffer);
    const lsn = view.getInt32(4);
    const prevLsn = view.getInt32(8);
    const transactionId = view.getInt32(12);
    const pageId = view.getInt32(20);
    const slotId = view.getInt32(24);
    const tupleSize = view.byteLength - 28;
    const tupleData = new Uint8Array(tupleSize);
    const tupleDataView = new DataView(buffer, 32);
    tupleData.forEach((_, i) => (tupleData[i] = tupleDataView.getUint8(i)));
    return new MarkDeleteLogRecord(
      lsn,
      prevLsn,
      transactionId,
      { pageId, slotId },
      tupleData.buffer
    );
  }
  toJSON() {
    return {
      lsn: this._lsn,
      prevLsn: this._prevLsn,
      transactionId: this._transactionId,
      type: this._type,
      rid: this._rid,
      tupleData: this._tupleData,
    };
  }
}
export class RollbackDeleteLogRecord extends LogRecord {
  constructor(
    protected _lsn: number,
    protected _prevLsn: number,
    protected _transactionId: number,
    private _rid: RID,
    private _tupleData: ArrayBuffer
  ) {
    super(_lsn, _prevLsn, _transactionId, LogRecordType.ROLLBACK_DELETE);
  }
  get rid(): RID {
    return this._rid;
  }
  get tupleData(): ArrayBuffer {
    return this._tupleData;
  }
  serialize(): ArrayBuffer {
    const buffer = new ArrayBuffer(
      HEADER_SIZE + 8 + this._tupleData.byteLength
    );
    const view = new DataView(buffer);
    view.setInt32(0, HEADER_SIZE + 8 + this._tupleData.byteLength);
    view.setInt32(4, this._lsn);
    view.setInt32(8, this._prevLsn);
    view.setInt32(12, this._transactionId);
    view.setInt32(16, this._type);
    view.setInt32(20, this._rid.pageId);
    view.setInt32(24, this._rid.slotId);
    const data = new Uint8Array(this._tupleData);
    const dataView = new DataView(buffer, 28);
    data.forEach((byte, i) => dataView.setUint8(i, byte));
    return buffer;
  }
  static deserialize(buffer: ArrayBuffer): RollbackDeleteLogRecord {
    const view = new DataView(buffer);
    const lsn = view.getInt32(4);
    const prevLsn = view.getInt32(8);
    const transactionId = view.getInt32(12);
    const pageId = view.getInt32(20);
    const slotId = view.getInt32(24);
    const tupleSize = view.byteLength - 28;
    const tupleData = new Uint8Array(tupleSize);
    const tupleDataView = new DataView(buffer, 32);
    tupleData.forEach((_, i) => (tupleData[i] = tupleDataView.getUint8(i)));
    return new RollbackDeleteLogRecord(
      lsn,
      prevLsn,
      transactionId,
      { pageId, slotId },
      tupleData.buffer
    );
  }
  toJSON() {
    return {
      lsn: this._lsn,
      prevLsn: this._prevLsn,
      transactionId: this._transactionId,
      type: this._type,
      rid: this._rid,
      tupleData: this._tupleData,
    };
  }
}
export class ApplyDeleteLogRecord extends LogRecord {
  constructor(
    protected _lsn: number,
    protected _prevLsn: number,
    protected _transactionId: number,
    private _rid: RID,
    private _tupleData: ArrayBuffer
  ) {
    super(_lsn, _prevLsn, _transactionId, LogRecordType.APPLY_DELETE);
  }
  get rid(): RID {
    return this._rid;
  }
  get tupleData(): ArrayBuffer {
    return this._tupleData;
  }
  serialize(): ArrayBuffer {
    const buffer = new ArrayBuffer(
      HEADER_SIZE + 8 + this._tupleData.byteLength
    );
    const view = new DataView(buffer);
    view.setInt32(0, HEADER_SIZE + 8 + this._tupleData.byteLength);
    view.setInt32(4, this._lsn);
    view.setInt32(8, this._prevLsn);
    view.setInt32(12, this._transactionId);
    view.setInt32(16, this._type);
    view.setInt32(20, this._rid.pageId);
    view.setInt32(24, this._rid.slotId);
    const data = new Uint8Array(this._tupleData);
    const dataView = new DataView(buffer, 28);
    data.forEach((byte, i) => dataView.setUint8(i, byte));
    return buffer;
  }
  static deserialize(buffer: ArrayBuffer): ApplyDeleteLogRecord {
    const view = new DataView(buffer);
    const lsn = view.getInt32(4);
    const prevLsn = view.getInt32(8);
    const transactionId = view.getInt32(12);
    const pageId = view.getInt32(20);
    const slotId = view.getInt32(24);
    const tupleSize = view.byteLength - 28;
    const tupleData = new Uint8Array(tupleSize);
    const tupleDataView = new DataView(buffer, 32);
    tupleData.forEach((_, i) => (tupleData[i] = tupleDataView.getUint8(i)));
    return new ApplyDeleteLogRecord(
      lsn,
      prevLsn,
      transactionId,
      { pageId, slotId },
      tupleData.buffer
    );
  }
  toJSON() {
    return {
      lsn: this._lsn,
      prevLsn: this._prevLsn,
      transactionId: this._transactionId,
      type: this._type,
      rid: this._rid,
      tupleData: this._tupleData,
    };
  }
}
export class BeginLogRecord extends LogRecord {
  constructor(
    protected _lsn: number,
    protected _prevLsn: number,
    protected _transactionId: number
  ) {
    super(_lsn, _prevLsn, _transactionId, LogRecordType.BEGIN);
  }
}
export class CommitLogRecord extends LogRecord {
  constructor(
    protected _lsn: number,
    protected _prevLsn: number,
    protected _transactionId: number
  ) {
    super(_lsn, _prevLsn, _transactionId, LogRecordType.COMMIT);
  }
}
export class AbortLogRecord extends LogRecord {
  constructor(
    protected _lsn: number,
    protected _prevLsn: number,
    protected _transactionId: number
  ) {
    super(_lsn, _prevLsn, _transactionId, LogRecordType.ABORT);
  }
}
