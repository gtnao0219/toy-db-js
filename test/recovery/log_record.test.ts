import { LogRecord, LogRecordType } from "../../src/recovery/log_record";

describe("LogRecord", () => {
  describe("deserialize and serialize", () => {
    test("BeginLogRecord", () => {
      const buffer = new Uint8Array([
        // size
        0x00, 0x00, 0x00, 0x14,
        // lsn
        0x00, 0x00, 0x00, 0x0c,
        // prevLsn
        0x00, 0x00, 0x00, 0x0b,
        // transactionId
        0x00, 0x00, 0x00, 0x0a,
        // type
        0x00, 0x00, 0x00, 0x05,
      ]).buffer;
      const logRecord = LogRecord.deserialize(buffer);
      expect(logRecord.lsn).toBe(12);
      expect(logRecord.prevLsn).toBe(11);
      expect(logRecord.transactionId).toBe(10);
      expect(logRecord.type).toBe(LogRecordType.BEGIN);
      expect(logRecord.serialize()).toEqual(buffer);
    });
    test("CommitLogRecord", () => {
      const buffer = new Uint8Array([
        // size
        0x00, 0x00, 0x00, 0x14,
        // lsn
        0x00, 0x00, 0x00, 0x0c,
        // prevLsn
        0x00, 0x00, 0x00, 0x0b,
        // transactionId
        0x00, 0x00, 0x00, 0x0a,
        // type
        0x00, 0x00, 0x00, 0x06,
      ]).buffer;
      const logRecord = LogRecord.deserialize(buffer);
      expect(logRecord.lsn).toBe(12);
      expect(logRecord.prevLsn).toBe(11);
      expect(logRecord.transactionId).toBe(10);
      expect(logRecord.type).toBe(LogRecordType.COMMIT);
      expect(logRecord.serialize()).toEqual(buffer);
    });
    test("AbortLogRecord", () => {
      const buffer = new Uint8Array([
        // size
        0x00, 0x00, 0x00, 0x14,
        // lsn
        0x00, 0x00, 0x00, 0x0c,
        // prevLsn
        0x00, 0x00, 0x00, 0x0b,
        // transactionId
        0x00, 0x00, 0x00, 0x0a,
        // type
        0x00, 0x00, 0x00, 0x07,
      ]).buffer;
      const logRecord = LogRecord.deserialize(buffer);
      expect(logRecord.lsn).toBe(12);
      expect(logRecord.prevLsn).toBe(11);
      expect(logRecord.transactionId).toBe(10);
      expect(logRecord.type).toBe(LogRecordType.ABORT);
      expect(logRecord.serialize()).toEqual(buffer);
    });
  });
});
