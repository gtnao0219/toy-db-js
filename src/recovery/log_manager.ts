import { DiskManager } from "../storage/disk/disk_manager";
import { LogRecord } from "./log_record";
import Mutex from "../../node_modules/async-mutex/lib/Mutex";

export class LogManager {
  private _mutex: Mutex;
  private _logRecords: LogRecord[] = [];
  constructor(private _diskManager: DiskManager, private _nextLsn: number = 0) {
    this._mutex = new Mutex();
  }
  set nextLsn(nextLsn: number) {
    this._nextLsn = nextLsn;
  }
  async flush() {
    await this._mutex.runExclusive(async () => {
      const serialized = this._logRecords.map((logRecord) => {
        return logRecord.serialize();
      });
      const size = serialized.reduce((acc, buffer) => {
        return acc + buffer.byteLength;
      }, 0);
      const buffer = new ArrayBuffer(size);
      const view = new DataView(buffer);
      for (let i = 0, offset = 0; i < serialized.length; i++) {
        const logRecord = serialized[i];
        const logRecordView = new DataView(logRecord);
        for (let j = 0; j < logRecord.byteLength; j++) {
          view.setUint8(offset, logRecordView.getUint8(j));
          offset++;
        }
      }
      await this._diskManager.writeLog(buffer);
      this._logRecords = [];
    });
  }
  async appendLogRecord(logRecord: LogRecord): Promise<number> {
    await this._mutex.runExclusive(async () => {
      logRecord.lsn = this._nextLsn++;
      this._logRecords.push(logRecord);
    });
    return logRecord.lsn;
  }
}
