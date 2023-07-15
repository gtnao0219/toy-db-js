import { DiskManager } from "../storage/disk/disk_manager";
import { LogRecord } from "./log_record";
import Mutex from "../../node_modules/async-mutex/lib/Mutex";

export interface LogManager {
  bootstrap(): Promise<void>;
  read(): Promise<LogRecord[]>;
  append(record: LogRecord): Promise<number>;
  flush(): Promise<void>;
}

export class LogManagerImpl implements LogManager {
  private _mutex: Mutex;
  private _logRecords: LogRecord[] = [];
  private _nextLsn: number = 0;
  constructor(private _diskManager: DiskManager) {
    this._mutex = new Mutex();
  }
  async bootstrap(): Promise<void> {
    const logRecords = await this.read();
    if (logRecords.length === 0) {
      return;
    }
    this._nextLsn = logRecords[logRecords.length - 1].lsn + 1;
  }
  async read(): Promise<LogRecord[]> {
    const log = await this._diskManager.readLog();
    const logView = new DataView(log);
    const logLength = log.byteLength;
    if (logLength === 0) {
      return [];
    }
    let offset = 0;
    const logRecords: LogRecord[] = [];
    while (offset < logLength) {
      const size = logView.getInt32(offset);
      const logRecord = LogRecord.deserialize(log.slice(offset, offset + size));
      logRecords.push(logRecord);
      offset += size;
    }
    return logRecords;
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
  async append(logRecord: LogRecord): Promise<number> {
    await this._mutex.runExclusive(async () => {
      logRecord.lsn = this._nextLsn++;
      this._logRecords.push(logRecord);
    });
    return logRecord.lsn;
  }
}
