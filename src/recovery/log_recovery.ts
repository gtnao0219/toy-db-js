import { BufferPoolManager } from "../buffer/buffer_pool_manager";
import { Catalog } from "../catalog/catalog";
import { DiskManager } from "../storage/disk/disk_manager";
import {
  TablePage,
  TablePageDeserializerUsingCatalog,
} from "../storage/page/table_page";
import { deserializeTuple } from "../storage/table/tuple";
import {
  BeginLogRecord,
  CommitLogRecord,
  AbortLogRecord,
  InsertLogRecord,
  LogRecord,
  UpdateLogRecord,
  MarkDeleteLogRecord,
  RollbackDeleteLogRecord,
  ApplyDeleteLogRecord,
} from "./log_record";

async function readLog(diskManager: DiskManager): Promise<LogRecord[]> {
  const log = await diskManager.readLog();
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
export async function nextTransactionIdAndLsn(
  diskManager: DiskManager
): Promise<[number, number]> {
  const logs = await readLog(diskManager);
  let nextTransactionId = 0;
  let nextLsn = 0;
  for (const log of logs) {
    nextTransactionId = Math.max(nextTransactionId, log.transactionId + 1);
    nextLsn = log.lsn + 1;
  }
  return [nextTransactionId, nextLsn];
}

export async function recover(
  diskManager: DiskManager,
  bufferPoolManager: BufferPoolManager,
  catalog: Catalog
): Promise<void> {
  const activeTx = await redo(diskManager, bufferPoolManager, catalog);
  await undo(diskManager, bufferPoolManager, catalog, activeTx);
  await bufferPoolManager.flushAllPages();
}

async function redo(
  diskManager: DiskManager,
  bufferPoolManager: BufferPoolManager,
  catalog: Catalog
) {
  const logs = await readLog(diskManager);
  console.log(JSON.stringify(logs, null, 2));
  const activeTx = new Set<number>();
  for (const log of logs) {
    if (log instanceof BeginLogRecord) {
      activeTx.add(log.transactionId);
    }
    if (log instanceof CommitLogRecord) {
      activeTx.delete(log.transactionId);
    }
    if (
      log instanceof InsertLogRecord ||
      log instanceof UpdateLogRecord ||
      log instanceof MarkDeleteLogRecord ||
      log instanceof RollbackDeleteLogRecord ||
      log instanceof ApplyDeleteLogRecord
    ) {
      const page = await bufferPoolManager.fetchPage(
        log.rid.pageId,
        new TablePageDeserializerUsingCatalog(catalog)
      );
      if (!(page instanceof TablePage)) {
        throw new Error("Page is not a table page");
      }
      if (page.lsn < log.lsn) {
        if (log instanceof InsertLogRecord) {
          console.log(`Redo insert: ${log.rid}`);
          const tuple = deserializeTuple(log.tupleData, page.schema);
          page.insertTuple(tuple, null);
          page.lsn = log.lsn;
        }
        if (log instanceof UpdateLogRecord) {
          console.log(`Redo update: ${log.rid}`);
          const tuple = deserializeTuple(log.newTupleData, page.schema);
          page.updateTuple(log.rid, tuple, null);
          page.lsn = log.lsn;
        }
        if (log instanceof MarkDeleteLogRecord) {
          console.log(`Redo mark delete: ${log.rid}`);
          page.markDelete(log.rid, null);
          page.lsn = log.lsn;
        }
        if (log instanceof RollbackDeleteLogRecord) {
          console.log(`Redo rollback delete: ${log.rid}`);
          page.rollbackDelete(log.rid, null);
          page.lsn = log.lsn;
        }
        if (log instanceof ApplyDeleteLogRecord) {
          console.log(`Redo apply delete: ${log.rid}`);
          page.applyDelete(log.rid, null);
          page.lsn = log.lsn;
        }
      }
      bufferPoolManager.unpinPage(page.pageId, true);
    }
  }
  return activeTx;
}
async function undo(
  diskManager: DiskManager,
  bufferPoolManager: BufferPoolManager,
  catalog: Catalog,
  activeTx: Set<number>
) {
  const logs = await readLog(diskManager);
  logs.reverse();
  for (const log of logs) {
    if (activeTx.has(log.transactionId)) {
      if (
        log instanceof InsertLogRecord ||
        log instanceof UpdateLogRecord ||
        log instanceof MarkDeleteLogRecord ||
        log instanceof RollbackDeleteLogRecord ||
        log instanceof ApplyDeleteLogRecord
      ) {
        const page = await bufferPoolManager.fetchPage(
          log.rid.pageId,
          new TablePageDeserializerUsingCatalog(catalog)
        );
        if (!(page instanceof TablePage)) {
          throw new Error("Page is not a table page");
        }
        if (log instanceof InsertLogRecord) {
          console.log(`Undo insert: ${log.rid}`);
          page.applyDelete(log.rid, null);
        }
        if (log instanceof UpdateLogRecord) {
          const tuple = deserializeTuple(log.oldTupleData, page.schema);
          console.log(`Undo update: ${tuple}`);
          page.updateTuple(log.rid, tuple, null);
        }
        if (log instanceof MarkDeleteLogRecord) {
          console.log(`Undo mark delete: ${log.rid}`);
          page.rollbackDelete(log.rid, null);
        }
        if (log instanceof RollbackDeleteLogRecord) {
          console.log(`Undo rollback delete: ${log.rid}`);
          page.markDelete(log.rid, null);
        }
        if (log instanceof ApplyDeleteLogRecord) {
          const tuple = deserializeTuple(log.tupleData, page.schema);
          console.log(`Undo apply delete: ${tuple}`);
          page.insertTuple(tuple, null);
        }
        bufferPoolManager.unpinPage(page.pageId, true);
      }
    }
  }
}
