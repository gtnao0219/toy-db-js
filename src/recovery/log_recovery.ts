import { BufferPoolManager } from "../buffer/buffer_pool_manager";
import { Catalog } from "../catalog/catalog";
import {
  TablePage,
  TablePageDeserializerUsingCatalog,
} from "../storage/page/table_page";
import { Tuple } from "../storage/table/tuple";
import { LogManager } from "./log_manager";
import {
  BeginLogRecord,
  CommitLogRecord,
  InsertLogRecord,
  UpdateLogRecord,
  MarkDeleteLogRecord,
  RollbackDeleteLogRecord,
  ApplyDeleteLogRecord,
} from "./log_record";

export async function recover(
  bufferPoolManager: BufferPoolManager,
  logManager: LogManager,
  catalog: Catalog
): Promise<void> {
  const activeTx = await redo(bufferPoolManager, logManager, catalog);
  await undo(bufferPoolManager, logManager, catalog, activeTx);
  await bufferPoolManager.flushAllPages();
}

async function redo(
  bufferPoolManager: BufferPoolManager,
  logManager: LogManager,
  catalog: Catalog
) {
  const logs = await logManager.read();
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
          const tuple = Tuple.deserialize(log.tupleData, page.schema);
          page.insertTuple(tuple, null);
          page.lsn = log.lsn;
        }
        if (log instanceof UpdateLogRecord) {
          console.log(`Redo update: ${log.rid}`);
          const tuple = Tuple.deserialize(log.newTupleData, page.schema);
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
  bufferPoolManager: BufferPoolManager,
  logManager: LogManager,
  catalog: Catalog,
  activeTx: Set<number>
) {
  const logs = await logManager.read();
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
          const tuple = Tuple.deserialize(log.oldTupleData, page.schema);
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
          const tuple = Tuple.deserialize(log.tupleData, page.schema);
          console.log(`Undo apply delete: ${tuple}`);
          page.insertTuple(tuple, null);
        }
        bufferPoolManager.unpinPage(page.pageId, true);
      }
    }
  }
}
