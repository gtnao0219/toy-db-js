import { BufferPoolManager } from "../../buffer/buffer_pool_manager";
import { Catalog } from "../../catalog/catalog";
import { SeqScanPlanNode } from "../plan/seq_scan_plan_node";
import { Executor, ExecutorType } from "./executor";

export class SeqScanExecutor extends Executor {
  constructor(
    protected _catalog: Catalog,
    protected _bufferPoolManager: BufferPoolManager,
    private _planNode: SeqScanPlanNode
  ) {
    super(_catalog, _bufferPoolManager, ExecutorType.SEQ_SCAN);
  }
  next(): any[][] {
    const tableHeap = this._catalog.getTableHeapByOid(this._planNode.tableOid);
    const result = tableHeap.scan().map((tuple) => {
      return this._planNode.schema.columns.map((column) => {
        const columnIndex = tableHeap.schema.columns.findIndex((c) => {
          return c.name === column.name;
        });
        return tuple.tuple.values[columnIndex].value;
      });
    });
    return result;
  }
}
