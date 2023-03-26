import { BufferPoolManager } from "../../buffer/buffer_pool_manager";
import { Catalog } from "../../catalog/catalog";
import { TupleWithRID } from "../../storage/table/table_heap";
import { SeqScanPlanNode } from "../plan/seq_scan_plan_node";
import { Executor, ExecutorType } from "./executor";

export class SeqScanExecutor extends Executor {
  // TODO: implement iterator
  private _tuples: TupleWithRID[];
  private _cursor: number = 0;

  constructor(
    protected _catalog: Catalog,
    protected _bufferPoolManager: BufferPoolManager,
    private _planNode: SeqScanPlanNode
  ) {
    super(_catalog, _bufferPoolManager, ExecutorType.SEQ_SCAN);
    const tableHeap = this._catalog.getTableHeapByOid(
      this._planNode.table.tableOid
    );
    this._tuples = tableHeap.scan();
  }
  next(): TupleWithRID | null {
    if (this._cursor >= this._tuples.length) {
      return null;
    }
    return this._tuples[this._cursor++];
  }
}
