import { BufferPoolManager } from "../../buffer/buffer_pool_manager";
import { Catalog } from "../../catalog/catalog";
import { TableHeap, TupleWithRID } from "../../storage/table/table_heap";
import { DeletePlanNode } from "../plan/delete_plan_node";
import { Executor, ExecutorType } from "./executor";

export class DeleteExecutor extends Executor {
  private tableHeap: TableHeap;
  constructor(
    protected _catalog: Catalog,
    protected _bufferPoolManager: BufferPoolManager,
    private _planNode: DeletePlanNode,
    private _child: Executor
  ) {
    super(_catalog, _bufferPoolManager, ExecutorType.INSERT);
    this.tableHeap = this._catalog.getTableHeapByOid(
      this._planNode.table.tableOid
    );
  }
  next(): TupleWithRID | null {
    let tuple: TupleWithRID | null = null;
    while ((tuple = this._child.next()) !== null) {
      this.tableHeap.deleteTuple(tuple.rid);
    }
    return null;
  }
}
