import { BufferPoolManager } from "../../buffer/buffer_pool_manager";
import { Catalog } from "../../catalog/catalog";
import { TupleWithRID } from "../../storage/table/table_heap";
import { FilterPlanNode } from "../plan/filter_plan";
import { Executor, ExecutorType } from "./executor";

export class FilterExecutor extends Executor {
  constructor(
    protected _catalog: Catalog,
    protected _bufferPoolManager: BufferPoolManager,
    private _planNode: FilterPlanNode,
    private _child: Executor
  ) {
    super(_catalog, _bufferPoolManager, ExecutorType.FILTER);
  }
  next(): TupleWithRID | null {
    let tuple = this._child.next();
    while (tuple !== null) {
      if (this._planNode.predicate.evaluate(tuple.tuple)) {
        return tuple;
      }
      tuple = this._child.next();
    }
    return null;
  }
}
