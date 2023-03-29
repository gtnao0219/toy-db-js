import { TupleWithRID } from "../../storage/table/table_heap";
import { ExecutorContext } from "../executor_context";
import { FilterPlanNode } from "../plan/filter_plan";
import { Executor, ExecutorType } from "./executor";

export class FilterExecutor extends Executor {
  constructor(
    protected _executorContext: ExecutorContext,
    private _planNode: FilterPlanNode,
    private _child: Executor
  ) {
    super(_executorContext, ExecutorType.FILTER);
  }
  init(): void {
    this._child.init();
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
