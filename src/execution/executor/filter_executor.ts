import { evaluate } from "../../binder/bound";
import { TupleWithRID } from "../../storage/table/table_heap";
import { ExecutorContext } from "../executor_context";
import { FilterPlanNode } from "../plan";
import { Executor, ExecutorType } from "./executor";

export class FilterExecutor extends Executor {
  constructor(
    protected _executorContext: ExecutorContext,
    protected _planNode: FilterPlanNode,
    private _child: Executor
  ) {
    super(_executorContext, _planNode, ExecutorType.FILTER);
  }
  async init(): Promise<void> {
    await this._child.init();
  }
  async next(): Promise<TupleWithRID | null> {
    let tuple = await this._child.next();
    while (tuple !== null) {
      if (
        evaluate(this._planNode.condition, tuple, this._child.outputSchema())
      ) {
        return tuple;
      }
      tuple = await this._child.next();
    }
    return null;
  }
}
