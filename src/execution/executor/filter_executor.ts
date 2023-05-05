import { TupleWithRID } from "../../storage/table/table_heap";
import { Type } from "../../type/type";
import { ExecutorContext } from "../executor_context";
import { evaluate } from "../expression_plan";
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
      const evaluated = evaluate(
        this._planNode.condition,
        tuple.tuple,
        this._child.outputSchema()
      );
      if (evaluated.type === Type.BOOLEAN && evaluated.value) {
        return tuple;
      }
      tuple = await this._child.next();
    }
    return null;
  }
}
