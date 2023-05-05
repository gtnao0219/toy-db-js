import { TupleWithRID } from "../../storage/table/table_heap";
import { Tuple } from "../../storage/table/tuple";
import { ExecutorContext } from "../executor_context";
import { evaluate } from "../expression_plan";
import { ProjectPlanNode } from "../plan";
import { Executor, ExecutorType } from "./executor";

export class ProjectionExecutor extends Executor {
  constructor(
    protected _executorContext: ExecutorContext,
    protected _planNode: ProjectPlanNode,
    private _child: Executor
  ) {
    super(_executorContext, _planNode, ExecutorType.PROJECTION);
  }
  async init(): Promise<void> {
    await this._child.init();
  }
  async next(): Promise<TupleWithRID | null> {
    const tuple = await this._child.next();
    if (tuple === null) {
      return null;
    }
    const values = this._planNode.selectElements.map((selectElement) => {
      return evaluate(
        selectElement.expression,
        tuple.tuple,
        this._child.outputSchema()
      );
    });
    const newTuple = new Tuple(this.outputSchema(), values);
    return { tuple: newTuple, rid: tuple.rid };
  }
}
