import { Schema } from "../../catalog/schema";
import { TupleWithRID } from "../../storage/table/table_heap";
import { Tuple } from "../../storage/table/tuple";
import { ExecutorContext } from "../executor_context";
import { evaluate } from "../expression_plan";
import { LimitPlanNode } from "../plan";
import { Executor, ExecutorType } from "./executor";

export class LimitExecutor extends Executor {
  private childTuples: TupleWithRID[] = [];
  private childTupleIndex = 0;
  constructor(
    protected _executorContext: ExecutorContext,
    protected _planNode: LimitPlanNode,
    private _child: Executor
  ) {
    super(_executorContext, _planNode, ExecutorType.LIMIT);
  }
  async init(): Promise<void> {
    await this._child.init();
    let tuple = await this._child.next();
    while (tuple !== null) {
      this.childTuples.push(tuple);
      tuple = await this._child.next();
    }
    // TODO:
    const emptySchema = new Schema([]);
    const emptyTuple = new Tuple(emptySchema, []);
    const evaluated = evaluate(this._planNode.count, emptyTuple, emptySchema);
    this.childTuples = this.childTuples.slice(0, Number(evaluated.value));
  }
  async next(): Promise<TupleWithRID | null> {
    return this.childTuples[this.childTupleIndex++] ?? null;
  }
}
