import { TupleWithRID } from "../../storage/table/table_heap";
import { ExecutorContext } from "../executor_context";
import { LimitPlanNode } from "../plan/limit_plan";
import { Executor, ExecutorType } from "./executor";

export class LimitExecutor extends Executor {
  private childTuples: TupleWithRID[] = [];
  private childTupleIndex = 0;
  constructor(
    protected _executorContext: ExecutorContext,
    private _planNode: LimitPlanNode,
    private _child: Executor
  ) {
    super(_executorContext, ExecutorType.LIMIT);
  }
  async init(): Promise<void> {
    await this._child.init();
    let tuple = await this._child.next();
    while (tuple !== null) {
      this.childTuples.push(tuple);
      tuple = await this._child.next();
    }
    this.childTuples = this.childTuples.slice(0, this._planNode.count);
  }
  async next(): Promise<TupleWithRID | null> {
    return this.childTuples[this.childTupleIndex++] ?? null;
  }
}
