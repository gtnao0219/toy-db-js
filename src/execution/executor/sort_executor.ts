import { TupleWithRID } from "../../storage/table/table_heap";
import { ExecutorContext } from "../executor_context";
import { evaluate } from "../expression_plan";
import { SortPlanNode } from "../plan";
import { Executor, ExecutorType } from "./executor";

export class SortExecutor extends Executor {
  private childTuples: TupleWithRID[] = [];
  private childTupleIndex = 0;
  constructor(
    protected _executorContext: ExecutorContext,
    protected _planNode: SortPlanNode,
    private _child: Executor
  ) {
    super(_executorContext, _planNode, ExecutorType.SORT);
  }
  async init(): Promise<void> {
    await this._child.init();
    let tuple = await this._child.next();
    while (tuple !== null) {
      this.childTuples.push(tuple);
      tuple = await this._child.next();
    }
    this.childTuples = this.childTuples.sort((a, b) => {
      for (const sortKey of this._planNode.sortKeys) {
        const aEvaluated = evaluate(
          sortKey.expression,
          a.tuple,
          this._child.outputSchema()
        );
        const bEvaluated = evaluate(
          sortKey.expression,
          b.tuple,
          this._child.outputSchema()
        );
        if (aEvaluated.lessThan(bEvaluated)) {
          return sortKey.direction === "ASC" ? -1 : 1;
        } else if (aEvaluated.greaterThanEqual(bEvaluated)) {
          return sortKey.direction === "ASC" ? 1 : -1;
        }
      }
      return 0;
    });
    console.log(
      "sorted tuples",
      this.childTuples.map((t) => t.tuple.values)
    );
  }
  async next(): Promise<TupleWithRID | null> {
    return this.childTuples[this.childTupleIndex++] ?? null;
  }
}
