import { TupleWithRID } from "../../storage/table/table_heap";
import { Tuple } from "../../storage/table/tuple";
import { ExecutorContext } from "../executor_context";
import { evaluateJoin } from "../expression_plan";
import { NestedLoopJoinPlanNode } from "../plan";
import { Executor, ExecutorType } from "./executor";

export class NestedLoopJoinExecutor extends Executor {
  // TODO: implement iterator
  private _tuples: TupleWithRID[] = [];
  private _rightTuples: TupleWithRID[] = [];
  private _cursor: number = 0;
  private _rightCursor: number = 0;

  constructor(
    protected _executorContext: ExecutorContext,
    protected _planNode: NestedLoopJoinPlanNode,
    protected _left: Executor,
    protected _right: Executor
  ) {
    super(_executorContext, _planNode, ExecutorType.NESTED_LOOP_JOIN);
  }
  async init(): Promise<void> {
    await this._left.init();
    await this._right.init();
    let tuple = await this._right.next();
    while (tuple !== null) {
      this._rightTuples.push(tuple);
      tuple = await this._right.next();
    }

    {
      let tuple = await this._left.next();
      while (tuple !== null) {
        this._tuples.push(tuple);
        tuple = await this._left.next();
      }
    }
  }
  async next(): Promise<TupleWithRID | null> {
    while (this._cursor < this._tuples.length) {
      if (this._rightCursor >= this._rightTuples.length) {
        this._rightCursor = 0;
        this._cursor++;
        continue;
      }
      const leftTuple = this._tuples[this._cursor];
      const rightTuple = this._rightTuples[this._rightCursor++];
      if (
        evaluateJoin(
          this._planNode.condition,
          leftTuple.tuple,
          this._left.outputSchema(),
          rightTuple.tuple,
          this._right.outputSchema()
        ).value
      ) {
        return {
          tuple: new Tuple(this._planNode.outputSchema, [
            ...leftTuple.tuple.values,
            ...rightTuple.tuple.values,
          ]),
          // TODO:
          rid: rightTuple.rid,
        };
      }
    }
    return null;
  }
}
