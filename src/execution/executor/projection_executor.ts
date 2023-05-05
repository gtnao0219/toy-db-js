import { evaluate } from "../../binder/bound";
import { Column } from "../../catalog/column";
import { Schema } from "../../catalog/schema";
import { TupleWithRID } from "../../storage/table/table_heap";
import { Tuple } from "../../storage/table/tuple";
import { BooleanValue } from "../../type/boolean_value";
import { IntegerValue } from "../../type/integer_value";
import { Type } from "../../type/type";
import { VarcharValue } from "../../type/varchar_value";
import { ExecutorContext } from "../executor_context";
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
    const rawValues = this._planNode.selectElements.map((selectElement) => {
      return evaluate(
        selectElement.expression,
        tuple,
        this._child.outputSchema()
      );
    });
    const newTuple = new Tuple(
      new Schema(
        rawValues.map((value, i) => {
          if (typeof value === "string") {
            return new Column(`col${i}`, Type.VARCHAR);
          }
          if (typeof value === "number") {
            return new Column(`col${i}`, Type.INTEGER);
          }
          if (typeof value === "boolean") {
            return new Column(`col${i}`, Type.BOOLEAN);
          }
          throw new Error(`Unexpected value type ${typeof value}`);
        })
      ),
      rawValues.map((value) => {
        if (typeof value === "string") {
          return new VarcharValue(value);
        }
        if (typeof value === "number") {
          return new IntegerValue(value);
        }
        if (typeof value === "boolean") {
          return new BooleanValue(value);
        }
        throw new Error(`Unexpected value type ${typeof value}`);
      })
    );
    return { tuple: newTuple, rid: tuple.rid };
  }
}
