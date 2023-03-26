import { BufferPoolManager } from "../../buffer/buffer_pool_manager";
import { Catalog } from "../../catalog/catalog";
import { Column } from "../../catalog/column";
import { Schema } from "../../catalog/schema";
import { TupleWithRID } from "../../storage/table/table_heap";
import { Tuple } from "../../storage/table/tuple";
import { BooleanValue } from "../../type/boolean_value";
import { IntegerValue } from "../../type/integer_value";
import { Type } from "../../type/type";
import { VarcharValue } from "../../type/varchar_value";
import { ProjectionPlanNode } from "../plan/projection_plan";
import { Executor, ExecutorType } from "./executor";

export class ProjectionExecutor extends Executor {
  constructor(
    protected _catalog: Catalog,
    protected _bufferPoolManager: BufferPoolManager,
    private _planNode: ProjectionPlanNode,
    private _child: Executor
  ) {
    super(_catalog, _bufferPoolManager, ExecutorType.PROJECTION);
  }
  next(): TupleWithRID | null {
    const tuple = this._child.next();
    if (tuple === null) {
      return null;
    }
    const rawValues = this._planNode.selectList.map((expr) => {
      return expr.evaluate(tuple.tuple);
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
