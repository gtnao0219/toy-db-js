import { Schema } from "../../catalog/schema";
import { TupleWithRID } from "../../storage/table/table_heap";
import { ExecutorContext } from "../executor_context";
import { PlanNode } from "../plan";

export enum ExecutorType {
  INSERT,
  DELETE,
  UPDATE,
  NESTED_LOOP_JOIN,
  SEQ_SCAN,
  INDEX_SCAN,
  PROJECTION,
  AGGREGATE,
  FILTER,
  SORT,
  LIMIT,
}
export abstract class Executor {
  constructor(
    protected _executorContext: ExecutorContext,
    protected _planNode: PlanNode,
    protected _executorType: ExecutorType
  ) {}
  get executorType(): ExecutorType {
    return this._executorType;
  }
  abstract init(): Promise<void>;
  abstract next(): Promise<TupleWithRID | null>;
  outputSchema(): Schema {
    return this._planNode.outputSchema;
  }
}
