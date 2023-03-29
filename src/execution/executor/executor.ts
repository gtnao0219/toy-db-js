import { TupleWithRID } from "../../storage/table/table_heap";
import { ExecutorContext } from "../executor_context";

export enum ExecutorType {
  INSERT,
  DELETE,
  UPDATE,
  SEQ_SCAN,
  PROJECTION,
  FILTER,
}
export abstract class Executor {
  constructor(
    protected _executorContext: ExecutorContext,
    protected _executorType: ExecutorType
  ) {}
  get executorType(): ExecutorType {
    return this._executorType;
  }
  abstract init(): void;
  abstract next(): TupleWithRID | null;
}
