import { TupleWithRID } from "../../storage/table/table_heap";
import { Tuple } from "../../storage/table/tuple";
import { BooleanValue } from "../../type/boolean_value";
import { IntegerValue } from "../../type/integer_value";
import { Type } from "../../type/type";
import { Value } from "../../type/value";
import { VarcharValue } from "../../type/varchar_value";
import { ExecutorContext } from "../executor_context";
import { evaluate } from "../expression_plan";
import { AggregatePlanNode, AggregationType } from "../plan";
import { Executor, ExecutorType } from "./executor";

export class AggregateExecutor extends Executor {
  private _aggregateHashTables: AggregateHashTable;
  private _cursor: number = 0;
  private _aggregated: any[][] = [];
  constructor(
    protected _executorContext: ExecutorContext,
    protected _planNode: AggregatePlanNode,
    private _child: Executor
  ) {
    super(_executorContext, _planNode, ExecutorType.AGGREGATE);
    this._aggregateHashTables = new AggregateHashTable(
      this._planNode.aggregationTypes
    );
  }
  async init(): Promise<void> {
    await this._child.init();
    let tuple = await this._child.next();
    while (tuple !== null) {
      this._aggregateHashTables.insert(
        this.makeAggregateKey(tuple.tuple),
        this.makeAggregateValue(tuple.tuple)
      );
      tuple = await this._child.next();
    }
    this._aggregated = this._aggregateHashTables.getAll();
  }
  async next(): Promise<TupleWithRID | null> {
    if (this._cursor >= this._aggregated.length) {
      return null;
    }
    const schema = this.outputSchema();
    const tuple = new Tuple(
      schema,
      this._aggregated[this._cursor++].map((v, i) => {
        switch (schema.columns[i].type) {
          case Type.INTEGER:
            return new IntegerValue(Number(v));
          case Type.VARCHAR:
            return new VarcharValue(String(v));
          case Type.BOOLEAN:
            return new BooleanValue(Boolean(v));
          default:
            throw new Error("Unsupported type");
        }
      })
    );
    return { rid: { pageId: -1, slotId: -1 }, tuple };
  }
  private makeAggregateKey(tuple: Tuple): Value[] {
    return this._planNode.groupBy.map((expr) =>
      evaluate(expr, tuple, this.outputSchema())
    );
  }
  private makeAggregateValue(tuple: Tuple): Value[] {
    return this._planNode.aggregations.map((expr) =>
      evaluate(expr, tuple, this.outputSchema())
    );
  }
}

class AggregateHashTable {
  private _hash: Map<any, any> = new Map();
  constructor(private _aggregationTypes: AggregationType[]) {}
  insert(keys: Value[], values: Value[]): void {
    const result = this.getOrCreate(keys);
    for (let i = 0; i < this._aggregationTypes.length; i++) {
      switch (this._aggregationTypes[i]) {
        case "count":
          result[i] = result[i].add(new IntegerValue(1));
          break;
        case "sum":
          result[i] = result[i].add(values[i]);
          break;
        case "min":
          result[i] = result[i].lessThan(values[i]) ? result[i] : values[i];
          break;
        case "max":
          result[i] = result[i].greaterThan(values[i]) ? result[i] : values[i];
          break;
      }
    }
  }
  getAll(): any[][] {
    return this.flatten(this._hash);
  }
  flatten(hash: Map<any, any>, keys: any[] = []): any[][] {
    let result: any[][] = [];
    hash.forEach((value, key) => {
      if (Array.isArray(value)) {
        result.push([...keys, key, ...value.map((v) => v.value)]);
      } else {
        result = result.concat(this.flatten(value, [...keys, key]));
      }
    });
    return result;
  }
  private getOrCreate(keys: Value[]): Value[] {
    let currentHash = this._hash;
    for (let i = 0; i < keys.length; i++) {
      const key = keys[i];
      const ret = currentHash.get(key.value);
      if (i === keys.length - 1) {
        if (ret !== undefined) {
          return ret;
        } else {
          currentHash.set(key.value, this.makeInitialValue());
          return currentHash.get(key.value);
        }
      } else {
        if (ret !== undefined) {
          currentHash = ret;
        } else {
          currentHash.set(key.value, new Map());
          currentHash = currentHash.get(key.value);
        }
      }
    }
    throw new Error("unreachable");
  }
  private makeInitialValue(): Value[] {
    return this._aggregationTypes.map((type) => {
      switch (type) {
        case "count":
          return new IntegerValue(0);
        case "sum":
          return new IntegerValue(0);
        case "min":
          return new IntegerValue(Number.MAX_SAFE_INTEGER);
        case "max":
          return new IntegerValue(Number.MIN_SAFE_INTEGER);
      }
    });
  }
}
