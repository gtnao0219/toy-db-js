import { Tuple } from "../storage/table/tuple";
import { BoundBaseTableRef } from "./bound_table_ref";

export abstract class BoundExpression {
  abstract evaluate(tuple: Tuple): number | string | boolean;
}
export class BoundColumnRefExpression extends BoundExpression {
  constructor(private _table: BoundBaseTableRef, private _columnIndex: number) {
    super();
  }
  get table(): BoundBaseTableRef {
    return this._table;
  }
  get columnIndex(): number {
    return this._columnIndex;
  }
  evaluate(tuple: Tuple): number | string | boolean {
    return tuple.values[this._columnIndex].value;
  }
}
export class BoundLiteralExpression extends BoundExpression {
  constructor(private _value: number | string | boolean) {
    super();
  }
  get value(): number | string | boolean {
    return this._value;
  }
  evaluate(tuple: Tuple): number | string | boolean {
    return this._value;
  }
}
export class BoundBinaryExpression extends BoundExpression {
  constructor(
    private _left: BoundExpression,
    private _operator: string,
    private _right: BoundExpression
  ) {
    super();
  }
  get left(): BoundExpression {
    return this._left;
  }
  get operator(): string {
    return this._operator;
  }
  get right(): BoundExpression {
    return this._right;
  }
  evaluate(tuple: Tuple): boolean {
    const left = this._left.evaluate(tuple);
    const right = this._right.evaluate(tuple);
    switch (this._operator) {
      case "=":
        return left === right;
      default:
        throw new Error(`Unexpected operator ${this._operator}`);
    }
  }
}
