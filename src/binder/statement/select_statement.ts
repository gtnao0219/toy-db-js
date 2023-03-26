import { BoundExpression } from "../bound_expression";
import { BoundTableRef } from "../bound_table_ref";
import { Statement, StatementType } from "./statement";

export class SelectStatement extends Statement {
  constructor(
    private _table: BoundTableRef,
    private _selectList: BoundExpression[],
    private _predicate: BoundExpression
  ) {
    super(StatementType.SELECT);
  }
  get table(): BoundTableRef {
    return this._table;
  }
  get selectList(): BoundExpression[] {
    return this._selectList;
  }
  get predicate(): BoundExpression {
    return this._predicate;
  }
}
