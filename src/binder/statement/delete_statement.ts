import { BoundExpression } from "../bound_expression";
import { BoundBaseTableRef } from "../bound_table_ref";
import { Statement, StatementType } from "./statement";

export class DeleteStatement extends Statement {
  constructor(
    private _table: BoundBaseTableRef,
    private _predicate: BoundExpression
  ) {
    super(StatementType.DELETE);
  }
  get table(): BoundBaseTableRef {
    return this._table;
  }
  get predicate(): BoundExpression {
    return this._predicate;
  }
}
