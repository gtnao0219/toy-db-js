import { Value } from "../../type/value";
import { BoundExpression } from "../bound_expression";
import { BoundBaseTableRef } from "../bound_table_ref";
import { Statement, StatementType } from "./statement";

export type UpdateAssignment = {
  columnIndex: number;
  value: Value;
};

export class UpdateStatement extends Statement {
  constructor(
    private _table: BoundBaseTableRef,
    private _assignments: UpdateAssignment[],
    private _predicate: BoundExpression
  ) {
    super(StatementType.UPDATE);
  }
  get table(): BoundBaseTableRef {
    return this._table;
  }
  get assignments(): UpdateAssignment[] {
    return this._assignments;
  }
  get predicate(): BoundExpression {
    return this._predicate;
  }
}
