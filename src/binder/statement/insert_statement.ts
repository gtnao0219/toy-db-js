import { Value } from "../../type/value";
import { BoundBaseTableRef } from "../bound_table_ref";
import { Statement, StatementType } from "./statement";

export class InsertStatement extends Statement {
  constructor(private _table: BoundBaseTableRef, private _values: Value[]) {
    super(StatementType.INSERT);
  }
  get table(): BoundBaseTableRef {
    return this._table;
  }
  get values(): Value[] {
    return this._values;
  }
}
