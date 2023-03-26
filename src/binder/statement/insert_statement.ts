import { Value } from "../../type/value";
import { Statement, StatementType } from "./statement";

export class InsertStatement extends Statement {
  constructor(private _tableOid: number, private _values: Value[]) {
    super(StatementType.INSERT);
  }
  get tableOid(): number {
    return this._tableOid;
  }
  get values(): Value[] {
    return this._values;
  }
}
