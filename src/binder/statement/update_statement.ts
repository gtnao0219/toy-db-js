import { Value } from "../../type/value";
import { Statement, StatementType } from "./statement";

export class UpdateStatement extends Statement {
  constructor(
    private _tableOid: number,
    private _assignments: {
      columnIndex: number;
      value: Value;
    }[]
  ) {
    super(StatementType.UPDATE);
  }
  get tableOid(): number {
    return this._tableOid;
  }
  get assignments(): {
    columnIndex: number;
    value: Value;
  }[] {
    return this._assignments;
  }
}
