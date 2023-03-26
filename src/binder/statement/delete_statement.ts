import { Statement, StatementType } from "./statement";

export class DeleteStatement extends Statement {
  constructor(private _tableOid: number) {
    super(StatementType.DELETE);
  }
  get tableOid(): number {
    return this._tableOid;
  }
}
