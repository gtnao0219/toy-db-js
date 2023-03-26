export enum StatementType {
  CREATE_TABLE,
  INSERT,
  SELECT,
}
export class Statement {
  constructor(private _statementType: StatementType) {}
  get statementType(): StatementType {
    return this._statementType;
  }
}
