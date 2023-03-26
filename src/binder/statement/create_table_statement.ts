import { Schema } from "../../catalog/schema";
import { Statement, StatementType } from "./statement";

export class CreateTableStatement extends Statement {
  constructor(private _tableName: string, private _schema: Schema) {
    super(StatementType.CREATE_TABLE);
  }
  get tableName(): string {
    return this._tableName;
  }
  get schema(): Schema {
    return this._schema;
  }
}
