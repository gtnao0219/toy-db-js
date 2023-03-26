import { Schema } from "../catalog/schema";

export abstract class BoundTableRef {}
export class BoundBaseTableRef extends BoundTableRef {
  constructor(private _tableOid: number, private _schema: Schema) {
    super();
  }
  get tableOid(): number {
    return this._tableOid;
  }
  get schema(): Schema {
    return this._schema;
  }
}
