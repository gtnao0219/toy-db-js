import { Schema } from "../../catalog/schema";

export class SeqScanPlanNode {
  constructor(private _tableOid: number, private _schema: Schema) {}
  get tableOid(): number {
    return this._tableOid;
  }
  get schema(): Schema {
    return this._schema;
  }
}
