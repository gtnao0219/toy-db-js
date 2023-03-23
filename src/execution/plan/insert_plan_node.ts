import { Value } from "../../type/value";

export class InsertPlanNode {
  constructor(private _tableOid: number, private _rawValues: Value[]) {}
  get tableOid(): number {
    return this._tableOid;
  }
  get rawValues(): Value[] {
    return this._rawValues;
  }
}
