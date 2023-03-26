import { Schema } from "../../catalog/schema";
import { PlanNode, PlanType } from "./plan_node";

export class SeqScanPlanNode extends PlanNode {
  constructor(private _tableOid: number, private _schema: Schema) {
    super(PlanType.SEQ_SCAN);
  }
  get tableOid(): number {
    return this._tableOid;
  }
  get schema(): Schema {
    return this._schema;
  }
}
