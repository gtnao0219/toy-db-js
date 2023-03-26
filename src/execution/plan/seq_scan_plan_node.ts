import { BoundBaseTableRef } from "../../binder/bound_table_ref";
import { PlanNode, PlanType } from "./plan_node";

export class SeqScanPlanNode extends PlanNode {
  constructor(private _table: BoundBaseTableRef) {
    super(PlanType.SEQ_SCAN);
  }
  get table(): BoundBaseTableRef {
    return this._table;
  }
}
