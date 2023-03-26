import { BoundBaseTableRef } from "../../binder/bound_table_ref";
import { PlanNode, PlanType } from "./plan_node";

export class DeletePlanNode extends PlanNode {
  constructor(private _table: BoundBaseTableRef, private _child: PlanNode) {
    super(PlanType.DELETE);
  }
  get table(): BoundBaseTableRef {
    return this._table;
  }
  get child(): PlanNode {
    return this._child;
  }
}
