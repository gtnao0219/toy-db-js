import { BoundBaseTableRef } from "../../binder/bound_table_ref";
import { UpdateAssignment } from "../../binder/statement/update_statement";
import { PlanNode, PlanType } from "./plan_node";

export class UpdatePlanNode extends PlanNode {
  constructor(
    private _table: BoundBaseTableRef,
    private _assignments: UpdateAssignment[],
    private _child: PlanNode
  ) {
    super(PlanType.UPDATE);
  }
  get table(): BoundBaseTableRef {
    return this._table;
  }
  get assignments(): UpdateAssignment[] {
    return this._assignments;
  }
  get child(): PlanNode {
    return this._child;
  }
}
