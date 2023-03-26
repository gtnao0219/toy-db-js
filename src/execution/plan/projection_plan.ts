import { BoundExpression } from "../../binder/bound_expression";
import { PlanNode, PlanType } from "./plan_node";

export class ProjectionPlanNode extends PlanNode {
  constructor(
    private _selectList: BoundExpression[],
    private _child: PlanNode
  ) {
    super(PlanType.PROJECTION);
  }
  get selectList(): BoundExpression[] {
    return this._selectList;
  }
  get child(): PlanNode {
    return this._child;
  }
}
