import { BoundExpression } from "../../binder/bound_expression";
import { PlanNode, PlanType } from "./plan_node";

export class FilterPlanNode extends PlanNode {
  constructor(private _predicate: BoundExpression, private _child: PlanNode) {
    super(PlanType.FILTER);
  }
  get predicate(): BoundExpression {
    return this._predicate;
  }
  get child(): PlanNode {
    return this._child;
  }
}
