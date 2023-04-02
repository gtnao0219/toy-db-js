import { PlanNode, PlanType } from "./plan_node";

export class LimitPlanNode extends PlanNode {
  constructor(private _count: number, private _child: PlanNode) {
    super(PlanType.LIMIT);
  }
  get count(): number {
    return this._count;
  }
  get child(): PlanNode {
    return this._child;
  }
}
