import { BoundSortKey } from "../../binder/statement/select_statement";
import { PlanNode, PlanType } from "./plan_node";

export class SortPlanNode extends PlanNode {
  constructor(private _sortKeys: BoundSortKey[], private _child: PlanNode) {
    super(PlanType.SORT);
  }
  get sortKeys(): BoundSortKey[] {
    return this._sortKeys;
  }
  get child(): PlanNode {
    return this._child;
  }
}
