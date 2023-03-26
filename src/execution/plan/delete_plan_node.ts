import { PlanNode, PlanType } from "./plan_node";

export class DeletePlanNode extends PlanNode {
  constructor(private _tableOid: number) {
    super(PlanType.DELETE);
  }
  get tableOid(): number {
    return this._tableOid;
  }
}
