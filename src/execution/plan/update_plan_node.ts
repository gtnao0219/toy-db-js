import { Value } from "../../type/value";
import { PlanNode, PlanType } from "./plan_node";

export class UpdatePlanNode extends PlanNode {
  constructor(
    private _tableOid: number,
    private _assignments: {
      columnIndex: number;
      value: Value;
    }[]
  ) {
    super(PlanType.UPDATE);
  }
  get tableOid(): number {
    return this._tableOid;
  }
  get assignments(): {
    columnIndex: number;
    value: Value;
  }[] {
    return this._assignments;
  }
}
