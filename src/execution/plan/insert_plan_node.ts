import { Value } from "../../type/value";
import { PlanNode, PlanType } from "./plan_node";

export class InsertPlanNode extends PlanNode {
  constructor(private _tableOid: number, private _values: Value[]) {
    super(PlanType.INSERT);
  }
  get tableOid(): number {
    return this._tableOid;
  }
  get values(): Value[] {
    return this._values;
  }
}
