import { BoundBaseTableRef } from "../../binder/bound_table_ref";
import { Value } from "../../type/value";
import { PlanNode, PlanType } from "./plan_node";

export class InsertPlanNode extends PlanNode {
  constructor(private _table: BoundBaseTableRef, private _values: Value[]) {
    super(PlanType.INSERT);
  }
  get table(): BoundBaseTableRef {
    return this._table;
  }
  get values(): Value[] {
    return this._values;
  }
}
