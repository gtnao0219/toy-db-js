export enum PlanType {
  INSERT,
  DELETE,
  UPDATE,
  SEQ_SCAN,
  PROJECTION,
  FILTER,
  SORT,
  LIMIT,
}
export abstract class PlanNode {
  constructor(protected _planType: PlanType) {}
  get planType(): PlanType {
    return this._planType;
  }
}
