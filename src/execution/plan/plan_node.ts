export enum PlanType {
  INSERT,
  DELETE,
  UPDATE,
  SEQ_SCAN,
}
export abstract class PlanNode {
  constructor(protected _planType: PlanType) {}
  get planType(): PlanType {
    return this._planType;
  }
}
