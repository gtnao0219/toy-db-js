import { Schema } from "../catalog/schema";
import { BoundExpression } from "./bound_expression";

export abstract class BoundTableRef {}

export class BoundBaseTableRef extends BoundTableRef {
  constructor(
    private _tableName: string,
    private _tableOid: number,
    private _alias: string,
    private _schema: Schema
  ) {
    super();
  }
  get tableName(): string {
    return this._tableName;
  }
  get tableOid(): number {
    return this._tableOid;
  }
  get alias(): string {
    return this._alias;
  }
  get schema(): Schema {
    return this._schema;
  }
}

export enum JoinType {
  INNER,
  LEFT,
  RIGHT,
}
export class BoundJoinRef extends BoundTableRef {
  constructor(
    private _joinType: JoinType,
    private _left: BoundTableRef,
    private _right: BoundTableRef,
    private _condition: BoundExpression
  ) {
    super();
  }
  get joinType(): JoinType {
    return this._joinType;
  }
  get left(): BoundTableRef {
    return this._left;
  }
  get right(): BoundTableRef {
    return this._right;
  }
  get condition(): BoundExpression {
    return this._condition;
  }
}

export class BoundCrossProductRef extends BoundTableRef {
  constructor(private _left: BoundTableRef, private _right: BoundTableRef) {
    super();
  }
  get left(): BoundTableRef {
    return this._left;
  }
  get right(): BoundTableRef {
    return this._right;
  }
}

export class BoundCteRef extends BoundTableRef {
  constructor(private _cteName: string, private _alias: string) {
    super();
  }
  get cteName(): string {
    return this._cteName;
  }
  get alias(): string {
    return this._alias;
  }
}

abstract class BoundSubQueryRef extends BoundTableRef {}
