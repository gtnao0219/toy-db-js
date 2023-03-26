export abstract class BoundExpression {}
export class BoundColumnRefExpression extends BoundExpression {
  constructor(private _tableOid: number, private _columnIndex: number) {
    super();
  }
  get tableOid(): number {
    return this._tableOid;
  }
  get columnIndex(): number {
    return this._columnIndex;
  }
}
