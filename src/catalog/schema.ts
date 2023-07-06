import { Column } from "./column";

export class Schema {
  constructor(private _columns: Column[]) {}
  get columns(): Column[] {
    return this._columns;
  }
  toJSON() {
    return {
      columns: this._columns,
    };
  }
}
