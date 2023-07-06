import { Type } from "../type/type";

export class Column {
  constructor(private _name: string, private _type: Type) {}
  get name(): string {
    return this._name;
  }
  get type(): Type {
    return this._type;
  }
  toJSON() {
    return {
      name: this._name,
      type: this._type,
    };
  }
}
