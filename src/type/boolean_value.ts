import { Type } from "./type";
import { InlinedValue } from "./value";

export class BooleanValue extends InlinedValue {
  private type = Type.BOOLEAN;
  constructor(private _value: boolean) {
    super();
  }
  static of(str: string): BooleanValue {
    const value = str.toUpperCase() === "TRUE";
    return new BooleanValue(value);
  }
  static deserialize(buffer: ArrayBuffer, offset: number): BooleanValue {
    const dataView = new DataView(buffer);
    const value = dataView.getInt8(offset);
    return new BooleanValue(value === 1);
  }
  get value(): boolean {
    return this._value;
  }
  serialize(): ArrayBuffer {
    const buffer = new ArrayBuffer(1);
    const dataView = new DataView(buffer);
    dataView.setInt8(0, this._value ? 1 : 0);
    return buffer;
  }
}
