import { Type } from "./type";
import { InlinedValue } from "./value";

export class IntegerValue extends InlinedValue {
  private _type = Type.INTEGER;
  constructor(private _value: number) {
    super();
  }
  static of(str: string): IntegerValue {
    const number = parseInt(str);
    if (isNaN(number)) {
      throw new Error("Invalid integer value");
    }
    // TODO: check if number is in range of int32
    return new IntegerValue(number);
  }
  static deserialize(buffer: ArrayBuffer, offset: number): IntegerValue {
    const dataView = new DataView(buffer);
    const value = dataView.getInt32(offset);
    return new IntegerValue(value);
  }
  get value(): number {
    return this._value;
  }
  serialize(): ArrayBuffer {
    const buffer = new ArrayBuffer(4);
    const dataView = new DataView(buffer);
    dataView.setInt32(0, this._value);
    return buffer;
  }
}
