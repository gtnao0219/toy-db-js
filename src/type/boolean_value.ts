import { IntegerValue } from "./integer_value";
import { Type } from "./type";
import { InlinedValue, Value } from "./value";
import { VarcharValue } from "./varchar_value";

export class BooleanValue extends InlinedValue {
  constructor(private _value: boolean) {
    super();
  }
  static deserialize(buffer: ArrayBuffer, offset: number): BooleanValue {
    const dataView = new DataView(buffer);
    const value = dataView.getUint8(offset);
    return new BooleanValue(value === 1);
  }
  get value(): boolean {
    return this._value;
  }
  get type(): Type {
    return Type.BOOLEAN;
  }
  serialize(): ArrayBuffer {
    const buffer = new ArrayBuffer(1);
    const dataView = new DataView(buffer);
    dataView.setUint8(0, this._value ? 1 : 0);
    return buffer;
  }
  castAs(type: Type): Value {
    switch (type) {
      case Type.BOOLEAN:
        return new BooleanValue(this._value);
      case Type.INTEGER:
        return new IntegerValue(this._value ? 1 : 0);
      case Type.VARCHAR:
        return new VarcharValue(this._value.toString());
    }
  }
  performAnd(right: BooleanValue): BooleanValue {
    return new BooleanValue(this._value && right.value);
  }
  performOr(right: BooleanValue): BooleanValue {
    return new BooleanValue(this._value || right.value);
  }
}
