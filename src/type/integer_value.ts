import { BooleanValue } from "./boolean_value";
import { Type } from "./type";
import { InlinedValue, Value } from "./value";
import { VarcharValue } from "./varchar_value";

export class IntegerValue extends InlinedValue {
  constructor(private _value: number) {
    super();
  }
  static deserialize(buffer: ArrayBuffer, offset: number): IntegerValue {
    const dataView = new DataView(buffer);
    const value = dataView.getInt32(offset);
    return new IntegerValue(value);
  }
  get type(): Type {
    return Type.INTEGER;
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
  castAs(type: Type): Value {
    switch (type) {
      case Type.BOOLEAN:
        return new BooleanValue(this._value !== 0);
      case Type.INTEGER:
        return new IntegerValue(this._value);
      case Type.VARCHAR:
        return new VarcharValue(this._value.toString());
    }
  }
  performAdd(right: IntegerValue): IntegerValue {
    return new IntegerValue(this._value + right.value);
  }
  performSubtract(right: IntegerValue): IntegerValue {
    return new IntegerValue(this._value - right.value);
  }
  performMultiply(right: IntegerValue): IntegerValue {
    return new IntegerValue(this._value * right.value);
  }
  performEqual(right: IntegerValue): BooleanValue {
    return new BooleanValue(this._value === right.value);
  }
  performNotEqual(right: IntegerValue): BooleanValue {
    return new BooleanValue(this._value !== right.value);
  }
  performLessThan(right: IntegerValue): BooleanValue {
    return new BooleanValue(this._value < right.value);
  }
  performGreaterThan(right: IntegerValue): BooleanValue {
    return new BooleanValue(this._value > right.value);
  }
  performLessThanEqual(right: IntegerValue): BooleanValue {
    return new BooleanValue(this._value <= right.value);
  }
  performGreaterThanEqual(right: IntegerValue): BooleanValue {
    return new BooleanValue(this._value >= right.value);
  }
}
