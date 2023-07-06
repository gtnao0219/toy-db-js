import { IntegerValue } from "./integer_value";
import { Type } from "./type";
import { Value } from "./value";
import { VarcharValue } from "./varchar_value";

export class BooleanValue extends Value {
  constructor(private _value: boolean) {
    super();
  }
  static deserialize(buffer: ArrayBuffer, offset: number): BooleanValue {
    const dataView = new DataView(buffer);
    const value = dataView.getUint8(offset);
    switch (value) {
      case 0x00:
        return new BooleanValue(false);
      case 0x01:
        return new BooleanValue(true);
      default:
        throw new Error(`Invalid boolean value: ${value}`);
    }
  }
  get value(): boolean {
    return this._value;
  }
  get type(): Type {
    return Type.BOOLEAN;
  }
  size(): number {
    return 1;
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
  performEqual(right: BooleanValue): BooleanValue {
    return new BooleanValue(this._value === right.value);
  }
  performNotEqual(right: BooleanValue): BooleanValue {
    return new BooleanValue(this._value !== right.value);
  }
  performLessThan(right: BooleanValue): BooleanValue {
    return new BooleanValue(this._value < right.value);
  }
  performGreaterThan(right: BooleanValue): BooleanValue {
    return new BooleanValue(this._value > right.value);
  }
  performLessThanEqual(right: BooleanValue): BooleanValue {
    return new BooleanValue(this._value <= right.value);
  }
  performGreaterThanEqual(right: BooleanValue): BooleanValue {
    return new BooleanValue(this._value >= right.value);
  }
  performAnd(right: BooleanValue): BooleanValue {
    return new BooleanValue(this._value && right.value);
  }
  performOr(right: BooleanValue): BooleanValue {
    return new BooleanValue(this._value || right.value);
  }
  performNot(): BooleanValue {
    return new BooleanValue(!this._value);
  }
  toJSON() {
    return this._value;
  }
}
