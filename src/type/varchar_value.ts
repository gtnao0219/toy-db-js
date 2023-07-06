import { BooleanValue } from "./boolean_value";
import { IntegerValue } from "./integer_value";
import { Type } from "./type";
import { Value } from "./value";

const SIZE_SIZE = 4;

export class VarcharValue extends Value {
  constructor(private _value: string) {
    super();
  }
  static deserialize(buffer: ArrayBuffer, offset: number): VarcharValue {
    const dataView = new DataView(buffer);
    const length = dataView.getInt32(offset);
    const uint8Array = new Uint8Array(buffer, offset + SIZE_SIZE, length);
    const value = new TextDecoder().decode(uint8Array);
    return new VarcharValue(value);
  }
  get value(): string {
    return this._value;
  }
  get type(): Type {
    return Type.VARCHAR;
  }
  size(): number {
    return SIZE_SIZE + this.variableSize();
  }
  variableSize() {
    return new TextEncoder().encode(this._value).length;
  }
  serialize(): ArrayBuffer {
    const size = this.variableSize();
    const buffer = new ArrayBuffer(SIZE_SIZE + size);
    const dataView = new DataView(buffer);
    dataView.setInt32(0, size);
    const uint8Array = new Uint8Array(buffer, SIZE_SIZE, size);
    new TextEncoder().encodeInto(this._value, uint8Array);
    return buffer;
  }
  castAs(type: Type): Value {
    switch (type) {
      case Type.BOOLEAN:
        return new BooleanValue(false);
      case Type.INTEGER:
        return new IntegerValue(0);
      case Type.VARCHAR:
        return new VarcharValue(this._value.toString());
    }
  }
  performEqual(right: VarcharValue): BooleanValue {
    return new BooleanValue(this._value === right.value);
  }
  performNotEqual(right: VarcharValue): BooleanValue {
    return new BooleanValue(this._value !== right.value);
  }
  performLessThan(right: VarcharValue): BooleanValue {
    return new BooleanValue(this._value < right.value);
  }
  performGreaterThan(right: VarcharValue): BooleanValue {
    return new BooleanValue(this._value > right.value);
  }
  performLessThanEqual(right: VarcharValue): BooleanValue {
    return new BooleanValue(this._value <= right.value);
  }
  performGreaterThanEqual(right: VarcharValue): BooleanValue {
    return new BooleanValue(this._value >= right.value);
  }
  toJSON() {
    return this._value;
  }
}
