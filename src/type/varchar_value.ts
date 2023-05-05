import { BooleanValue } from "./boolean_value";
import { IntegerValue } from "./integer_value";
import { Type } from "./type";
import { Value, VariableValue } from "./value";

export class VarcharValue extends VariableValue {
  constructor(private _value: string) {
    super();
  }
  static deserialize(buffer: ArrayBuffer, offset: number): VarcharValue {
    const { offset: variableOffset, size: variableSize } =
      this.deserializeInline(buffer, offset);
    const value = new TextDecoder().decode(
      buffer.slice(variableOffset, variableOffset + variableSize)
    );
    return new VarcharValue(value);
  }
  get value(): string {
    return this._value;
  }
  get type(): Type {
    return Type.VARCHAR;
  }
  size(): number {
    return new TextEncoder().encode(this._value).length;
  }
  serialize(): ArrayBuffer {
    const uint8Array = new TextEncoder().encode(this._value);
    const buffer = new ArrayBuffer(uint8Array.length);
    const dataView = new Uint8Array(buffer);
    dataView.set(uint8Array);
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
}
