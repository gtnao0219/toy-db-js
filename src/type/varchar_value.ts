import { Type } from "./type";
import { VariableValue } from "./value";

export class VarcharValue extends VariableValue {
  private _type = Type.VARCHAR;
  constructor(private _value: string) {
    super();
  }
  static of(str: string): VarcharValue {
    return new VarcharValue(str);
  }
  static deserialize(buffer: ArrayBuffer, offset: number): VarcharValue {
    const { offset: variable_offset, size: variable_size } =
      this.deserializeInline(buffer, offset);
    const value = new TextDecoder().decode(
      buffer.slice(variable_offset, variable_offset + variable_size)
    );
    return new VarcharValue(value);
  }
  get value(): string {
    return this._value;
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
}
