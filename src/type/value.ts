import {
  VARIABLE_VALUE_INLINE_OFFSET_SIZE,
  VARIABLE_VALUE_INLINE_SIZE,
} from "./type";

export abstract class Value {
  abstract serialize(): ArrayBuffer;
  abstract get value(): any;
}
export class IntegerValue extends Value {
  constructor(private _value: number) {
    super();
  }
  get value(): number {
    return this._value;
  }
  serialize(): ArrayBuffer {
    const buffer = new ArrayBuffer(4);
    const view = new DataView(buffer);
    view.setInt32(0, this._value);
    return buffer;
  }
}
export function deserializeIntegerValue(
  buffer: ArrayBuffer,
  offset: number
): IntegerValue {
  const dataView = new DataView(buffer);
  const value = dataView.getInt32(offset);
  return new IntegerValue(value);
}
export class BooleanValue extends Value {
  constructor(private _value: boolean) {
    super();
  }
  get value(): boolean {
    return this._value;
  }
  serialize(): ArrayBuffer {
    const buffer = new ArrayBuffer(1);
    const view = new DataView(buffer);
    view.setInt8(0, this._value ? 1 : 0);
    return buffer;
  }
}
export function deserializeBooleanValue(
  buffer: ArrayBuffer,
  offset: number
): BooleanValue {
  const dataView = new DataView(buffer);
  const value = dataView.getInt8(offset);
  return new BooleanValue(value === 1);
}
export class StringValue extends Value {
  constructor(private _value: string) {
    super();
  }
  get value(): string {
    return this._value;
  }
  size(): number {
    return new TextEncoder().encode(this._value).length;
  }
  serializeInline(offset: number): ArrayBuffer {
    const uint8Array = new TextEncoder().encode(this._value);
    const buffer = new ArrayBuffer(VARIABLE_VALUE_INLINE_SIZE);
    const view = new DataView(buffer);
    view.setInt32(0, offset);
    view.setInt32(VARIABLE_VALUE_INLINE_OFFSET_SIZE, uint8Array.length);
    return buffer;
  }
  serialize(): ArrayBuffer {
    const uint8Array = new TextEncoder().encode(this._value);
    const buffer = new ArrayBuffer(uint8Array.length);
    const view = new Uint8Array(buffer);
    view.set(uint8Array);
    return buffer;
  }
}
export function deserializeVariableValueInlineOffset(
  buffer: ArrayBuffer,
  offset: number
): number {
  const dataView = new DataView(buffer);
  return dataView.getInt32(offset);
}
export function deserializeVariableValueInlineSize(
  buffer: ArrayBuffer,
  offset: number
): number {
  const dataView = new DataView(buffer);
  return dataView.getInt32(offset);
}
export function deserializeStringValue(
  buffer: ArrayBuffer,
  offset: number,
  length: number
): StringValue {
  const dataView = new Uint8Array(buffer);
  const value = new TextDecoder().decode(
    dataView.slice(offset, offset + length)
  );
  return new StringValue(value);
}
