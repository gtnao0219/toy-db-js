import { BooleanValue } from "./boolean_value";
import { IntegerValue } from "./integer_value";
import {
  Type,
  VARIABLE_VALUE_INLINE_OFFSET_SIZE,
  VARIABLE_VALUE_INLINE_SIZE,
} from "./type";

export abstract class Value {
  abstract get value(): any;
  abstract get type(): Type;
  abstract serialize(): ArrayBuffer;
  abstract castAs(type: Type): Value;

  add(right: Value): IntegerValue {
    const castedLeft = this.castAs(Type.INTEGER);
    const castedRight = right.castAs(Type.INTEGER);
    if (
      castedLeft instanceof IntegerValue &&
      castedRight instanceof IntegerValue
    ) {
      return castedLeft.performAdd(castedRight);
    }
    throw new Error(`Cannot add ${this.type} and ${right.type}`);
  }
  subtract(right: Value): IntegerValue {
    const castedLeft = this.castAs(Type.INTEGER);
    const castedRight = right.castAs(Type.INTEGER);
    if (
      castedLeft instanceof IntegerValue &&
      castedRight instanceof IntegerValue
    ) {
      return castedLeft.performSubtract(castedRight);
    }
    throw new Error(`Cannot subtract ${this.type} and ${right.type}`);
  }
  multiply(right: Value): IntegerValue {
    const castedLeft = this.castAs(Type.INTEGER);
    const castedRight = right.castAs(Type.INTEGER);
    if (
      castedLeft instanceof IntegerValue &&
      castedRight instanceof IntegerValue
    ) {
      return castedLeft.performMultiply(castedRight);
    }
    throw new Error(`Cannot subtract ${this.type} and ${right.type}`);
  }
  equal(right: Value): BooleanValue {
    if (this.type !== right.type) {
      throw new Error(`Cannot compare ${this.type} and ${right.type}`);
    }
    return this.performEqual(right);
  }
  notEqual(right: Value): BooleanValue {
    if (this.type !== right.type) {
      throw new Error(`Cannot compare ${this.type} and ${right.type}`);
    }
    return this.performNotEqual(right);
  }
  lessThan(right: Value): BooleanValue {
    if (this.type !== right.type) {
      throw new Error(`Cannot compare ${this.type} and ${right.type}`);
    }
    return this.performLessThan(right);
  }
  greaterThan(right: Value): BooleanValue {
    if (this.type !== right.type) {
      throw new Error(`Cannot compare ${this.type} and ${right.type}`);
    }
    return this.performGreaterThan(right);
  }
  lessThanEqual(right: Value): BooleanValue {
    if (this.type !== right.type) {
      throw new Error(`Cannot compare ${this.type} and ${right.type}`);
    }
    return this.performLessThanEqual(right);
  }
  greaterThanEqual(right: Value): BooleanValue {
    if (this.type !== right.type) {
      throw new Error(`Cannot compare ${this.type} and ${right.type}`);
    }
    return this.performGreaterThanEqual(right);
  }
  and(right: Value): Value {
    const castedLeft = this.castAs(Type.BOOLEAN);
    const castedRight = right.castAs(Type.BOOLEAN);
    if (
      castedLeft instanceof BooleanValue &&
      castedRight instanceof BooleanValue
    ) {
      return castedLeft.performAnd(castedRight);
    }
    throw new Error(`Cannot AND ${this.type} and ${right.type}`);
  }
  or(right: Value): Value {
    const castedLeft = this.castAs(Type.BOOLEAN);
    const castedRight = right.castAs(Type.BOOLEAN);
    if (
      castedLeft instanceof BooleanValue &&
      castedRight instanceof BooleanValue
    ) {
      return castedLeft.performOr(castedRight);
    }
    throw new Error(`Cannot OR ${this.type} and ${right.type}`);
  }
  not(): Value {
    const casted = this.castAs(Type.BOOLEAN);
    if (casted instanceof BooleanValue) {
      return casted.performNot();
    }
    throw new Error(`Cannot NOT ${this.type}`);
  }
  abstract performEqual(right: Value): BooleanValue;
  abstract performNotEqual(right: Value): BooleanValue;
  abstract performLessThan(right: Value): BooleanValue;
  abstract performGreaterThan(right: Value): BooleanValue;
  abstract performLessThanEqual(right: Value): BooleanValue;
  abstract performGreaterThanEqual(right: Value): BooleanValue;
}

export abstract class InlinedValue extends Value {}
export abstract class VariableValue extends Value {
  abstract size(): number;
  serializeInline(offset: number): ArrayBuffer {
    const buffer = new ArrayBuffer(VARIABLE_VALUE_INLINE_SIZE);
    const view = new DataView(buffer);
    view.setInt32(0, offset);
    view.setInt32(VARIABLE_VALUE_INLINE_OFFSET_SIZE, this.size());
    return buffer;
  }
  static deserializeInline(
    buffer: ArrayBuffer,
    offset: number
  ): {
    offset: number;
    size: number;
  } {
    const dataView = new DataView(buffer);
    const variable_offset = dataView.getInt32(offset);
    const variable_size = dataView.getInt32(
      offset + VARIABLE_VALUE_INLINE_OFFSET_SIZE
    );
    return { offset: variable_offset, size: variable_size };
  }
}
