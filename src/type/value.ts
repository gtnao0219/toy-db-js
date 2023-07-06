import { BooleanValue } from "./boolean_value";
import { IntegerValue } from "./integer_value";
import { Type } from "./type";

export abstract class Value {
  abstract get value(): any;
  abstract get type(): Type;
  abstract size(): number;
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
