import { IntegerValue } from "./integer_value";
import {
  Type,
  VARIABLE_VALUE_INLINE_OFFSET_SIZE,
  VARIABLE_VALUE_INLINE_SIZE,
  isCompatible,
  priority,
} from "./type";

type Operator =
  | "="
  | "<>"
  | "<"
  | ">"
  | "<="
  | ">="
  | "AND"
  | "OR"
  | "NOT"
  | "+"
  | "-"
  | "*"
  | "/"
  | "||";

export abstract class Value {
  abstract get value(): any;
  abstract get type(): Type;
  abstract serialize(): ArrayBuffer;
  abstract castAs(type: Type): Value;
  add(right: Value): Value {
    const [castedLeft, castedRight] = this.castBasedOnPriority(right);
    if (
      castedLeft instanceof IntegerValue &&
      castedRight instanceof IntegerValue
    ) {
      return castedLeft.performAdd(castedRight);
    }
    throw new Error(`Cannot add ${this.type} and ${right.type}`);
  }
  subtract(right: Value): Value {
    const [castedLeft, castedRight] = this.castBasedOnPriority(right);
    if (
      castedLeft instanceof IntegerValue &&
      castedRight instanceof IntegerValue
    ) {
      return castedLeft.performSubtract(castedRight);
    }
    throw new Error(`Cannot subtract ${this.type} and ${right.type}`);
  }
  castBasedOnPriority(right: Value): [Value, Value] {
    if (this.type === right.type) {
      return [this, right];
    }
    if (priority(this.type) > priority(right.type)) {
      return [this, right.castAs(this.type)];
    } else {
      return [this.castAs(right.type), right];
    }
  }
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
