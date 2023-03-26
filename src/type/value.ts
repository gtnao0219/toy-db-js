import {
  VARIABLE_VALUE_INLINE_OFFSET_SIZE,
  VARIABLE_VALUE_INLINE_SIZE,
} from "./type";

export abstract class Value {
  abstract get value(): any;
  abstract serialize(): ArrayBuffer;
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
