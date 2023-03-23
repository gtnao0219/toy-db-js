import { Schema } from "../../catalog/schema";
import {
  Type,
  VARIABLE_VALUE_INLINE_OFFSET_SIZE,
  typeSize,
} from "../../type/type";
import {
  StringValue,
  Value,
  deserializeBooleanValue,
  deserializeIntegerValue,
  deserializeStringValue,
  deserializeVariableValueInlineOffset,
  deserializeVariableValueInlineSize,
} from "../../type/value";

export type RID = {
  pageId: number;
  slotId: number;
};

export class Tuple {
  constructor(
    private _rid: RID | null,
    private _schema: Schema,
    private _values: Value[]
  ) {}
  get values(): Value[] {
    return this._values;
  }
  set rid(_rid: RID) {
    this._rid = _rid;
  }
  serialize(): ArrayBuffer {
    const inlineValuesSize = this._schema.columns.reduce(
      (acc, column) => acc + typeSize(column.type),
      0
    );
    const variableValuesSize = this._schema.columns.reduce((acc, _, index) => {
      const value = this._values[index];
      if (value instanceof StringValue) {
        return acc + value.size();
      }
      return acc;
    }, 0);
    const size = inlineValuesSize + variableValuesSize;
    const buffer = new ArrayBuffer(size);
    const dataView = new DataView(buffer);
    let offset = 0;
    let variableValueOffset = 0;
    this._values.forEach((value) => {
      if (value instanceof StringValue) {
        const inlineBuffer = value.serializeInline(variableValueOffset);
        const inlineDataView = new DataView(inlineBuffer);
        for (let i = 0; i < inlineDataView.byteLength; ++i) {
          dataView.setInt8(offset, inlineDataView.getInt8(i));
          ++offset;
        }
        const variableBuffer = value.serialize();
        const variableDataView = new DataView(variableBuffer);
        for (let i = 0; i < variableDataView.byteLength; ++i) {
          dataView.setInt8(
            inlineValuesSize + variableValueOffset,
            variableDataView.getInt8(i)
          );
          ++variableValueOffset;
        }
      } else {
        const valueBuffer = value.serialize();
        const valueDataView = new DataView(valueBuffer);
        for (let i = 0; i < valueBuffer.byteLength; ++i) {
          dataView.setInt8(offset, valueDataView.getInt8(i));
          ++offset;
        }
      }
    });
    return buffer;
  }
}

export function deserializeTuple(
  buffer: ArrayBuffer,
  rid: RID,
  schema: Schema
): Tuple {
  const inlineValuesSize = schema.columns.reduce(
    (acc, column) => acc + typeSize(column.type),
    0
  );
  const values: Value[] = [];
  let offset = 0;
  schema.columns.forEach((column) => {
    switch (column.type) {
      case Type.INTEGER: {
        values.push(deserializeIntegerValue(buffer, offset));
        offset += typeSize(Type.INTEGER);
        break;
      }
      case Type.BOOLEAN: {
        values.push(deserializeBooleanValue(buffer, offset));
        offset += typeSize(Type.INTEGER);
        break;
      }
      case Type.STRING: {
        const variableValueOffset = deserializeVariableValueInlineOffset(
          buffer,
          offset
        );
        const variableValueSize = deserializeVariableValueInlineSize(
          buffer,
          offset + VARIABLE_VALUE_INLINE_OFFSET_SIZE
        );
        values.push(
          deserializeStringValue(
            buffer,
            inlineValuesSize + variableValueOffset,
            variableValueSize
          )
        );
        offset += typeSize(Type.STRING);
        break;
      }
    }
  });
  return new Tuple(rid, schema, values);
}
