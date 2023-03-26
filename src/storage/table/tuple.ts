import { Schema } from "../../catalog/schema";
import { BooleanValue } from "../../type/boolean_value";
import { IntegerValue } from "../../type/integer_value";
import { StringValue } from "../../type/string_value";
import { Type, typeSize } from "../../type/type";
import { Value, VariableValue } from "../../type/value";

export class Tuple {
  constructor(private _schema: Schema, private _values: Value[]) {}
  get values(): Value[] {
    return this._values;
  }
  serialize(): ArrayBuffer {
    const inlineValuesSize = this._schema.columns.reduce(
      (acc, column) => acc + typeSize(column.type),
      0
    );
    const variableValuesSize = this._schema.columns.reduce((acc, _, index) => {
      const value = this._values[index];
      if (value instanceof VariableValue) {
        return acc + value.size();
      }
      return acc;
    }, 0);
    const size = inlineValuesSize + variableValuesSize;
    const buffer = new ArrayBuffer(size);
    const dataView = new DataView(buffer);
    let offset = 0;
    let variableValueOffset = inlineValuesSize;
    this._values.forEach((value) => {
      if (value instanceof VariableValue) {
        const inlineBuffer = value.serializeInline(variableValueOffset);
        const inlineDataView = new DataView(inlineBuffer);
        for (let i = 0; i < inlineDataView.byteLength; ++i) {
          dataView.setInt8(offset, inlineDataView.getInt8(i));
          ++offset;
        }
        const variableBuffer = value.serialize();
        const variableDataView = new DataView(variableBuffer);
        for (let i = 0; i < variableDataView.byteLength; ++i) {
          dataView.setInt8(variableValueOffset, variableDataView.getInt8(i));
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

export function deserializeTuple(buffer: ArrayBuffer, schema: Schema): Tuple {
  const values: Value[] = [];
  let offset = 0;
  schema.columns.forEach((column) => {
    switch (column.type) {
      case Type.INTEGER: {
        values.push(IntegerValue.deserialize(buffer, offset));
        offset += typeSize(Type.INTEGER);
        break;
      }
      case Type.BOOLEAN: {
        values.push(BooleanValue.deserialize(buffer, offset));
        offset += typeSize(Type.INTEGER);
        break;
      }
      case Type.STRING: {
        values.push(StringValue.deserialize(buffer, offset));
        offset += typeSize(Type.STRING);
        break;
      }
    }
  });
  return new Tuple(schema, values);
}
