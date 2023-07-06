import { Schema } from "../../catalog/schema";
import { BooleanValue } from "../../type/boolean_value";
import { IntegerValue } from "../../type/integer_value";
import { Type } from "../../type/type";
import { Value } from "../../type/value";
import { VarcharValue } from "../../type/varchar_value";

export class Tuple {
  constructor(private _schema: Schema, private _values: Value[]) {}
  get values(): Value[] {
    return this._values;
  }
  get schema(): Schema {
    return this._schema;
  }
  serialize(): ArrayBuffer {
    const size = this._values.reduce((acc, value) => {
      return acc + value.size();
    }, 0);
    const buffer = new ArrayBuffer(size);
    const dataView = new DataView(buffer);
    let offset = 0;
    this._values.forEach((value) => {
      const valueBuffer = value.serialize();
      const valueDataView = new DataView(valueBuffer);
      for (let i = 0; i < valueBuffer.byteLength; ++i) {
        dataView.setInt8(offset, valueDataView.getInt8(i));
        ++offset;
      }
    });
    return buffer;
  }
  static deserialize(buffer: ArrayBuffer, schema: Schema): Tuple {
    const values: Value[] = [];
    let offset = 0;
    schema.columns.forEach((column) => {
      switch (column.type) {
        case Type.INTEGER: {
          const value = IntegerValue.deserialize(buffer, offset);
          values.push(value);
          offset += value.size();
          break;
        }
        case Type.BOOLEAN: {
          const value = BooleanValue.deserialize(buffer, offset);
          values.push(value);
          offset += value.size();
          break;
        }
        case Type.VARCHAR: {
          const value = VarcharValue.deserialize(buffer, offset);
          values.push(value);
          offset += value.size();
          break;
        }
      }
    });
    return new Tuple(schema, values);
  }
  toJSON() {
    return {
      values: this._values,
    };
  }
}
