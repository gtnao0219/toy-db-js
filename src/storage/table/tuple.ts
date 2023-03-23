import { Schema } from "../../catalog/schema";
import { Type, typeSize } from "../../type/type";
import {
  Value,
  deserializeBooleanValue,
  deserializeIntegerValue,
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
    // TODO: variable length
    const size = this._schema.columns.reduce(
      (acc, column) => acc + typeSize(column.type),
      0
    );
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
}

export function deserializeTuple(
  buffer: ArrayBuffer,
  rid: RID,
  schema: Schema
): Tuple {
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
    }
  });
  return new Tuple(rid, schema, values);
}
