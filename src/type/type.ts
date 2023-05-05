export enum Type {
  INTEGER,
  BOOLEAN,
  VARCHAR,
}
export function isInlined(type: Type): boolean {
  switch (type) {
    case Type.INTEGER:
    case Type.BOOLEAN:
      return true;
    case Type.VARCHAR:
      return false;
  }
}
export const VARIABLE_VALUE_INLINE_OFFSET_SIZE = 4;
export const VARIABLE_VALUE_INLINE_SIZE_SIZE = 4;
export const VARIABLE_VALUE_INLINE_SIZE =
  VARIABLE_VALUE_INLINE_OFFSET_SIZE + VARIABLE_VALUE_INLINE_SIZE_SIZE;
export function typeSize(type: Type): number {
  switch (type) {
    case Type.INTEGER:
      return 4;
    case Type.BOOLEAN:
      return 1;
    case Type.VARCHAR:
      return VARIABLE_VALUE_INLINE_SIZE;
  }
}
export function string2Type(str: string): Type {
  switch (str.toUpperCase()) {
    case "INTEGER":
      return Type.INTEGER;
    case "BOOLEAN":
      return Type.BOOLEAN;
    case "VARCHAR":
      return Type.VARCHAR;
    default:
      throw new Error(`Unknown type: ${str}`);
  }
}
