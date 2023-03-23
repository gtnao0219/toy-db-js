export enum Type {
  INTEGER,
  BOOLEAN,
  STRING,
}
export function isInlined(type: Type): boolean {
  switch (type) {
    case Type.INTEGER:
    case Type.BOOLEAN:
      return true;
    case Type.STRING:
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
    case Type.STRING:
      return VARIABLE_VALUE_INLINE_SIZE;
  }
}
