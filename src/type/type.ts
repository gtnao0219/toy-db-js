export enum Type {
  INTEGER,
  BOOLEAN,
}

export function typeSize(type: Type): number {
  switch (type) {
    case Type.INTEGER:
      return 4;
    case Type.BOOLEAN:
      return 1;
  }
}
