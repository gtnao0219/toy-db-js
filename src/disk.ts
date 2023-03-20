import { existsSync, writeSync, openSync, readSync } from "fs";
import { PAGE_SIZE } from "./page";

const DATA_FILE_NAME = "data";

export function initDataFile(): boolean {
  if (existsSync(DATA_FILE_NAME)) {
    return false;
  }
  const buffer = new ArrayBuffer(0);
  const view = new DataView(buffer);
  const fd = openSync(DATA_FILE_NAME, "w");
  writeSync(fd, view, 0, 0, 0);
  return true;
}
export function writePage(pageNumber: number, buffer: ArrayBuffer) {
  const view = new DataView(buffer);
  const fd = openSync(DATA_FILE_NAME, "w");
  writeSync(fd, view, 0, PAGE_SIZE, pageNumber * PAGE_SIZE);
}
export function readPage(pageNumber: number): ArrayBuffer {
  const buffer = new ArrayBuffer(PAGE_SIZE);
  const view = new DataView(buffer);
  const fd = openSync(DATA_FILE_NAME, "r");
  readSync(fd, view, 0, PAGE_SIZE, pageNumber * PAGE_SIZE);
  return buffer;
}
