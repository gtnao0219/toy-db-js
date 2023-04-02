export const TRANSACTION_ID_HEADER_NAME = "x-transaction-id";
export type Response = {
  transactionId: number | null;
  result: string;
};
export interface Debuggable {
  debug(): object;
}
