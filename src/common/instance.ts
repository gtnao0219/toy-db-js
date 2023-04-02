import { Binder } from "../binder/binder";
import { CreateTableStatement } from "../binder/statement/create_table_statement";
import { StatementType } from "../binder/statement/statement";
import { BufferPoolManager } from "../buffer/buffer_pool_manager";
import { Catalog } from "../catalog/catalog";
import { LockManager } from "../concurrency/lock_manager";
import { Transaction } from "../concurrency/transaction";
import { TransactionManager } from "../concurrency/transaction_manager";
import { ExecutorContext } from "../execution/executor_context";
import { ExecutorEngine } from "../execution/executor_engine";
import { planStatement } from "../execution/plan/planner";
import { Parser } from "../parser/parser";
import { DiskManager } from "../storage/disk/disk_manager";
import { Tuple } from "../storage/table/tuple";
import { Debuggable } from "./common";

export type SQLResult = {
  transactionId: number | null;
  rows: Tuple[];
};

export class Instance implements Debuggable {
  private _diskManager: DiskManager;
  private _bufferPoolManager: BufferPoolManager;
  private _lockManager: LockManager;
  private _transactionManager: TransactionManager;
  private _catalog: Catalog;
  private _executorEngine: ExecutorEngine;
  constructor() {
    this._diskManager = new DiskManager();
    this._bufferPoolManager = new BufferPoolManager(this._diskManager);
    this._lockManager = new LockManager();
    this._transactionManager = new TransactionManager(this._lockManager);
    this._catalog = new Catalog(this._bufferPoolManager);
    this._executorEngine = new ExecutorEngine(
      this._bufferPoolManager,
      this._transactionManager,
      this._catalog
    );
  }
  async init(): Promise<void> {
    const mustInitialize = !this._diskManager.existsDataFile();
    if (mustInitialize) {
      await this._diskManager.createDataFile();
      const transaction = this._transactionManager.begin();
      await this._catalog.initialize(transaction);
      this._transactionManager.commit(transaction);
    }
  }
  async executeSQL(
    sql: string,
    transactionId: number | null
  ): Promise<SQLResult> {
    const parser = new Parser(sql);
    const ast = parser.parse();

    const transaction: Transaction =
      transactionId == null
        ? this._transactionManager.begin()
        : this._transactionManager.getTransaction(transactionId)!;
    console.log("transactionId", transaction.transactionId);
    switch (ast.type) {
      case "begin_stmt":
        return {
          transactionId: transaction.transactionId,
          rows: [],
        };
      case "commit_stmt":
        this._transactionManager.commit(transaction);
        return {
          transactionId: null,
          rows: [],
        };
      case "rollback_stmt":
        this._transactionManager.abort(transaction);
        return {
          transactionId: null,
          rows: [],
        };
    }

    const binder = new Binder(this._catalog);
    const statement = await binder.bind(ast);

    const executorContext = new ExecutorContext(
      transaction,
      this._bufferPoolManager,
      this._lockManager,
      this._transactionManager,
      this._catalog
    );

    switch (statement.statementType) {
      case StatementType.CREATE_TABLE:
        if (!(statement instanceof CreateTableStatement)) {
          throw new Error("Invalid statement");
        }
        this._catalog.createTable(
          statement.tableName,
          statement.schema,
          transaction
        );
        return {
          transactionId: transaction.transactionId,
          rows: [],
        };
    }

    const plan = planStatement(statement);
    const tuples = await this._executorEngine.execute(executorContext, plan);
    if (transactionId == null) {
      this._transactionManager.commit(transaction);
    }
    return {
      transactionId: transactionId == null ? null : transaction.transactionId,
      rows: tuples,
    };
  }
  async shutdown(): Promise<void> {
    await this._bufferPoolManager.flushAllPages();
  }
  debug(): object {
    return {
      // bufferPoolManager: this._bufferPoolManager.debug(),
      lockManager: this._lockManager.debug(),
      transactionManager: this._transactionManager.debug(),
    };
  }
}
