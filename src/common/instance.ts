import { Binder } from "../binder/binder";
import { CreateTableStatement } from "../binder/statement/create_table_statement";
import { StatementType } from "../binder/statement/statement";
import { BufferPoolManager } from "../buffer/buffer_pool_manager";
import { Catalog } from "../catalog/catalog";
import { LockManager } from "../concurrency/lock_manager";
import { TransactionManager } from "../concurrency/transaction_manager";
import { ExecutorContext } from "../execution/executor_context";
import { ExecutorEngine } from "../execution/executor_engine";
import { planStatement } from "../execution/plan/planner";
import { Parser } from "../parser/parser";
import { DiskManager } from "../storage/disk/disk_manager";
import { Tuple } from "../storage/table/tuple";

export class Instance {
  private _diskManager: DiskManager;
  private _bufferPoolManager: BufferPoolManager;
  private _lockManager: LockManager;
  private _transactionManager: TransactionManager;
  private _catalog: Catalog;
  private _executorEngine: ExecutorEngine;
  constructor() {
    this._diskManager = new DiskManager();
    const mustInitialize = !this._diskManager.existsDataFile();
    if (mustInitialize) {
      this._diskManager.createDataFile();
    }
    this._bufferPoolManager = new BufferPoolManager(this._diskManager);
    this._lockManager = new LockManager();
    this._transactionManager = new TransactionManager(this._lockManager);
    this._catalog = new Catalog(this._bufferPoolManager);
    if (mustInitialize) {
      const transaction = this._transactionManager.begin();
      this._catalog.initialize(transaction);
      this._transactionManager.commit(transaction);
    }
    this._executorEngine = new ExecutorEngine(
      this._bufferPoolManager,
      this._transactionManager,
      this._catalog
    );
  }
  executeSQL(sql: string): Tuple[] {
    const parser = new Parser(sql);
    const ast = parser.parse();
    const binder = new Binder(this._catalog);
    const statement = binder.bind(ast);

    const transaction = this._transactionManager.begin();
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
        return [];
    }

    const plan = planStatement(statement);
    const result = this._executorEngine.execute(executorContext, plan);
    this._transactionManager.commit(transaction);
    return result;
  }
  shutdown(): void {
    this._bufferPoolManager.flushAllPages();
  }
}
