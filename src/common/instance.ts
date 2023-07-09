import { Binder } from "../binder/binder";
import {
  BufferPoolManager,
  BufferPoolManagerImpl,
} from "../buffer/buffer_pool_manager";
import { Catalog } from "../catalog/catalog";
import { LockManager } from "../concurrency/lock_manager";
import { Transaction } from "../concurrency/transaction";
import { TransactionManager } from "../concurrency/transaction_manager";
import { ExecutorContext } from "../execution/executor_context";
import { ExecutorEngine } from "../execution/executor_engine";
import { plan } from "../execution/planner";
import { Parser } from "../parser/parser";
import { nextTransactionIdAndLsn, recover } from "../recovery/log_recovery";
import { LogManager } from "../recovery/log_manager";
import { DiskManager, DiskManagerImpl } from "../storage/disk/disk_manager";
import { Tuple } from "../storage/table/tuple";
import { optimize } from "../optimizer/optimizer";

export type SQLResult = {
  transactionId: number | null;
  rows: Tuple[];
};

export class Instance {
  private _diskManager: DiskManager;
  private _bufferPoolManager: BufferPoolManager;
  private _logManager: LogManager;
  private _lockManager: LockManager;
  private _transactionManager: TransactionManager;
  private _catalog: Catalog;
  private _executorEngine: ExecutorEngine;
  constructor() {
    this._diskManager = new DiskManagerImpl();
    this._bufferPoolManager = new BufferPoolManagerImpl(this._diskManager);
    this._logManager = new LogManager(this._diskManager);
    this._lockManager = new LockManager();
    this._transactionManager = new TransactionManager(
      this._lockManager,
      this._logManager
    );
    this._catalog = new Catalog(this._bufferPoolManager, this._logManager);
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
      await this._diskManager.createLogFile();
      const transaction = await this._transactionManager.begin();
      await this._catalog.initialize(transaction);
      await this._transactionManager.commit(transaction);
    }
    await this._catalog.setupNextOid();
    const [nextTransactionId, nextLsn] = await nextTransactionIdAndLsn(
      this._diskManager
    );
    this._transactionManager.nextTransactionId = nextTransactionId;
    this._logManager.nextLsn = nextLsn;

    await recover(this._diskManager, this._bufferPoolManager, this._catalog);
  }
  async executeSQL(
    sql: string,
    transactionId: number | null
  ): Promise<SQLResult> {
    const parser = new Parser(sql);
    const ast = parser.parse();

    const transaction: Transaction =
      transactionId == null
        ? await this._transactionManager.begin()
        : this._transactionManager.getTransaction(transactionId)!;
    console.log("transactionId", transaction.transactionId);
    switch (ast.type) {
      case "begin_statement":
        return {
          transactionId: transaction.transactionId,
          rows: [],
        };
      case "commit_statement":
        await this._transactionManager.commit(transaction);
        return {
          transactionId: null,
          rows: [],
        };
      case "rollback_statement":
        await this._transactionManager.abort(transaction);
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

    switch (statement.type) {
      case "create_table_statement":
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

    const planNode = plan(statement);
    const optimizedPlan = await optimize(planNode, this._catalog);
    const tuples = await this._executorEngine.execute(
      executorContext,
      optimizedPlan
    );
    if (transactionId == null) {
      await this._transactionManager.commit(transaction);
    }
    return {
      transactionId: transactionId == null ? null : transaction.transactionId,
      rows: tuples,
    };
  }
  async shutdown(): Promise<void> {
    await this._bufferPoolManager.flushAllPages();
    await this._logManager.flush();
  }
  toJSON() {
    return {
      bufferPoolManager: this._bufferPoolManager,
    };
  }
}
