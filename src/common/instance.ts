import { Binder } from "../binder/binder";
import {
  BufferPoolManager,
  BufferPoolManagerImpl,
} from "../buffer/buffer_pool_manager";
import { Catalog, CatalogImpl } from "../catalog/catalog";
import { LockManager, LockManagerImpl } from "../concurrency/lock_manager";
import { Transaction } from "../concurrency/transaction";
import {
  TransactionManager,
  TransactionManagerImpl,
} from "../concurrency/transaction_manager";
import { ExecutorEngine } from "../execution/executor_engine";
import { plan } from "../execution/planner";
import { Parser } from "../parser/parser";
import { recover } from "../recovery/log_recovery";
import { LogManager, LogManagerImpl } from "../recovery/log_manager";
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
  private _catalog: Catalog;
  private _lockManager: LockManager;
  private _transactionManager: TransactionManager;
  private _executorEngine: ExecutorEngine;
  constructor() {
    this._diskManager = new DiskManagerImpl();
    this._bufferPoolManager = new BufferPoolManagerImpl(this._diskManager);
    this._logManager = new LogManagerImpl(this._diskManager);
    this._lockManager = new LockManagerImpl();
    this._transactionManager = new TransactionManagerImpl(
      this._lockManager,
      this._logManager
    );
    this._catalog = new CatalogImpl(
      this._bufferPoolManager,
      this._logManager,
      this._transactionManager
    );
    this._executorEngine = new ExecutorEngine(
      this._bufferPoolManager,
      this._catalog,
      this._lockManager,
      this._transactionManager
    );
  }
  async bootstrap(): Promise<void> {
    await this._diskManager.bootstrap();
    await this._logManager.bootstrap();
    await this._transactionManager.bootstrap();
    await this._catalog.bootstrap(await this._diskManager.isEmpty());
    await recover(this._bufferPoolManager, this._logManager, this._catalog);
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

    switch (statement.type) {
      case "create_table_statement":
        await this._catalog.createTable(
          statement.tableName,
          statement.schema,
          transaction
        );
        if (transactionId == null) {
          await this._transactionManager.commit(transaction);
        }
        return {
          transactionId:
            transactionId == null ? null : transaction.transactionId,
          rows: [],
        };
    }

    const planNode = plan(statement);
    const optimizedPlan = await optimize(planNode, this._catalog);
    const tuples = await this._executorEngine.execute(
      transaction.transactionId,
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
