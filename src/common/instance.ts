import { Binder } from "../binder/binder";
import { CreateTableStatement } from "../binder/statement/create_table_statement";
import { StatementType } from "../binder/statement/statement";
import { BufferPoolManager } from "../buffer/buffer_pool_manager";
import { Catalog } from "../catalog/catalog";
import { createExecutor } from "../execution/executor_factory";
import { getPlan } from "../execution/plan/planner";
import { Parser } from "../parser/parser";
import { DiskManager } from "../storage/disk/disk_manager";
import { Value } from "../type/value";

export class Instance {
  private _diskManager: DiskManager;
  private _bufferPoolManager: BufferPoolManager;
  private _catalog: Catalog;
  constructor() {
    this._diskManager = new DiskManager();
    const mustInitialize = !this._diskManager.existsDataFile();
    if (mustInitialize) {
      this._diskManager.createDataFile();
    }
    this._bufferPoolManager = new BufferPoolManager(this._diskManager);
    this._catalog = new Catalog(this._bufferPoolManager);
    if (mustInitialize) {
      this._catalog.initialize();
    }
  }
  executeSQL(sql: string): Value[][] {
    const parser = new Parser(sql);
    const ast = parser.parse();
    const binder = new Binder(this._catalog);
    const statement = binder.bind(ast);
    switch (statement.statementType) {
      case StatementType.CREATE_TABLE:
        if (!(statement instanceof CreateTableStatement)) {
          throw new Error("Invalid statement");
        }
        this._catalog.createTable(statement.tableName, statement.schema);
        return [];
    }
    const plan = getPlan(statement);
    const executor = createExecutor(
      this._catalog,
      this._bufferPoolManager,
      plan
    );
    return executor.next();
  }
  shutdown(): void {
    this._bufferPoolManager.flushAllPages();
  }
}
