import { Client } from "ts-postgres";
import { SchemaType, Translator, processSchema } from "./schema";
import { z } from "zod";

export type TableName<Schema extends SchemaType> = Exclude<
  keyof Schema,
  "events"
>;
export type SchemaRow<
  Schema extends SchemaType,
  Name extends TableName<Schema>
> = {
  [key in keyof Schema[Name]]: Translator[Schema[Name][key]];
};

export type Mutator<Schema extends SchemaType> = {
  [key: string]: { table: TableName<Schema>; id: string };
};

export type MutatorData<
  Schema extends SchemaType,
  T extends Mutator<Schema>
> = {
  [key in keyof T]?: SchemaRow<Schema, T[key]["table"]> & { id: string };
};

export type CommandBase<
  Schema extends SchemaType,
  Input,
  T extends Mutator<Schema>
> = {
  name: string;
  input: z.ZodType<Input>;
  mutator: (input: Input) => T;
};

export type CreatorCommand<
  Schema extends SchemaType,
  Input,
  T extends Mutator<Schema>
> = CommandBase<Schema, Input, T> & {
  validate: (
    client: Client,
    input: Input,
    mutator: MutatorData<Schema, T>
  ) => Promise<void>;
};

export type UpdaterCommand<
  Schema extends SchemaType,
  Input,
  T extends Mutator<Schema>
> = CommandBase<Schema, Input, T> & {
  validate: (
    client: Client,
    input: Input,
    mutator: MutatorData<Schema, T>
  ) => Promise<string>;
};

export type DispatchCommand<
  Schema extends SchemaType,
  Input,
  T extends Mutator<Schema>
> = CommandBase<Schema, Input, T> & {
  planActions: (
    client: Client,
    input: Input,
    mutator: MutatorData<Schema, T>
  ) => Promise<ESEvent[]>;
};

// Instead of declaring really complicated types for everything,
// we use symbols to validate that the data we're getting is correct, and
// then otherwise use raw types like `any`.
const _event_name: unique symbol = Symbol("event-name");

export type EventInfo = {
  name: string;
};

export type CreationEvent = {
  [_event_name]: string;

  kind: "create";
  table: string;
  data: any;
};

export type ModificationEvent = {
  [_event_name]: string;

  kind: "modify";
  rowId: string;
  table: string;
  data: any;
};

export type ESEvent = CreationEvent | ModificationEvent | undefined;

export type CreateInfo<
  Schema extends SchemaType,
  Data extends object
> = EventInfo & {
  kind: "create";
  event(data: Data & { table: keyof Schema }): CreationEvent;
};

export type CreateInfoD<
  Schema extends SchemaType,
  Data extends object
> = EventInfo & {
  kind: "create";
  table: TableName<Schema>;
  event(data: Data): CreationEvent;
};

export type ModifyInfo<
  Schema extends SchemaType,
  Data extends object
> = EventInfo & {
  kind: "modify";
  event(id: string, data: Data & { table: keyof Schema }): ModificationEvent;
};

export type ModifyInfoD<
  Schema extends SchemaType,
  Data extends object
> = EventInfo & {
  kind: "modify";
  table: TableName<Schema>;
  event(id: string, data: Data): ModificationEvent;
};

export type Reducer<
  Schema extends SchemaType,
  Table extends TableName<Schema>
> = {
  creator<Data extends object>(
    eventInfo: CreateInfo<Schema, Data>,
    creator: (data: Data) => SchemaRow<Schema, Table>
  ): void;

  updater<Data extends object>(
    eventInfo: ModifyInfo<Schema, Data>,
    updater: (
      previous: SchemaRow<Schema, Table>,
      data: Data
    ) => Partial<SchemaRow<Schema, Table>>
  ): void;
};

// NOTE: using a prepared statement here causes a crash.
const addEvent = `
  INSERT INTO events
  (name, table_name, row_id, actor_id, data, created_at)
  VALUES (
    $1, $2, $3::uuid,
    $4::uuid, $5::jsonb, $6::timestamp
  )
`;

const selectEvents = `
  SELECT name, data FROM events
  WHERE table_name = $1 AND row_id = $2::uuid
  ORDER BY id ASC
`;

export function eventSourcing<Schema extends SchemaType>(schema: Schema) {
  type Mut = Mutator<Schema>;
  type TName = TableName<Schema>;

  const {
    namesForTable,
    selectUpdateForTable,
    createForTable,
    updateForTable,
  } = processSchema(schema);

  const reducers: Record<string, Record<string, any>> = {};
  const commandNames: Record<string, true> = {};

  function createCommand<Input, T extends Mut>(
    command: DispatchCommand<Schema, Input, T>
  ): DispatchCommand<Schema, Input, T>;

  function createCommand<
    Table extends TName,
    Input extends object,
    T extends Mut
  >(
    command: CreatorCommand<Schema, Input, T> & { kind: "create" }
  ): DispatchCommand<Schema, Input, T> & CreateInfo<Schema, Input>;
  function createCommand<
    Table extends TName,
    Input extends object,
    T extends Mut
  >(
    command: CreatorCommand<Schema, Input, T> & { kind: "create"; table: Table }
  ): DispatchCommand<Schema, Input, T> & CreateInfoD<Schema, Input>;

  function createCommand<
    Table extends TName,
    Input extends object,
    T extends Mut
  >(
    command: UpdaterCommand<Schema, Input, T> & { kind: "modify" }
  ): DispatchCommand<Schema, Input, T> & ModifyInfo<Schema, Input>;
  function createCommand<
    Table extends TName,
    Input extends object,
    T extends Mut
  >(
    command: UpdaterCommand<Schema, Input, T> & { kind: "modify"; table: Table }
  ): DispatchCommand<Schema, Input, T> & ModifyInfoD<Schema, Input>;

  function createCommand(command: any): any {
    const { name, kind, validate, table: commandTable } = command;

    if (commandNames[name]) {
      throw new Error(`Duplicate command name ${name}`);
    }

    commandNames[name] = true;

    const creatorEvent = (data_: any): any => {
      const { table, ...data } = data_;

      return { [_event_name]: name, kind, data, table: commandTable ?? table };
    };

    const updaterEvent = (rowId: string, data_: any): any => {
      const { table, ...data } = data_;

      return {
        [_event_name]: name,
        kind,
        data,
        rowId,
        table: commandTable ?? table,
      };
    };

    const creatorPlanActions = async (
      client: any,
      input: any,
      mutator: any
    ): Promise<any> => {
      await validate(client, input, mutator);

      return [creatorEvent(input)];
    };

    const updaterPlanActions = async (
      client: any,
      input: any,
      mutator: any
    ): Promise<any> => {
      const id = await validate(client, input, mutator);

      return [updaterEvent(id, input)];
    };

    if (command.kind === "create") {
      return {
        planActions: creatorPlanActions,
        ...command,
        event: creatorEvent,
      };
    }

    if (command.kind === "modify") {
      return {
        planActions: updaterPlanActions,
        ...command,
        event: updaterEvent,
      };
    }

    return command;
  }

  function addReducer(table: string, name: string, reducer: any) {
    reducers[table] ||= {};

    if (reducers[table]![name]) {
      console.log(name, table);
      throw new Error("Duplicate event handler for table");
    }

    reducers[table]![name] = reducer;
  }

  class ReducerObject<Table extends TName> {
    private readonly table: string;
    constructor(table: Table) {
      this.table = String(table);
    }

    creator<Data extends object>(
      eventInfo: CreateInfo<Schema, Data>,
      creator: (data: Data) => SchemaRow<Schema, Table>
    ): Reducer<Schema, Table> {
      addReducer(this.table, eventInfo.name, {
        kind: "create",
        creator,
      });

      return this;
    }

    updater<Data extends object>(
      eventInfo: ModifyInfo<Schema, Data>,
      updater: (
        previous: SchemaRow<Schema, Table>,
        data: Data
      ) => Partial<SchemaRow<Schema, Table>>
    ): Reducer<Schema, Table> {
      addReducer(this.table, eventInfo.name, {
        kind: "modify",
        updater,
      });

      return this;
    }
  }

  function createReducer<Table extends TName>(
    table: Table
  ): Reducer<Schema, Table> {
    return new ReducerObject(table);
  }

  async function runCommand<Input>(
    client: Client,
    command: DispatchCommand<Schema, Input, any>,
    input: Input,
    actorId: string
  ): Promise<void> {
    const now = new Date();

    // TODO: certain parts of this function can be "parallelized" (really concurrent-ized)
    try {
      await client.query("begin");

      const mutator: Mut = command.mutator(input);

      const selectedObjects: any = {};
      const mutatorData: any = {};
      for (const field in mutator) {
        const { table, id } = mutator[field];

        const query: string = selectUpdateForTable[table];

        const result = await client.query(query, [id]);
        const data = result.rows[0];
        if (!data) {
          continue;
        }

        const outputData: any = { id };
        namesForTable[table].forEach((sqlField, index) => {
          outputData[sqlField] = data[index];
        });

        selectedObjects[id] = outputData;
        mutatorData[field] = outputData;
      }

      const events = await command.planActions(client, input, mutatorData);

      for (const event of events) {
        if (!event) continue;

        const eventName = event[_event_name];
        if (!eventName) {
          // this is an invalid event
          console.error("Received invalid event");
          continue;
        }

        const table = event.table;

        const reducer = reducers[table]?.[eventName];

        if (!reducer) continue;

        if (reducer.kind === "create" && event.kind === "create") {
          const names = namesForTable[table];
          const insertQuery = createForTable[table];

          const data = reducer.creator(event.data);
          const rowData = names.map((name) => (data[name] ?? null) as any);

          const result = await client.query(insertQuery, rowData);
          const id = result.rows[0]?.[0];
          if (typeof id !== "string") {
            throw new Error("failed to get ID from data");
          }

          await client.query(addEvent, [
            eventName,
            table,
            id,
            actorId,
            event.data,
            now,
          ]);

          continue;
        }

        if (reducer.kind === "modify" && event.kind === "modify") {
          const names = namesForTable[table];
          const updateQuery = updateForTable[table];

          const prev = selectedObjects[event.rowId];
          if (!prev) {
            throw new Error(
              "Tried to update object that wasn't requested in mutator"
            );
          }

          const data = reducer.updater(prev, event.data);
          selectedObjects[event.rowId] = {
            ...selectedObjects[event.rowId],
            ...data,
          };

          const rowData = names.map((name) => (data[name] ?? null) as any);
          rowData.push(event.rowId ?? null);

          await client.query(updateQuery, rowData);
          await client.query(addEvent, [
            eventName,
            table,
            event.rowId,
            actorId,
            event.data,
            now,
          ]);

          continue;
        }

        console.error(
          "reached potentially invalid state: reducer and event don't match"
        );
      }

      await client.query("commit");
    } catch (e: any) {
      console.log("Failed: ", e);
      await client.query("rollback");
    } finally {
    }
  }

  async function reduceRow<Table extends TName>(
    client: Client,
    table: Table,
    rowId: string
  ): Promise<SchemaRow<Schema, Table>> {
    try {
      await client.query("begin");

      const result = await client.query(selectEvents, [String(table), rowId]);
      for (const [name, data] of result.rows) {
        console.log(name, data);
      }

      await client.query("commit");

      return {} as unknown as any;
    } catch (e: any) {
      console.log("Failed: ", e);
      await client.query("rollback");

      return {} as unknown as any;
    } finally {
    }
  }

  return {
    createCommand,
    createReducer,
    runCommand,
    reduceRow,
  };
}
