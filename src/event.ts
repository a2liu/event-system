import { DataType, Client, Query, AnyJson } from "ts-postgres";
import {SchemaType, Translator, processSchema} from './schema';
import { z } from "zod";

// Instead of declaring really complicated types for everything,
// we use symbols to validate that the data we're getting is correct, and
// then otherwise use raw types like `any`.
const _event_name: unique symbol = Symbol("event-name");

type EventInfo = {
  name: string;
};

type CreationEvent = {
  [_event_name]: string;

  kind: "create";
  table: string;
  data: any;
};

type ModificationEvent = {
  [_event_name]: string;

  kind: "modify";
  rowId: string;
  table: string;
  data: any;
};

export type ESEvent = CreationEvent | ModificationEvent | undefined;

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
  const {
    namesForTable,
    selectUpdateForTable,
    createForTable,
    updateForTable,
  } = processSchema(schema);

  const reducers: Record<string, Record<string, any>> = {};
  const commandNames: Record<string, true> = {};

  type TName = Exclude<keyof Schema, "events">;
  type SchemaRow<Name extends TName> = {
    [key in keyof Schema[Name]]: Translator[Schema[Name][key]];
  };

  type Mutator = { [key: string]: { table: TName; id: string } };

  type MutatorData<T extends Mutator> = {
    [key in keyof T]?: SchemaRow<T[key]["table"]> & { id: string };
  };

  type CommandBase<Input, T extends Mutator> = {
    name: string;
    input: z.ZodType<Input>;
    mutator: (input: Input) => T;
  };

  type CreatorCommand<Input, T extends Mutator> = CommandBase<Input, T> & {
    validate: (
      client: Client,
      input: Input,
      mutator: MutatorData<T>
    ) => Promise<void>;
  };

  type UpdaterCommand<Input, T extends Mutator> = CommandBase<Input, T> & {
    validate: (
      client: Client,
      input: Input,
      mutator: MutatorData<T>
    ) => Promise<string>;
  };

  type DispatchCommand<Input, T extends Mutator> = CommandBase<Input, T> & {
    planActions: (
      client: Client,
      input: Input,
      mutator: MutatorData<T>
    ) => Promise<ESEvent[]>;
  };

  type CreateInfo<Data extends object> = EventInfo & {
    kind: "create";
    event(data: Data & { table: keyof Schema }): CreationEvent;
  };

  type CreateInfoD<Data extends object> = EventInfo & {
    kind: "create";
    table: TName;
    event(data: Data): CreationEvent;
  };

  type ModifyInfo<Data extends object> = EventInfo & {
    kind: "modify";
    event(id: string, data: Data & { table: keyof Schema }): ModificationEvent;
  };

  type ModifyInfoD<Data extends object> = EventInfo & {
    kind: "modify";
    table: TName;
    event(id: string, data: Data): ModificationEvent;
  };

  function createCommand<Input, T extends Mutator>(
    command: DispatchCommand<Input, T>
  ): DispatchCommand<Input, T>;

  function createCommand<
    Table extends TName,
    Input extends object,
    T extends Mutator
  >(
    command: CreatorCommand<Input, T> & { kind: "create" }
  ): DispatchCommand<Input, T> & CreateInfo<Input>;
  function createCommand<
    Table extends TName,
    Input extends object,
    T extends Mutator
  >(
    command: CreatorCommand<Input, T> & { kind: "create"; table: Table }
  ): DispatchCommand<Input, T> & CreateInfoD<Input>;

  function createCommand<
    Table extends TName,
    Input extends object,
    T extends Mutator
  >(
    command: UpdaterCommand<Input, T> & { kind: "modify" }
  ): DispatchCommand<Input, T> & ModifyInfo<Input>;
  function createCommand<
    Table extends TName,
    Input extends object,
    T extends Mutator
  >(
    command: UpdaterCommand<Input, T> & { kind: "modify"; table: Table }
  ): DispatchCommand<Input, T> & ModifyInfoD<Input>;

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

  class Reducer<Table extends TName> {
    private readonly table: string;
    constructor(table: Table) {
      this.table = String(table);
    }

    creator<Data extends object>(
      eventInfo: CreateInfo<Data>,
      creator: (data: Data) => SchemaRow<Table>
    ): void {
      addReducer(this.table, eventInfo.name, {
        kind: "create",
        creator,
      });
    }

    updater<Data extends object>(
      eventInfo: ModifyInfo<Data>,
      updater: (
        previous: SchemaRow<Table>,
        data: Data
      ) => Partial<SchemaRow<Table>>
    ): void {
      addReducer(this.table, eventInfo.name, {
        kind: "modify",
        updater,
      });
    }
  }

  async function runCommand<Input>(
    client: Client,
    command: DispatchCommand<Input, any>,
    input: Input,
    actorId: string
  ): Promise<void> {
    const now = new Date();

    // TODO: certain parts of this function can be "parallelized" (really concurrent-ized)
    try {
      await client.query("begin");

      const mutator: Mutator = command.mutator(input);

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
  ): Promise<SchemaRow<Table>> {
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
    Reducer,
    runCommand,
    reduceRow,
  };
}
