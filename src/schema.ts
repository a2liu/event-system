import { DataType, AnyJson } from "ts-postgres";

export type SchemaType = {
  [key: string]: Record<string, DataType>;
  events: {
    created_at: DataType.Timestamp;
    name: DataType.Text;
    table_name: DataType.Text;
    row_id: DataType.Uuid;
    actor_id: DataType.Uuid;
    data: DataType.Jsonb;
  };
};

export type Translator = PgTypes &
  Omit<{ [key in DataType]: void }, keyof PgTypes>;
export type PgTypes = {
  [DataType.Bool]: boolean;
  [DataType.Int8]: number;
  [DataType.Int2]: number;
  [DataType.Int4]: number;
  [DataType.Uuid]: string;
  [DataType.Text]: string;
  [DataType.Json]: AnyJson;
  [DataType.Timestamp]: Date;
  [DataType.ArrayJson]: AnyJson[];
  [DataType.Jsonb]: AnyJson;
  [DataType.ArrayJsonb]: AnyJson[];
};

type SchemaInfo<Schema extends SchemaType> = {
  namesForTable: Record<keyof Schema, string[]>;
  selectUpdateForTable: Record<keyof Schema, string>;
  createForTable: Record<keyof Schema, string>;
  updateForTable: Record<keyof Schema, string>;
};

export function processSchema<Schema extends SchemaType>(
  schema: Schema
): SchemaInfo<Schema> {
  const namesForTable: any = {};
  const selectUpdateForTable: any = {};
  const createForTable: any = {};
  const updateForTable: any = {};

  for (const table in schema) {
    if (table === "events") {
      continue;
    }

    const names = (namesForTable[table] = Object.keys(schema[table]));

    selectUpdateForTable[table] = `
      SELECT ${names.join(",")}
      FROM ${String(table)}
      WHERE id = $1::uuid FOR UPDATE
    `;

    const insertionValues = names.map((_name, i) => `$${i + 1}`);
    createForTable[table] = `
      INSERT INTO ${String(table)}
      (${names.join(",")})
      VALUES (${insertionValues.join(",")})
      RETURNING id::text
    `;

    const updateValues = names.map((n, i) => `${n}=COALESCE($${i + 1}, ${n})`);
    updateForTable[table] = `
      UPDATE ${String(table)}
      SET ${updateValues.join(",")}
      WHERE id = $${names.length + 1}::uuid
    `;
  }

  return {
    namesForTable,
    selectUpdateForTable,
    createForTable,
    updateForTable,
  };
}
