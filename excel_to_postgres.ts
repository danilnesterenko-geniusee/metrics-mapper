import * as fs from "fs";
import * as dotenv from "dotenv";
import { v4 as uuidv4 } from "uuid";
import * as XLSX from "xlsx";
import { Pool, PoolClient } from "pg";

dotenv.config();

const dbConfig = {
  host: process.env.DB_HOST || "localhost",
  port: parseInt(process.env.DB_PORT || "5432"),
  database: process.env.DB_NAME || "your_database",
  user: process.env.DB_USER || "postgres",
  password: process.env.DB_PASSWORD || "",
};

const VALUE_TYPE_MAPPING: Record<string, string> = {
  PLAIN: "plain",
  plain: "plain",
  CURRENCY: "currency",
  PERCENTAGE: "percentage",
  DAYS: "days",
  MONTHS: "months",
};

const GROUP_NAME_MAPPING: Record<string, string> = {
  "Live Metrics": "Live Metrics",
  "Permanent Job Metrics": "Permanent Job Metrics",
  "Secondary Metrics": "Secondary Metrics",
  "Contract Jobs Metrics": "Contract Jobs Metrics",
  "Leads Metrics": "Leads Metrics",
  Ratios: "Ratios",
};

interface ReportingDashboard {
  id: string;
  created_at: Date;
  updated_at: Date;
  deleted_at: Date | null;
  name: string;
  order: number;
  is_top_dial: boolean;
  metric_value_query: string;
  details_query: string;
  leaderboard_query: string;
  metric_value_type: string;
  group_name: string;
  description: string | null;
}

async function connectToDatabase(): Promise<Pool> {
  try {
    const pool = new Pool(dbConfig);
    const client = await pool.connect();
    client.release();

    console.log(
      `Successfully connected to PostgreSQL database: ${dbConfig.database}`,
    );
    return pool;
  } catch (error) {
    console.error(`Error connecting to PostgreSQL database: ${error}`);
    process.exit(1);
  }
}

function readExcelFile(filePath: string): any[] {
  try {
    if (!fs.existsSync(filePath)) {
      console.error(`File not found: ${filePath}`);
      process.exit(1);
    }

    const workbook = XLSX.readFile(filePath);
    const firstSheetName = workbook.SheetNames[0];
    const worksheet = workbook.Sheets[firstSheetName];

    const data = XLSX.utils.sheet_to_json(worksheet);

    console.log(`Successfully read Excel file: ${filePath}`);
    return data;
  } catch (error) {
    console.error(`Error reading Excel file: ${error}`);
    process.exit(1);
  }
}

function preprocessData(data: any[]): ReportingDashboard[] {
  return data.map((row) => {
    const metricValueType = VALUE_TYPE_MAPPING[row["Value Type"]] || "plain";

    const groupName =
      GROUP_NAME_MAPPING[row["Group Name"]] || "Secondary Metrics";

    const leaderboardQuery = row["Leaderboard SQL"] || ";";

    return {
      id: uuidv4(),
      created_at: new Date(),
      updated_at: new Date(),
      deleted_at: null,
      name: row["Metric Name"],
      order: row["Order"],
      is_top_dial: false,
      metric_value_query: row["Dashboard SQL"],
      details_query: row["Detail SQL"],
      leaderboard_query: leaderboardQuery,
      metric_value_type: metricValueType,
      group_name: groupName,
      description: row["Description"] || null,
    };
  });
}

async function insertData(
  client: PoolClient,
  data: ReportingDashboard[],
): Promise<void> {
  try {
    await client.query("BEGIN");

    for (const record of data) {
      await client.query(
        `
        INSERT INTO public.reporting_dashboard (
          id, created_at, updated_at, deleted_at, name, "order", 
          is_top_dial, metric_value_query, details_query, 
          leaderboard_query, metric_value_type, group_name, description
        ) VALUES (
          $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
        )
        ON CONFLICT (id) 
        DO UPDATE SET
          updated_at = $3,
          name = $5,
          "order" = $6,
          is_top_dial = $7,
          metric_value_query = $8,
          details_query = $9,
          leaderboard_query = $10,
          metric_value_type = $11,
          group_name = $12,
          description = $13;
      `,
        [
          record.id,
          record.created_at,
          record.updated_at,
          record.deleted_at,
          record.name,
          record.order,
          record.is_top_dial,
          record.metric_value_query,
          record.details_query,
          record.leaderboard_query,
          record.metric_value_type,
          record.group_name,
          record.description,
        ],
      );
    }

    await client.query("COMMIT");

    console.log(
      `Successfully inserted/updated ${data.length} records into reporting_dashboard table`,
    );
  } catch (error) {
    await client.query("ROLLBACK");
    console.error(`Error inserting data: ${error}`);
    throw error;
  }
}

async function main(excelFilePath: string): Promise<void> {
  let pool: Pool | null = null;
  let client: PoolClient | null = null;

  try {
    pool = await connectToDatabase();
    client = await pool.connect();

    const rawData = readExcelFile(excelFilePath);
    const processedData = preprocessData(rawData);

    await insertData(client, processedData);

    console.log("Database import completed successfully!");
  } catch (error) {
    console.error(`Database import failed: ${error}`);
    process.exit(1);
  } finally {
    if (client) client.release();
    if (pool) await pool.end();
  }
}

if (require.main === module) {
  const args = process.argv.slice(2);

  if (args.length !== 1) {
    console.log("Usage: ts-node excel_to_postgres.ts <excel_file_path>");
    process.exit(1);
  }

  const excelFilePath = args[0];
  main(excelFilePath).catch((error) => {
    console.error(`Unhandled error: ${error}`);
    process.exit(1);
  });
}
