export interface ClickHouseConfig {
  host: string;
  port: number;
  database: string;
  username: string;
  token: string;
}

export interface FlatFileConfig {
  delimiter: string;
}

export interface Column {
  name: string;
  type: string;
  selected: boolean;
}

export interface Table {
  name: string;
  columns: Column[];
}

export type SourceType = 'clickhouse' | 'flatfile';

export interface IngestionStatus {
  status: 'idle' | 'connecting' | 'fetching' | 'ingesting' | 'completed' | 'error';
  message?: string;
  recordCount?: number;
}