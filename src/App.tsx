import React, { useState } from 'react';
import { Database, Upload, ArrowDownUp } from 'lucide-react';
import { ClickHouseConfig, FlatFileConfig, SourceType, IngestionStatus, Table, Column } from './types';

function App() {
  const [sourceType, setSourceType] = useState<SourceType>('clickhouse');
  const [clickhouseConfig, setClickhouseConfig] = useState<ClickHouseConfig>({
    host: '',
    port: 8123,
    database: '',
    username: '',
    token: '',
  });
  const [flatFileConfig, setFlatFileConfig] = useState<FlatFileConfig>({
    delimiter: ',',
  });
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [tables, setTables] = useState<Table[]>([]);
  const [selectedTable, setSelectedTable] = useState<string>('');
  const [status, setStatus] = useState<IngestionStatus>({ status: 'idle' });

  const handleConnect = async () => {
    setStatus({ status: 'connecting' });
    try {
      const response = await fetch('http://localhost:3000/api/connect', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ config: clickhouseConfig }),
      });
      
      if (!response.ok) throw new Error('Connection failed');
      
      const data = await response.json();
      setTables(data.tables);
      setStatus({ status: 'idle' });
    } catch (error) {
      setStatus({ status: 'error', message: error instanceof Error ? error.message : 'Connection failed' });
    }
  };

  const handleColumnToggle = (tableName: string, columnName: string) => {
    setTables(tables.map(table => {
      if (table.name === tableName) {
        return {
          ...table,
          columns: table.columns.map(col => {
            if (col.name === columnName) {
              return { ...col, selected: !col.selected };
            }
            return col;
          }),
        };
      }
      return table;
    }));
  };

  const handleFileUpload = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (file) {
      setSelectedFile(file);
    }
  };

  const handleStartIngestion = async () => {
    setStatus({ status: 'ingesting' });
    const formData = new FormData();
    
    if (sourceType === 'clickhouse') {
      formData.append('config', JSON.stringify(clickhouseConfig));
      formData.append('selectedTable', selectedTable);
      formData.append('columns', JSON.stringify(
        tables.find(t => t.name === selectedTable)?.columns.filter(c => c.selected)
      ));
    } else {
      if (!selectedFile) {
        setStatus({ status: 'error', message: 'No file selected' });
        return;
      }
      formData.append('file', selectedFile);
      formData.append('config', JSON.stringify(flatFileConfig));
    }

    try {
      const response = await fetch(`http://localhost:3000/api/ingest/${sourceType}`, {
        method: 'POST',
        body: formData,
      });
      
      if (!response.ok) throw new Error('Ingestion failed');
      
      const data = await response.json();
      setStatus({ 
        status: 'completed',
        recordCount: data.recordCount,
        message: `Successfully ingested ${data.recordCount} records`
      });
    } catch (error) {
      setStatus({ 
        status: 'error',
        message: error instanceof Error ? error.message : 'Ingestion failed'
      });
    }
  };

  return (
    <div className="min-h-screen bg-gray-100 p-8">
      <div className="max-w-4xl mx-auto bg-white rounded-lg shadow-md p-6">
        <h1 className="text-2xl font-bold mb-6 flex items-center gap-2">
          <ArrowDownUp className="h-6 w-6" />
          Data Ingestion Tool
        </h1>

        {/* Source Selection */}
        <div className="mb-6">
          <h2 className="text-lg font-semibold mb-3">Select Source</h2>
          <div className="flex gap-4">
            <button
              className={`flex items-center gap-2 px-4 py-2 rounded ${
                sourceType === 'clickhouse' ? 'bg-blue-500 text-white' : 'bg-gray-200'
              }`}
              onClick={() => setSourceType('clickhouse')}
            >
              <Database className="h-5 w-5" />
              ClickHouse
            </button>
            <button
              className={`flex items-center gap-2 px-4 py-2 rounded ${
                sourceType === 'flatfile' ? 'bg-blue-500 text-white' : 'bg-gray-200'
              }`}
              onClick={() => setSourceType('flatfile')}
            >
              <Upload className="h-5 w-5" />
              Flat File
            </button>
          </div>
        </div>

        {/* Configuration Forms */}
        {sourceType === 'clickhouse' ? (
          <div className="mb-6">
            <h2 className="text-lg font-semibold mb-3">ClickHouse Configuration</h2>
            <div className="grid grid-cols-2 gap-4">
              <input
                type="text"
                placeholder="Host"
                className="p-2 border rounded"
                value={clickhouseConfig.host}
                onChange={(e) => setClickhouseConfig({ ...clickhouseConfig, host: e.target.value })}
              />
              <input
                type="number"
                placeholder="Port"
                className="p-2 border rounded"
                value={clickhouseConfig.port}
                onChange={(e) => setClickhouseConfig({ ...clickhouseConfig, port: parseInt(e.target.value) })}
              />
              <input
                type="text"
                placeholder="Database"
                className="p-2 border rounded"
                value={clickhouseConfig.database}
                onChange={(e) => setClickhouseConfig({ ...clickhouseConfig, database: e.target.value })}
              />
              <input
                type="text"
                placeholder="Username"
                className="p-2 border rounded"
                value={clickhouseConfig.username}
                onChange={(e) => setClickhouseConfig({ ...clickhouseConfig, username: e.target.value })}
              />
              <input
                type="password"
                placeholder="JWT Token"
                className="p-2 border rounded col-span-2"
                value={clickhouseConfig.token}
                onChange={(e) => setClickhouseConfig({ ...clickhouseConfig, token: e.target.value })}
              />
            </div>
            <button
              className="mt-4 px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600"
              onClick={handleConnect}
            >
              Connect
            </button>
          </div>
        ) : (
          <div className="mb-6">
            <h2 className="text-lg font-semibold mb-3">Flat File Configuration</h2>
            <div className="grid grid-cols-2 gap-4">
              <input
                type="file"
                className="p-2 border rounded"
                onChange={handleFileUpload}
              />
              <input
                type="text"
                placeholder="Delimiter"
                className="p-2 border rounded"
                value={flatFileConfig.delimiter}
                onChange={(e) => setFlatFileConfig({ ...flatFileConfig, delimiter: e.target.value })}
              />
            </div>
          </div>
        )}

        {/* Table and Column Selection */}
        {sourceType === 'clickhouse' && tables.length > 0 && (
          <div className="mb-6">
            <h2 className="text-lg font-semibold mb-3">Select Table and Columns</h2>
            <select
              className="w-full p-2 border rounded mb-4"
              value={selectedTable}
              onChange={(e) => setSelectedTable(e.target.value)}
            >
              <option value="">Select a table</option>
              {tables.map(table => (
                <option key={table.name} value={table.name}>{table.name}</option>
              ))}
            </select>

            {selectedTable && (
              <div className="border rounded p-4">
                <h3 className="font-semibold mb-2">Columns</h3>
                <div className="grid grid-cols-2 gap-2">
                  {tables
                    .find(t => t.name === selectedTable)
                    ?.columns.map(column => (
                      <label key={column.name} className="flex items-center gap-2">
                        <input
                          type="checkbox"
                          checked={column.selected}
                          onChange={() => handleColumnToggle(selectedTable, column.name)}
                        />
                        {column.name} ({column.type})
                      </label>
                    ))}
                </div>
              </div>
            )}
          </div>
        )}

        {/* Action Buttons */}
        <div className="flex justify-end gap-4">
          <button
            className="px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600"
            onClick={handleStartIngestion}
            disabled={status.status === 'ingesting'}
          >
            {status.status === 'ingesting' ? 'Ingesting...' : 'Start Ingestion'}
          </button>
        </div>

        {/* Status Display */}
        {status.status !== 'idle' && (
          <div className={`mt-6 p-4 rounded ${
            status.status === 'error' ? 'bg-red-100 text-red-700' :
            status.status === 'completed' ? 'bg-green-100 text-green-700' :
            'bg-blue-100 text-blue-700'
          }`}>
            <p className="font-semibold">{status.status.charAt(0).toUpperCase() + status.status.slice(1)}</p>
            {status.message && <p>{status.message}</p>}
            {status.recordCount !== undefined && (
              <p>Total records processed: {status.recordCount}</p>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

export default App;