# Duck Delta Share

DuckDB extension for Delta Sharing protocol. By loading this extension, DuckDB could now act as a Delta Sharing client by using `delta_share_list` and `delta_share_read` functions.
- Disclaimer: This extension is in experimental phase. Please do note that the extension might have bugs. If you find any, please raise an issue/discussion.

## Functions

- `delta_share_list(<0-2>)` → Can list `shares`, `schemas`, and `tables` depending on provided argument/s.
  - `delta_share_list()`                            → List all shares under Delta Sharing server
  - `delta_share_list('share_name')`                → List all schemas under `share_name`
  - `delta_share_list('share_name', 'schema_name')` → List all tables under `schema_name`

- `delta_share_read('share_name', 'schema_name', 'table_name')` → Reads all files under `table_name`.
  - Supports predicate pushdown and partition column isolation (experimental).
  - Filters might not work as expected when using this function.

## Dependencies

- `httpfs`
- `read_parquet`

## Configuration

This extension uses internal config to store Delta Sharing credentials. You can set configuration by doing SET or running DuckDB with environment variables.
- `delta_share_endpoint`     → Base endpoint for Delta Sharing server (i.e. `localhost:8080/delta-sharing`).
- `delta_share_bearer_token` → Bearer token without the `Bearer ` prefix