#pragma once

#include "duckdb.hpp"
#include "delta_sharing_client.hpp"

namespace duckdb {

struct ListSharesBindData : public TableFunctionData {
    std::vector<Share> shares;
    idx_t current_idx = 0;
};

struct ListSchemasBindData : public TableFunctionData {
    std::string share_name;
    std::vector<Schema> schemas;
    idx_t current_idx = 0;
};

struct ListTablesBindData : public TableFunctionData {
    std::string share_name;
    std::string schema_name;
    std::vector<Table> tables;
    idx_t current_idx = 0;
};

struct ReadDeltaShareBindData : public TableFunctionData {
    std::string share_name;
    std::string schema_name;
    std::string table_name;
    std::vector<FileAction> files;
    std::vector<std::string> filters;
    json predicate_hints;
    TableMetadata metadata;
    idx_t current_idx = 0;
    std::unordered_set<std::string> partition_columns;
};

struct ReadDeltaShareGlobalState : public GlobalTableFunctionState {
    unique_ptr<Connection> con;
    unique_ptr<MaterializedQueryResult> current_result;
    std::string parquet_query;
    idx_t file_idx = 0;

    ReadDeltaShareGlobalState() {
    }
};

static std::unordered_map<std::string, LogicalType> DeltaLogicalMap = {
    {"string"    , LogicalType::VARCHAR},
    {"long"      , LogicalType::BIGINT},
    {"bigint"    , LogicalType::BIGINT},
    {"integer"   , LogicalType::INTEGER},
    {"int"       , LogicalType::INTEGER},
    {"short"     , LogicalType::SMALLINT},
    {"byte"      , LogicalType::TINYINT},
    {"float"     , LogicalType::FLOAT},
    {"double"    , LogicalType::DOUBLE},
    {"boolean"   , LogicalType::BOOLEAN},
    {"binary"    , LogicalType::BLOB},
    {"date"      , LogicalType::DATE},
    {"timestamp" , LogicalType::TIMESTAMP},
};

class DuckDeltaShareExtension : public Extension {
public:
	void Load(ExtensionLoader &db) override;
	std::string Name() override;
	std::string Version() const override;
};

} // namespace duckdb
