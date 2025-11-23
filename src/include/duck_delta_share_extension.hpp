#pragma once

#include "duckdb.hpp"
#include "delta_sharing_client.hpp"

namespace duckdb {

// Unified bind data for all list operations
struct ListBindData : public TableFunctionData {
    JsonValue items;  // Store items as JSON array
    idx_t current_idx = 0;
    int list_type; // 0=shares, 1=schemas, 2=tables
};

struct ReadDeltaShareBindData : public TableFunctionData {
    std::string share_name;
    std::string schema_name;
    std::string table_name;
    std::vector<FileAction> files;
    std::vector<std::string> filters;
    JsonValue predicate_hints;
    TableMetadata metadata;
    idx_t current_idx = 0;
    std::unordered_set<std::string> partition_columns;
};

struct ReadDeltaShareGlobalState : public GlobalTableFunctionState {
    unique_ptr<Connection> con;
    unique_ptr<QueryResult> current_result;
    std::string parquet_filters;
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
