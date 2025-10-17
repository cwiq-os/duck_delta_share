#pragma once

#include "duckdb.hpp"

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
    std::vector<std::string> predicate_hints;
    TableMetadata metadata;
    idx_t current_idx = 0;
    std::unordered_set<std::string> partition_columns;
};

struct ReadDeltaShareGlobalState : public GlobalTableFunctionState {
    unique_ptr<Connection> con;
    unique_ptr<MaterializedQueryResult> current_result;
    idx_t file_idx = 0;

    ReadDeltaShareGlobalState() {
    }
};

class DuckDeltaShareExtension : public Extension {
public:
	void Load(ExtensionLoader &db) override;
	std::string Name() override;
	std::string Version() const override;
};

} // namespace duckdb
