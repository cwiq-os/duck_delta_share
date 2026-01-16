#pragma once

#include "duckdb.hpp"
#include "delta_sharing_client.hpp"
#include <atomic>

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
    std::vector<std::string> column_names;  // Store column names for projection mapping
};

struct ReadDeltaShareLocalState : public LocalTableFunctionState {
    unique_ptr<Connection> con;              // Per-thread connection
    unique_ptr<QueryResult> current_result;  // Per-thread query result
    idx_t current_file_idx;                  // File this thread is processing

    ReadDeltaShareLocalState() : current_file_idx(DConstants::INVALID_INDEX) {}
};

struct ReadDeltaShareGlobalState : public GlobalTableFunctionState {
    atomic<idx_t> next_file_idx{0};          // Thread-safe file assignment
    std::string parquet_filters;             // Computed once, shared read-only
    std::vector<idx_t> projected_column_ids;
    std::vector<std::string> projected_columns;
    idx_t file_count{0};

    idx_t MaxThreads() const override {
        return file_count > 0 ? file_count : 1;
    }
};

struct DeltaShareListFilesBindData : public FunctionData {
    string share_name;
    string schema_name;
    string table_name;
    string predicate_hints_string;
    bool has_predicate_hints = false;

    unique_ptr<FunctionData> Copy() const override {
        auto copy = make_uniq<DeltaShareListFilesBindData>();
        copy->share_name = share_name;
        copy->schema_name = schema_name;
        copy->table_name = table_name;
        copy->predicate_hints_string = predicate_hints_string;
        copy->has_predicate_hints = has_predicate_hints;
        return std::move(copy);
    }

    bool Equals(const FunctionData &other_p) const override {
        auto &other = other_p.Cast<DeltaShareListFilesBindData>();
        return share_name == other.share_name &&
               schema_name == other.schema_name &&
               table_name == other.table_name &&
               predicate_hints_string == other.predicate_hints_string;
    }
};

class DuckDeltaShareExtension : public Extension {
public:
	void Load(ExtensionLoader &db) override;
	std::string Name() override;
	std::string Version() const override;
};

} // namespace duckdb
