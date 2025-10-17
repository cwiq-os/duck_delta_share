#define DUCKDB_EXTENSION_MAIN

#include "duck_delta_share_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include <unordered_set>

namespace duckdb {

// Section: Function data binds
// Data binds used by delta_share functions

static unique_ptr<FunctionData> ListSharesBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<string> &names) {

    auto result = make_uniq<ListSharesBindData>();

    // Load profile and query shares
    try {
        DeltaSharingProfile profile = DeltaSharingProfile::FromConfig(context);
        DeltaSharingClient client(profile);
        result->shares = client.ListShares();
    } catch (const std::exception &e) {
        throw IOException("Failed to list shares: " + std::string(e.what()));
    }

    // Define output schema
    names.push_back("name");
    names.push_back("id");

    return_types.push_back(LogicalType::VARCHAR);
    return_types.push_back(LogicalType::VARCHAR);

    return std::move(result);
}

static unique_ptr<FunctionData> ListSchemasBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<string> &names) {

    auto result = make_uniq<ListSchemasBindData>();

    if (input.inputs.size() < 1) {
        throw BinderException("delta_share_list_schemas requires share_name parameter");
    }

    result->share_name = input.inputs[0].GetValue<string>();

    try {
        DeltaSharingProfile profile = DeltaSharingProfile::FromConfig(context);
        DeltaSharingClient client(profile);
        result->schemas = client.ListSchemas(result->share_name);
    } catch (const std::exception &e) {
        throw IOException("Failed to list schemas: " + std::string(e.what()));
    }

    names.push_back("name");
    names.push_back("share");
    names.push_back("id");

    return_types.push_back(LogicalType::VARCHAR);
    return_types.push_back(LogicalType::VARCHAR);
    return_types.push_back(LogicalType::VARCHAR);

    return std::move(result);
}

static unique_ptr<FunctionData> ListTablesBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<string> &names) {

    auto result = make_uniq<ListTablesBindData>();

    if (input.inputs.size() < 2) {
        throw BinderException("delta_share_list_tables requires share_name and schema_name parameters");
    }

    result->share_name = input.inputs[0].GetValue<string>();
    result->schema_name = input.inputs[1].GetValue<string>();

    try {
        DeltaSharingProfile profile = DeltaSharingProfile::FromConfig(context);
        DeltaSharingClient client(profile);
        result->tables = client.ListTables(result->share_name, result->schema_name);
    } catch (const std::exception &e) {
        throw IOException("Failed to list tables: " + std::string(e.what()));
    }

    // Define output schema
    names.push_back("name");
    names.push_back("schema");
    names.push_back("share");
    names.push_back("id");

    return_types.push_back(LogicalType::VARCHAR);
    return_types.push_back(LogicalType::VARCHAR);
    return_types.push_back(LogicalType::VARCHAR);
    return_types.push_back(LogicalType::VARCHAR);

    return std::move(result);
}

// Section: Extension functions
// Todo: Merge these functions together. Harder to maintain.
// Current functions:
// delta_share_list_shares() -> Lists all shares on Delta Sharing server
// delta_share_list_schemas('share') -> List of schemas under specified share
// delta_share_list_tables('share','schema') -> List of tables under a schema
// delta_share_read('share','schema','table') -> Read all shares under 

static void ListSharesFunction(
    ClientContext &context,
    TableFunctionInput &data_p,
    DataChunk &output) {

    auto &bind_data = data_p.bind_data->CastNoConst<ListSharesBindData>();

    idx_t count = 0;
    while (bind_data.current_idx < bind_data.shares.size() && count < STANDARD_VECTOR_SIZE) {
        auto &share = bind_data.shares[bind_data.current_idx];

        output.SetValue(0, count, Value(share.name));
        output.SetValue(1, count, Value(share.id));

        bind_data.current_idx++;
        count++;
    }

    output.SetCardinality(count);
}

static void ListSchemasFunction(
    ClientContext &context,
    TableFunctionInput &data_p,
    DataChunk &output) {

    auto &bind_data = data_p.bind_data->CastNoConst<ListSchemasBindData>();

    idx_t count = 0;
    while (bind_data.current_idx < bind_data.schemas.size() && count < STANDARD_VECTOR_SIZE) {
        auto &schema = bind_data.schemas[bind_data.current_idx];

        output.SetValue(0, count, Value(schema.name));
        output.SetValue(1, count, Value(schema.share));
        output.SetValue(2, count, Value(schema.id));

        bind_data.current_idx++;
        count++;
    }

    output.SetCardinality(count);
}

static void ListTablesFunction(
    ClientContext &context,
    TableFunctionInput &data_p,
    DataChunk &output) {

    auto &bind_data = data_p.bind_data->CastNoConst<ListTablesBindData>();

    idx_t count = 0;
    while (bind_data.current_idx < bind_data.tables.size() && count < STANDARD_VECTOR_SIZE) {
        auto &table = bind_data.tables[bind_data.current_idx];

        output.SetValue(0, count, Value(table.name));
        output.SetValue(1, count, Value(table.schema));
        output.SetValue(2, count, Value(table.share));
        output.SetValue(3, count, Value(table.id));

        bind_data.current_idx++;
        count++;
    }

    output.SetCardinality(count);
}

static std::string ExtractColumnNameFromHint(const std::string &hint) {
    size_t pos = hint.find_first_of(" =<>!");
    if (pos != std::string::npos) {
        return hint.substr(0, pos);
    }
    return "";
}

static std::string ConvertExpressionToPredicateHint(Expression &expr) {

    switch (expr.type) {
        case ExpressionType::COMPARE_EQUAL:
        case ExpressionType::COMPARE_NOTEQUAL:
        case ExpressionType::COMPARE_LESSTHAN:
        case ExpressionType::COMPARE_GREATERTHAN:
        case ExpressionType::COMPARE_LESSTHANOREQUALTO:
        case ExpressionType::COMPARE_GREATERTHANOREQUALTO: {
            auto &comp_expr = expr.Cast<BoundComparisonExpression>();

            std::string col_name;
            std::string value_str;
            std::string op;

            switch (expr.type) {
                case ExpressionType::COMPARE_EQUAL: op = "="; break;
                case ExpressionType::COMPARE_NOTEQUAL: op = "!="; break;
                case ExpressionType::COMPARE_LESSTHAN: op = "<"; break;
                case ExpressionType::COMPARE_GREATERTHAN: op = ">"; break;
                case ExpressionType::COMPARE_LESSTHANOREQUALTO: op = "<="; break;
                case ExpressionType::COMPARE_GREATERTHANOREQUALTO: op = ">="; break;
                default: return "";
            }

            if (comp_expr.left->type == ExpressionType::BOUND_COLUMN_REF) {
                auto &col_ref = comp_expr.left->Cast<BoundColumnRefExpression>();
                col_name = col_ref.GetName();
            } else if (comp_expr.right->type == ExpressionType::BOUND_COLUMN_REF) {
                auto &col_ref = comp_expr.right->Cast<BoundColumnRefExpression>();
                col_name = col_ref.GetName();

                if (op == "<") op = ">";
                else if (op == ">") op = "<";
                else if (op == "<=") op = ">=";
                else if (op == ">=") op = "<=";
            }

            Expression *const_expr = nullptr;
            if (comp_expr.left->type == ExpressionType::VALUE_CONSTANT && !col_name.empty()) {
                const_expr = comp_expr.left.get();
            } else if (comp_expr.right->type == ExpressionType::VALUE_CONSTANT && !col_name.empty()) {
                const_expr = comp_expr.right.get();
            }

            if (const_expr && !col_name.empty()) {
                auto &const_val_expr = const_expr->Cast<BoundConstantExpression>();
                auto &value = const_val_expr.value;

                if (value.IsNull()) {
                    if (op == "=") return col_name + " IS NULL";
                    if (op == "!=") return col_name + " IS NOT NULL";
                } else {
                    switch (value.type().id()) {
                        case LogicalTypeId::VARCHAR:
                            value_str = "'" + value.ToString() + "'";
                            break;
                        case LogicalTypeId::INTEGER:
                        case LogicalTypeId::BIGINT:
                        case LogicalTypeId::DOUBLE:
                        case LogicalTypeId::FLOAT:
                            value_str = value.ToString();
                            break;
                        default:
                            value_str = "'" + value.ToString() + "'";
                            break;
                    }
                    return col_name + " " + op + " " + value_str;
                }
            }
            break;
        }
        case ExpressionType::COMPARE_IN: {
            auto &in_expr = expr.Cast<BoundOperatorExpression>();
            if (in_expr.children.size() >= 2 &&
                in_expr.children[0]->type == ExpressionType::BOUND_COLUMN_REF) {
                auto &col_ref = in_expr.children[0]->Cast<BoundColumnRefExpression>();
                std::string col_name = col_ref.GetName();

                std::vector<std::string> values;
                for (size_t i = 1; i < in_expr.children.size(); i++) {
                    if (in_expr.children[i]->type == ExpressionType::VALUE_CONSTANT) {
                        auto &const_expr = in_expr.children[i]->Cast<BoundConstantExpression>();
                        auto &value = const_expr.value;
                        if (!value.IsNull()) {
                            if (value.type().id() == LogicalTypeId::VARCHAR) {
                                values.push_back("'" + value.ToString() + "'");
                            } else {
                                values.push_back(value.ToString());
                            }
                        }
                    }
                }

                if (!values.empty()) {
                    std::string values_str = "";
                    for (size_t i = 0; i < values.size(); i++) {
                        if (i > 0) values_str += ", ";
                        values_str += values[i];
                    }
                    return col_name + " IN (" + values_str + ")";
                }
            }
            break;
        }
        case ExpressionType::CONJUNCTION_AND: {
            break;
        }
        default:
            break;
    }

    return "";
}

static LogicalType DeltaTypeToDuckDBType(const std::string &delta_type) {
    if (delta_type == "string") return LogicalType::VARCHAR;
    if (delta_type == "long" || delta_type == "bigint") return LogicalType::BIGINT;
    if (delta_type == "integer" || delta_type == "int") return LogicalType::INTEGER;
    if (delta_type == "short") return LogicalType::SMALLINT;
    if (delta_type == "byte") return LogicalType::TINYINT;
    if (delta_type == "float") return LogicalType::FLOAT;
    if (delta_type == "double") return LogicalType::DOUBLE;
    if (delta_type == "boolean") return LogicalType::BOOLEAN;
    if (delta_type == "binary") return LogicalType::BLOB;
    if (delta_type == "date") return LogicalType::DATE;
    if (delta_type == "timestamp") return LogicalType::TIMESTAMP;
    if (delta_type.find("decimal") == 0) {
        return LogicalType::DOUBLE;
    }

    return LogicalType::VARCHAR;
}

static void ParseDeltaSchema(const std::string &schema_json, vector<string> &names, vector<LogicalType> &types, const json &partition_columns_json, std::unordered_set<std::string> &partition_columns) {
    try {
        auto schema = json::parse(schema_json);

        if (!schema.contains("fields") || !schema["fields"].is_array()) {
            throw IOException("Invalid Delta schema: missing or invalid 'fields' array");
        }

        // Build set of partition column names from the partition_columns JSON array
        if (partition_columns_json.is_array()) {
            for (const auto &partition_col : partition_columns_json) {
                if (partition_col.is_string()) {
                    partition_columns.insert(partition_col.get<std::string>());
                }
            }
        }

        for (const auto &field : schema["fields"]) {
            if (!field.contains("name") || !field.contains("type")) {
                continue;
            }

            std::string col_name = field["name"].get<std::string>();
            names.push_back(col_name);

            // Type can be a string (simple type) or an object (complex type)
            if (field["type"].is_string()) {
                std::string type_str = field["type"].get<std::string>();
                types.push_back(DeltaTypeToDuckDBType(type_str));
            } else if (field["type"].is_object()) {
                // Complex type - for now just use VARCHAR
                // A full implementation would handle structs, arrays, maps
                types.push_back(LogicalType::VARCHAR);
            } else {
                types.push_back(LogicalType::VARCHAR);
            }
        }
    } catch (const std::exception &e) {
        throw IOException("Failed to parse Delta schema: " + std::string(e.what()));
    }
}

static void ReadDeltaSharePushdownComplexFilter(
    ClientContext &context,
    LogicalGet &get,
    FunctionData *bind_data_p,
    vector<unique_ptr<Expression>> &filters) {

    auto &bind_data = bind_data_p->Cast<ReadDeltaShareBindData>();

    vector<unique_ptr<Expression>> remaining_filters;

    for (auto &filter : filters) {
        std::string hint = ConvertExpressionToPredicateHint(*filter);
        if (!hint.empty()) {
            bind_data.predicate_hints.push_back(hint);
        } else {
            remaining_filters.push_back(std::move(filter));
        }
    }

    filters = std::move(remaining_filters);
}

static unique_ptr<FunctionData> ReadDeltaShareBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<string> &names) {

    auto result = make_uniq<ReadDeltaShareBindData>();

    // Get parameters
    if (input.inputs.size() < 3) {
        throw BinderException("delta_share_read requires share_name, schema_name, and table_name parameters");
    }

    result->share_name = input.inputs[0].GetValue<string>();
    result->schema_name = input.inputs[1].GetValue<string>();
    result->table_name = input.inputs[2].GetValue<string>();

    // Query table files (will be re-queried with filters during init)
    // For now, we fetch without filters during bind to establish schema
    try {
        DeltaSharingProfile profile = DeltaSharingProfile::FromConfig(context);
        DeltaSharingClient client(profile);
        auto query_result = client.QueryTable(result->share_name, result->schema_name, result->table_name);
        result->files = query_result.files;
        result->metadata = query_result.metadata;
    } catch (const std::exception &e) {
        throw IOException("Failed to read Delta Share table: " + std::string(e.what()));
    }

    // Parse Delta schema and define output schema from metadata
    ParseDeltaSchema(result->metadata.schema_string, names, return_types, result->metadata.partition_columns, result->partition_columns);

    return std::move(result);
}



// Init function - re-query with filters if any were pushed down, then prepare to read parquet files
static unique_ptr<GlobalTableFunctionState> ReadDeltaShareInit(
    ClientContext &context,
    TableFunctionInitInput &input) {

    auto &bind_data = input.bind_data->CastNoConst<ReadDeltaShareBindData>();

    // If predicate hints were pushed down during optimization, re-query with them
    if (!bind_data.predicate_hints.empty()) {
        try {
            DeltaSharingProfile profile = DeltaSharingProfile::FromConfig(context);
            DeltaSharingClient client(profile);
            auto query_result = client.QueryTable(
                bind_data.share_name,
                bind_data.schema_name,
                bind_data.table_name,
                bind_data.predicate_hints  // Pass predicate hints!
            );
            bind_data.files = query_result.files;
            bind_data.metadata = query_result.metadata;
            bind_data.current_idx = 0;
        } catch (const std::exception &e) {
            throw IOException("Failed to read Delta Share table with filters: " + std::string(e.what()));
        }
    }

    auto state = make_uniq<ReadDeltaShareGlobalState>();
    // Create a connection to execute read_parquet queries
    state->con = make_uniq<Connection>(*context.db);

    // Load httpfs extension if not already loaded
    try {
        state->con->Query("INSTALL httpfs");
        state->con->Query("LOAD httpfs");
    } catch (...) {
        // Extension might already be installed/loaded, ignore errors
    }

    return std::move(state);
}

static void ReadDeltaShareFunction(
    ClientContext &context,
    TableFunctionInput &data_p,
    DataChunk &output) {

    auto &bind_data = data_p.bind_data->CastNoConst<ReadDeltaShareBindData>();
    auto &gstate = data_p.global_state->Cast<ReadDeltaShareGlobalState>();

    // If no files, return empty
    if (bind_data.files.empty()) {
        output.SetCardinality(0);
        return;
    }

    // If we have a current result and it has data, fetch from it
    if (gstate.current_result) {
        auto chunk = gstate.current_result->Fetch();
        if (chunk) {
            output.Reference(*chunk);
            return;
        }
        // Current result is exhausted, move to next file
        gstate.current_result.reset();
    }

    // Move to next file
    if (gstate.file_idx >= bind_data.files.size()) {
        output.SetCardinality(0);
        return;
    }

    // Read next parquet file
    auto &file = bind_data.files[gstate.file_idx];
    gstate.file_idx++;

    // Build read_parquet query with optional WHERE clause for filters
    std::string query = "SELECT * FROM read_parquet('" + file.url + "')";

    // Apply predicate hints as WHERE clause, but exclude partition columns
    // that don't exist in the Parquet files
    if (!bind_data.predicate_hints.empty()) {
        std::vector<std::string> parquet_predicates;
        for (const auto &hint : bind_data.predicate_hints) {
            std::string col_name = ExtractColumnNameFromHint(hint);
            // Only include predicates for columns that exist in the Parquet file
            if (col_name.empty() || bind_data.partition_columns.find(col_name) == bind_data.partition_columns.end()) {
                parquet_predicates.push_back(hint);
            }
        }

        if (!parquet_predicates.empty()) {
            query += " WHERE ";
            for (size_t i = 0; i < parquet_predicates.size(); i++) {
                if (i > 0) {
                    query += " AND ";
                }
                query += parquet_predicates[i];
            }
        }
    }

    try {
        gstate.current_result = gstate.con->Query(query);
        if (gstate.current_result->HasError()) {
            throw IOException("Failed to read parquet file: " + gstate.current_result->GetError());
        }

        // Fetch first chunk
        auto chunk = gstate.current_result->Fetch();
        if (chunk) {
            output.Reference(*chunk);
        } else {
            output.SetCardinality(0);
        }
    } catch (const std::exception &e) {
        throw IOException("Failed to read parquet file from " + file.url + ": " + std::string(e.what()));
    }
}

static void LoadInternal(ExtensionLoader &loader) {
    auto &instance = loader.GetDatabaseInstance();
    auto &config = DBConfig::GetConfig(instance);

    config.AddExtensionOption("delta_sharing_endpoint", "URL of delta sharing server", LogicalType::VARCHAR, std::string {});
    config.AddExtensionOption("delta_sharing_bearer_token", "JWT Bearer token issued from server", LogicalType::VARCHAR, std::string {});

    TableFunction list_shares("delta_share_list_shares", {}, ListSharesFunction, ListSharesBind);

    TableFunction list_schemas("delta_share_list_schemas", {LogicalType::VARCHAR}, ListSchemasFunction, ListSchemasBind);

    TableFunction list_tables("delta_share_list_tables",
                              {LogicalType::VARCHAR, LogicalType::VARCHAR},
                              ListTablesFunction, ListTablesBind);
 
    TableFunction read_delta_share("delta_share_read",
                                   {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
                                   ReadDeltaShareFunction, ReadDeltaShareBind, ReadDeltaShareInit);
    read_delta_share.pushdown_complex_filter = ReadDeltaSharePushdownComplexFilter;

    loader.RegisterFunction(list_shares);    
    loader.RegisterFunction(list_schemas);
    loader.RegisterFunction(list_tables);
    loader.RegisterFunction(read_delta_share);

}

void DuckDeltaShareExtension::Load(ExtensionLoader &loader) {
    LoadInternal(loader);
}

std::string DuckDeltaShareExtension::Name() {
    return "duck_delta_share";
}

std::string DuckDeltaShareExtension::Version() const {
#ifdef EXT_VERSION_DUCK_DELTA_SHARE
    return EXT_VERSION_DUCK_DELTA_SHARE;
#else
    return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(duck_delta_share, loader) {
    duckdb::LoadInternal(loader);
}
}
