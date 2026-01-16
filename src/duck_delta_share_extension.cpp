#define DUCKDB_EXTENSION_MAIN

#include "duck_delta_share_extension.hpp"
#include "duck_delta_share_functions.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include <nlohmann/json.hpp>
#include <unordered_set>

namespace duckdb {

using json = nlohmann::json;

// Section: Function data binds
// Data binds used by delta_share functions

static unique_ptr<FunctionData> ListBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<string> &names) {

    auto result = make_uniq<ListBindData>();

    try {
        DeltaSharingProfile profile = DeltaSharingProfile::FromConfig(context);
        DeltaSharingClient client(profile);

        // Argument arity determines which function is called
        if (input.inputs.size() == 0) { // Shares
            result->list_type = 0;
            result->items = client.ListShares();
            names.push_back("name");
            names.push_back("id");
            return_types.push_back(LogicalType::VARCHAR);
            return_types.push_back(LogicalType::VARCHAR);

        } else if (input.inputs.size() == 1) { // Schemas
            result->list_type = 1;
            string share_name = input.inputs[0].GetValue<string>();
            result->items = client.ListSchemas(share_name);
            names.push_back("name");
            names.push_back("share");
            names.push_back("id");
            return_types.push_back(LogicalType::VARCHAR);
            return_types.push_back(LogicalType::VARCHAR);
            return_types.push_back(LogicalType::VARCHAR);

        } else if (input.inputs.size() == 2) { // Tables
            result->list_type = 2;
            string share_name = input.inputs[0].GetValue<string>();
            string schema_name = input.inputs[1].GetValue<string>();
            result->items = client.ListTables(share_name, schema_name);
            names.push_back("name");
            names.push_back("schema");
            names.push_back("share");
            names.push_back("id");
            return_types.push_back(LogicalType::VARCHAR);
            return_types.push_back(LogicalType::VARCHAR);
            return_types.push_back(LogicalType::VARCHAR);
            return_types.push_back(LogicalType::VARCHAR);

        } else {
            throw BinderException("ListBind error: function accepts 0, 1, 2 arguments");
        }
    } catch (const std::exception &e) {
        throw IOException("ListBind error: " + std::string(e.what()));
    }

    return std::move(result);
}

static void ListFunction(
    ClientContext &context,
    TableFunctionInput &data_p,
    DataChunk &output) {

    auto &bind_data = data_p.bind_data->CastNoConst<ListBindData>();

    idx_t count = 0;
    // Consider: architectural tradeoff to accept NULL json fields?
    // Convert JsonValue to json for internal processing
    auto* items_json = static_cast<json*>(bind_data.items.GetInternalPtr());
    while (bind_data.current_idx < items_json->size() && count < STANDARD_VECTOR_SIZE) {
        auto &item = (*items_json)[bind_data.current_idx];
        idx_t col = 0;

        output.SetValue(col++, count, Value(item["name"].get<string>()));

        if (bind_data.list_type == 1) {
            output.SetValue(col++, count, Value(item["share"].get<string>()));
        } else if (bind_data.list_type == 2) {
            output.SetValue(col++, count, Value(item["schema"].get<string>()));
            output.SetValue(col++, count, Value(item["share"].get<string>()));
        }

        // This should not be empty but our Delta Sharing server hides this from the response ._.
        output.SetValue(col++, count, Value(item["id"].is_null() ? "" : item["id"].get<string>()));
        bind_data.current_idx++;
        count++;
    }

    output.SetCardinality(count);
}

static void ReadDeltaSharePushdownComplexFilter(
    ClientContext &context,
    LogicalGet &get,
    FunctionData *bind_data_p,
    vector<unique_ptr<Expression>> &filters) {

    auto &bind_data = bind_data_p->Cast<ReadDeltaShareBindData>();
    if (!filters.empty()) {
        auto predicate_json = GetPredicateHints(filters);
        bind_data.predicate_hints = JsonValue::FromInternal(&predicate_json);
        for (auto &filter : filters) {
            ParseExpression(*filter, bind_data.filters);
            bind_data.filters.push_back("AND");
        }
        if (!bind_data.filters.empty()) bind_data.filters.pop_back();
    }
    filters = std::move(vector<unique_ptr<Expression>>{});
}

static unique_ptr<FunctionData> ReadDeltaShareBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<string> &names) {

    auto result = make_uniq<ReadDeltaShareBindData>();

    if (input.inputs.size() < 3) {
        throw BinderException("ReadDeltaShareBind usage: delta_share_read('share_name', 'schema_name', 'table_name')");
    }

    result->share_name = input.inputs[0].GetValue<string>();
    result->schema_name = input.inputs[1].GetValue<string>();
    result->table_name = input.inputs[2].GetValue<string>();

    DeltaSharingProfile profile = DeltaSharingProfile::FromConfig(context);
    DeltaSharingClient client(profile);
    auto query_result = client.QueryTableMetadata(result->share_name, result->schema_name, result->table_name);
    result->metadata = query_result.metadata;

    // Convert JsonValue to json for ParseDeltaSchema
    auto* partition_cols_json = static_cast<const json*>(result->metadata.partition_columns.GetInternalPtr());
    ParseDeltaSchema(result->metadata.schema_string, names, return_types, *partition_cols_json, result->partition_columns);
    result->column_names = names;  // Store column names for projection mapping
    return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> ReadDeltaShareInit(
    ClientContext &context,
    TableFunctionInitInput &input) {

    auto &bind_data = input.bind_data->CastNoConst<ReadDeltaShareBindData>();
    // If predicate hints were pushed down during optimization, re-query with them

    DeltaSharingProfile profile = DeltaSharingProfile::FromConfig(context);
    DeltaSharingClient client(profile);
    auto query_result = client.QueryTable(
        bind_data.share_name,
        bind_data.schema_name,
        bind_data.table_name,
        bind_data.predicate_hints
    );
    bind_data.files = query_result.files;
    bind_data.metadata = query_result.metadata;
    bind_data.current_idx = 0;

    auto state = make_uniq<ReadDeltaShareGlobalState>();

    // Capture projection info - map column IDs to column names
    for (auto col_id : input.column_ids) {
        if (col_id != COLUMN_IDENTIFIER_ROW_ID && col_id < bind_data.column_names.size()) {
            state->projected_column_ids.push_back(col_id);
            state->projected_columns.push_back(bind_data.column_names[col_id]);
        }
    }

    // Store file count for MaxThreads
    state->file_count = bind_data.files.size();

    // Pre-compute parquet filters once for all threads
    if (!bind_data.filters.empty()) {
        std::string parquet_filters = " WHERE ";
        std::vector<std::string> parquet_predicates;
        // Filter out partition columns
        for (const auto &hint : bind_data.filters) {
            if (parquet_predicates.empty() && (hint == "AND" || hint == "OR"))
                continue;
            std::string col_name = ExtractColumnNameFromHint(hint);
            if (col_name.empty() || bind_data.partition_columns.find(col_name) == bind_data.partition_columns.end()) {
                parquet_predicates.push_back(hint);
            }
        }

        // Build WHERE clause
        size_t filter_count{0};
        for (size_t i = 0; i < parquet_predicates.size(); i++) {
            if (parquet_predicates[i] == "OR" || parquet_predicates[i] == "AND") continue;
            if (i > 0) {
                parquet_filters += ' ' + parquet_predicates[i - 1] + ' ';
            }
            parquet_filters += parquet_predicates[i];
            ++filter_count;
        }
        if (filter_count) {
            state->parquet_filters = parquet_filters;
        }
    }

    return std::move(state);
}

static unique_ptr<LocalTableFunctionState> ReadDeltaShareInitLocal(
    ExecutionContext &context,
    TableFunctionInitInput &input,
    GlobalTableFunctionState *global_state) {

    auto local_state = make_uniq<ReadDeltaShareLocalState>();
    local_state->con = make_uniq<Connection>(*context.client.db);
    return std::move(local_state);
}

static void ReadDeltaShareFunction(
    ClientContext &context,
    TableFunctionInput &data_p,
    DataChunk &output) {

    auto &bind_data = data_p.bind_data->CastNoConst<ReadDeltaShareBindData>();
    auto &gstate = data_p.global_state->Cast<ReadDeltaShareGlobalState>();
    auto &lstate = data_p.local_state->Cast<ReadDeltaShareLocalState>();

    if (bind_data.files.empty()) {
        output.SetCardinality(0);
        return;
    }

    // Try to fetch from current result first
    while (true) {
        if (lstate.current_result) {
            auto chunk = lstate.current_result->Fetch();
            if (chunk) {
                output.Reference(*chunk);
                return;
            }
            lstate.current_result.reset();
        }

        // Get next file using atomic increment
        idx_t file_idx = gstate.next_file_idx.fetch_add(1);
        if (file_idx >= bind_data.files.size()) {
            output.SetCardinality(0);
            return;
        }
        lstate.current_file_idx = file_idx;

        auto &file = bind_data.files[file_idx];

        // Build column list for projection pushdown, excluding partition columns
        std::string select_columns;
        if (!gstate.projected_columns.empty()) {
            std::vector<std::string> parquet_columns;
            for (const auto &col : gstate.projected_columns) {
                // Only include non-partition columns (partition values aren't in parquet files)
                if (bind_data.partition_columns.find(col) == bind_data.partition_columns.end()) {
                    parquet_columns.push_back("\"" + col + "\"");
                }
            }
            if (!parquet_columns.empty()) {
                for (size_t i = 0; i < parquet_columns.size(); i++) {
                    if (i > 0) select_columns += ", ";
                    select_columns += parquet_columns[i];
                }
            } else {
                // All projected columns are partition columns - select minimal data
                select_columns = "*";
            }
        } else {
            select_columns = "*";
        }

        std::string query = "SELECT " + select_columns + " FROM read_parquet('" + file.url + "')";
        if (!gstate.parquet_filters.empty()) {
            query += gstate.parquet_filters;
        }

        try {
            // Use per-thread connection for read_parquet
            lstate.current_result = lstate.con->SendQuery(query);
            if (lstate.current_result->HasError()) {
                throw IOException("ReadDeltaShare error: read_parquet query failed. Reason: " + lstate.current_result->GetError());
            }

            // Return output from query
            auto chunk = lstate.current_result->Fetch();
            if (chunk) {
                output.Reference(*chunk);
                return;
            }
            lstate.current_result.reset();
            // Continue loop to get next file
        } catch (const std::exception &e) {
            throw IOException("ReadDeltaShare error: failed to read parquet file from " + file.url + ": " + std::string(e.what()));
        }
    }
}

// Section: delta_share_list_files scalar function

static unique_ptr<FunctionData> DeltaShareListFilesBind(
    ClientContext &context,
    ScalarFunction &bound_function,
    vector<unique_ptr<Expression>> &arguments) {

    auto result = make_uniq<DeltaShareListFilesBindData>();
    result->has_predicate_hints = (arguments.size() == 4);
    return std::move(result);
}

static void DeltaShareListFilesFunction(
    DataChunk &args,
    ExpressionState &state,
    Vector &result) {

    auto &bind_data = state.expr.Cast<BoundFunctionExpression>().bind_info->Cast<DeltaShareListFilesBindData>();

    idx_t count = args.size();

    // Setup result as LIST(VARCHAR)
    result.SetVectorType(VectorType::FLAT_VECTOR);
    auto list_data = FlatVector::GetData<list_entry_t>(result);
    auto &child_vector = ListVector::GetEntry(result);
    auto &result_validity = FlatVector::Validity(result);

    idx_t total_size = 0;

    // Process each row in the input
    for (idx_t row_idx = 0; row_idx < count; row_idx++) {
        // Get argument values
        auto share_name = args.data[0].GetValue(row_idx).ToString();
        auto schema_name = args.data[1].GetValue(row_idx).ToString();
        auto table_name = args.data[2].GetValue(row_idx).ToString();

        // Handle optional predicate hints
        JsonValue predicate_hints = JsonValue::Object();
        if (args.ColumnCount() >= 4) {
            auto pred_value = args.data[3].GetValue(row_idx);
            if (!pred_value.IsNull()) {
                string predicate_string = pred_value.ToString();
                auto predicate_json = ParsePredicateStringToJson(predicate_string);
                predicate_hints = JsonValue::FromInternal(&predicate_json);
            }
        }

        try {
            // Get client from context
            auto &context = state.GetContext();
            DeltaSharingProfile profile = DeltaSharingProfile::FromConfig(context);
            DeltaSharingClient client(profile);

            // Query table to get files
            auto query_result = client.QueryTable(
                share_name, schema_name, table_name, predicate_hints);

            // Set list entry metadata
            list_data[row_idx].offset = total_size;
            list_data[row_idx].length = query_result.files.size();

            // Reserve space in child vector
            ListVector::Reserve(result, total_size + query_result.files.size());
            auto child_data = FlatVector::GetData<string_t>(child_vector);

            // Add file URLs to child vector
            for (size_t i = 0; i < query_result.files.size(); i++) {
                child_data[total_size + i] = StringVector::AddString(child_vector, query_result.files[i].url);
            }

            total_size += query_result.files.size();

        } catch (const std::exception &e) {
            throw IOException("delta_share_list_files error: " + std::string(e.what()));
        }
    }

    ListVector::SetListSize(result, total_size);
}

static void LoadInternal(ExtensionLoader &loader) {
    auto &instance = loader.GetDatabaseInstance();
    auto &config = DBConfig::GetConfig(instance);

	// Delta Sharing required extensions
    Connection con(loader.GetDatabaseInstance());
	auto result = con.Query("LOAD httpfs");
	if (result->HasError()) {
		con.Query("INSTALL httpfs");
		con.Query("LOAD httpfs");
	}

    // Delta Sharing config
    const char* env_ep = std::getenv("DELTA_SHARING_ENDPOINT");
    const char* env_token = std::getenv("DELTA_SHARING_BEARER_TOKEN");
    config.AddExtensionOption("delta_sharing_endpoint", "URL of delta sharing server", 
        LogicalType::VARCHAR, 
        env_ep? std::string(env_ep) : "");
    config.AddExtensionOption("delta_sharing_bearer_token", "JWT Bearer token issued from server", 
        LogicalType::VARCHAR, 
        env_token? std::string(env_token) : "");

    // Delta Sharing Functions
    TableFunction list("delta_share_list", {}, ListFunction, ListBind);
    list.varargs = LogicalType::VARCHAR;

    TableFunction read_delta_share("delta_share_read",
                                   {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
                                   ReadDeltaShareFunction, ReadDeltaShareBind, ReadDeltaShareInit);
    read_delta_share.init_local = ReadDeltaShareInitLocal;
    read_delta_share.projection_pushdown = true;
    read_delta_share.pushdown_complex_filter = ReadDeltaSharePushdownComplexFilter;
    loader.RegisterFunction(list);
    loader.RegisterFunction(read_delta_share);

    // Scalar function: delta_share_list_files
    ScalarFunctionSet list_files_set("delta_share_list_files");

    // 3-argument version (no predicate hints)
    ScalarFunction list_files_3arg(
        {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
        LogicalType::LIST(LogicalType::VARCHAR),
        DeltaShareListFilesFunction,
        DeltaShareListFilesBind);

    // 4-argument version (with predicate hints)
    ScalarFunction list_files_4arg(
        {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
        LogicalType::LIST(LogicalType::VARCHAR),
        DeltaShareListFilesFunction,
        DeltaShareListFilesBind);

    list_files_set.AddFunction(list_files_3arg);
    list_files_set.AddFunction(list_files_4arg);
    loader.RegisterFunction(list_files_set);

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