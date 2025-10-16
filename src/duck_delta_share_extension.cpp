#define DUCKDB_EXTENSION_MAIN

#include "duck_delta_share_extension.hpp"
#include "delta_sharing_client.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

// =============================================================================
// DELTA_SHARE_LIST_SHARES - List all available shares
// =============================================================================

struct ListSharesBindData : public TableFunctionData {
    std::vector<Share> shares;
    idx_t current_idx = 0;
};

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

// =============================================================================
// DELTA_SHARE_LIST_SCHEMAS - List schemas in a share
// =============================================================================

struct ListSchemasBindData : public TableFunctionData {
    std::string share_name;
    std::vector<Schema> schemas;
    idx_t current_idx = 0;
};

static unique_ptr<FunctionData> ListSchemasBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<string> &names) {

    auto result = make_uniq<ListSchemasBindData>();

    // Get parameters
    if (input.inputs.size() < 1) {
        throw BinderException("delta_share_list_schemas requires share_name parameter");
    }

    result->share_name = input.inputs[0].GetValue<string>();

    // Load profile and query schemas
    try {
        DeltaSharingProfile profile = DeltaSharingProfile::FromConfig(context);
        DeltaSharingClient client(profile);
        result->schemas = client.ListSchemas(result->share_name);
    } catch (const std::exception &e) {
        throw IOException("Failed to list schemas: " + std::string(e.what()));
    }

    // Define output schema
    names.push_back("name");
    names.push_back("share");
    names.push_back("id");

    return_types.push_back(LogicalType::VARCHAR);
    return_types.push_back(LogicalType::VARCHAR);
    return_types.push_back(LogicalType::VARCHAR);

    return std::move(result);
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

// =============================================================================
// DELTA_SHARE_LIST_TABLES - List tables in a schema
// =============================================================================

struct ListTablesBindData : public TableFunctionData {
    std::string share_name;
    std::string schema_name;
    std::vector<Table> tables;
    idx_t current_idx = 0;
};

static unique_ptr<FunctionData> ListTablesBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<string> &names) {

    auto result = make_uniq<ListTablesBindData>();

    // Get parameters
    if (input.inputs.size() < 2) {
        throw BinderException("delta_share_list_tables requires share_name and schema_name parameters");
    }

    result->share_name = input.inputs[0].GetValue<string>();
    result->schema_name = input.inputs[1].GetValue<string>();

    // Load profile and query tables
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

// =============================================================================
// DELTA_SHARE_READ - Get list of URLs from a Delta Share table
// =============================================================================

static void ReadDeltaShareScalarFunction(
    DataChunk &args,
    ExpressionState &state,
    Vector &result) {

    auto &context = state.GetContext();

    // Process each row in the chunk
    auto count = args.size();

    UnifiedVectorFormat share_name_data;
    UnifiedVectorFormat schema_name_data;
    UnifiedVectorFormat table_name_data;

    args.data[0].ToUnifiedFormat(count, share_name_data);
    args.data[1].ToUnifiedFormat(count, schema_name_data);
    args.data[2].ToUnifiedFormat(count, table_name_data);

    auto share_name_ptr = UnifiedVectorFormat::GetData<string_t>(share_name_data);
    auto schema_name_ptr = UnifiedVectorFormat::GetData<string_t>(schema_name_data);
    auto table_name_ptr = UnifiedVectorFormat::GetData<string_t>(table_name_data);

    for (idx_t i = 0; i < count; i++) {
        auto share_idx = share_name_data.sel->get_index(i);
        auto schema_idx = schema_name_data.sel->get_index(i);
        auto table_idx = table_name_data.sel->get_index(i);

        if (!share_name_data.validity.RowIsValid(share_idx) ||
            !schema_name_data.validity.RowIsValid(schema_idx) ||
            !table_name_data.validity.RowIsValid(table_idx)) {
            FlatVector::SetNull(result, i, true);
            continue;
        }

        auto share_name = share_name_ptr[share_idx].GetString();
        auto schema_name = schema_name_ptr[schema_idx].GetString();
        auto table_name = table_name_ptr[table_idx].GetString();

        // Query table files
        std::vector<FileAction> files;
        try {
            DeltaSharingProfile profile = DeltaSharingProfile::FromConfig(context);
            DeltaSharingClient client(profile);
            auto query_result = client.QueryTable(share_name, schema_name, table_name);
            files = query_result.files;
        } catch (const std::exception &e) {
            throw IOException("Failed to read Delta Share table: " + std::string(e.what()));
        }

        // Build list of URLs
        vector<Value> url_values;
        url_values.reserve(files.size());
        for (const auto &file : files) {
            url_values.push_back(Value(file.url));
        }

        // Create list value and set in result
        auto list_value = Value::LIST(LogicalType::VARCHAR, url_values);
        result.SetValue(i, list_value);
    }
}

// =============================================================================
// Extension Load
// =============================================================================

static void LoadInternal(ExtensionLoader &loader) {

    auto &instance = loader.GetDatabaseInstance();
    auto &config = DBConfig::GetConfig(instance);

    config.AddExtensionOption("delta_sharing_endpoint", "URL of delta sharing server", LogicalType::VARCHAR, std::string {});
    config.AddExtensionOption("delta_sharing_bearer_token", "JWT Bearer token issued from server", LogicalType::VARCHAR, std::string {});
    // List shares table function - no parameters
    TableFunction list_shares("delta_share_list_shares", {}, ListSharesFunction, ListSharesBind);
    loader.RegisterFunction(list_shares);

    // List schemas table function - share_name
    TableFunction list_schemas("delta_share_list_schemas", {LogicalType::VARCHAR}, ListSchemasFunction, ListSchemasBind);
    loader.RegisterFunction(list_schemas);

    // List tables table function - share_name, schema_name
    TableFunction list_tables("delta_share_list_tables",
                              {LogicalType::VARCHAR, LogicalType::VARCHAR},
                              ListTablesFunction, ListTablesBind);
    loader.RegisterFunction(list_tables);

    // Read Delta Share scalar function - returns list of URLs
    ScalarFunction read_delta_share("delta_share_read",
                                   {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
                                   LogicalType::LIST(LogicalType::VARCHAR),
                                   ReadDeltaShareScalarFunction);
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
