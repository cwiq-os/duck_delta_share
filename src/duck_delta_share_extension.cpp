#define DUCKDB_EXTENSION_MAIN

#include "duck_delta_share_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
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

// Unified bind function for all list operations
static unique_ptr<FunctionData> ListBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<string> &names) {

    auto result = make_uniq<ListBindData>();

    try {
        DeltaSharingProfile profile = DeltaSharingProfile::FromConfig(context);
        DeltaSharingClient client(profile);

        // Determine what to list based on argument count
        if (input.inputs.size() == 0) {
            // List shares
            result->list_type = 0;
            result->items = client.ListShares();
            names.push_back("name");
            names.push_back("id");
            return_types.push_back(LogicalType::VARCHAR);
            return_types.push_back(LogicalType::VARCHAR);

        } else if (input.inputs.size() == 1) {
            // List schemas
            result->list_type = 1;
            string share_name = input.inputs[0].GetValue<string>();
            result->items = client.ListSchemas(share_name);
            names.push_back("name");
            names.push_back("share");
            names.push_back("id");
            return_types.push_back(LogicalType::VARCHAR);
            return_types.push_back(LogicalType::VARCHAR);
            return_types.push_back(LogicalType::VARCHAR);

        } else if (input.inputs.size() == 2) {
            // List tables
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
            throw BinderException("delta_share_list accepts 0, 1, or 2 arguments");
        }
    } catch (const std::exception &e) {
        throw IOException("Failed to list: " + std::string(e.what()));
    }

    return std::move(result);
}

// Unified execution function for all list operations
static void ListFunction(
    ClientContext &context,
    TableFunctionInput &data_p,
    DataChunk &output) {

    auto &bind_data = data_p.bind_data->CastNoConst<ListBindData>();

    idx_t count = 0;
    while (bind_data.current_idx < bind_data.items.size() && count < STANDARD_VECTOR_SIZE) {
        auto &item = bind_data.items[bind_data.current_idx];

        // Set values based on list type
        idx_t col = 0;

        // All types have 'name' first
        output.SetValue(col++, count, Value(item["name"].get<string>()));

        // Schemas have 'share' second, Tables have 'schema' second
        if (bind_data.list_type == 1) {
            // Schema: name, share, id
            output.SetValue(col++, count, Value(item["share"].get<string>()));
        } else if (bind_data.list_type == 2) {
            // Table: name, schema, share, id
            output.SetValue(col++, count, Value(item["schema"].get<string>()));
            output.SetValue(col++, count, Value(item["share"].get<string>()));
        }

        // All types have 'id' last
        output.SetValue(col++, count, Value(item["id"].get<string>()));

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

static void ParseExpression(Expression& expr, std::vector<std::string>& res) {
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
                default: return;
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
                    if (op == "=") res.push_back(col_name + " IS NULL");
                    if (op == "!=") res.push_back(col_name + " IS NOT NULL");
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
                    res.push_back(col_name + " " + op + " " + value_str);
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
                    res.push_back(col_name + " IN (" + values_str + ")");
                }
            }
            break;
        }
        case ExpressionType::COMPARE_BETWEEN: {
            auto& between = expr.Cast<BoundBetweenExpression>();
            auto lower_comp = between.LowerComparisonType();
            auto upper_comp = between.UpperComparisonType();
            auto left_expr = make_uniq<BoundComparisonExpression>(
                lower_comp, between.input->Copy(), between.lower->Copy());
            auto right_expr = make_uniq<BoundComparisonExpression>(
                upper_comp, between.input->Copy(), between.upper->Copy());

            ParseExpression(*left_expr, res);
            res.push_back("AND");
            ParseExpression(*right_expr, res);
            break;
        }
        case ExpressionType::CONJUNCTION_AND: {
            auto& expr_node = expr.Cast<BoundConjunctionExpression>();

            if (expr_node.children.size()) ParseExpression(*expr_node.children[0], res);
            res.push_back("AND");
            if (expr_node.children.size() > 1) ParseExpression(*expr_node.children[1], res);
            break;
        }
        case ExpressionType::CONJUNCTION_OR: {
            auto& expr_node = expr.Cast<BoundConjunctionExpression>();
            if (expr_node.children.size()) ParseExpression(*expr_node.children[0], res);
            res.push_back("OR");
            if (expr_node.children.size() > 1) ParseExpression(*expr_node.children[1], res);
            break;
        }
        default:
            break;
    }
}

static json OperandJSON(Expression& expr) {
    json res{};
    
    if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
        auto& column = expr.Cast<BoundColumnRefExpression>();
        res["op"] = "column";
        res["name"] = column.GetName();
        switch (column.return_type.id()) {
            case LogicalTypeId::BOOLEAN:
            case LogicalTypeId::TINYINT:
            case LogicalTypeId::INTEGER:
            case LogicalTypeId::BIGINT:
            case LogicalTypeId::DOUBLE:
                res["valueType"] = "int";
                break;
            case LogicalTypeId::VARCHAR:
            default:
                res["valueType"] = "string";
                break;
        }
        return res;
    } else if (expr.type == ExpressionType::VALUE_CONSTANT) {
        auto& literal = expr.Cast<BoundConstantExpression>();
        res["op"]        = "literal";
        res["value"]     = literal.value.ToString();
        switch (literal.return_type.id()) {
            case LogicalTypeId::BOOLEAN:
            case LogicalTypeId::TINYINT:
            case LogicalTypeId::INTEGER:
            case LogicalTypeId::BIGINT:
            case LogicalTypeId::DOUBLE:
                res["valueType"] = "int";
                break;
            case LogicalTypeId::VARCHAR:
            default:
                res["valueType"] = "string";
                break;
        }

        return res;
    }
    return res;
}

static json BinaryOpJSON(Expression& expr, std::string op) {
    json res{};
    res["op"] = op;
    res["children"] = json::array();
    auto &comp_expr = expr.Cast<BoundComparisonExpression>();
    if (comp_expr.left) res["children"].push_back(OperandJSON(*comp_expr.left));
    if (comp_expr.right) res["children"].push_back(OperandJSON(*comp_expr.right));
    return res;
}

static json ParseExpressionHint(Expression& expr) {
    json res{};
    std::string op;
    std::string left_name;
    std::string left_type;
    std::string right_name;
    std::string right_type;
    switch (expr.type) {
        case ExpressionType::COMPARE_EQUAL: {
            op = "equal";
            res = BinaryOpJSON(expr, op);
        } break;
        case ExpressionType::COMPARE_LESSTHAN: {
            op = "lessThan";
            res = BinaryOpJSON(expr, op);
        } break;
        case ExpressionType::COMPARE_GREATERTHAN: {
            op = "greaterThan";
            res = BinaryOpJSON(expr, op);
        } break;
        case ExpressionType::COMPARE_LESSTHANOREQUALTO: {
            op = "lessThanOrEqual";
            res = BinaryOpJSON(expr, op);
        } break;
        case ExpressionType::COMPARE_GREATERTHANOREQUALTO: {
            op = "greaterThanOrEqual";
            res = BinaryOpJSON(expr, op);
        } break;
        case ExpressionType::COMPARE_NOTEQUAL: {
            op = "equal";
            res["op"] = "not";
            res["children"] = json::array();
            json equal_predicate = BinaryOpJSON(expr, op); 
            res["children"].push_back(equal_predicate);
            break;
        }
        case ExpressionType::OPERATOR_IS_NULL: {
            res["op"] = "isNull";
            auto &op_expr = expr.Cast<BoundOperatorExpression>();
            res["children"] = json::array();
            res["children"].push_back(OperandJSON(*op_expr.children[0]));
            break;
        }
        case ExpressionType::OPERATOR_IS_NOT_NULL: {
            res["op"] = "not";
            auto &op_expr = expr.Cast<BoundOperatorExpression>();
            res["children"] = json::array();
            json is_null_child{};
            is_null_child["op"] = "isNull";
            is_null_child["children"] = json::array();
            is_null_child["children"].push_back(OperandJSON(*op_expr.children[0]));
            res["children"].push_back(is_null_child);
            break;
        } break;
        case ExpressionType::CONJUNCTION_AND: {
            auto& expr_node = expr.Cast<BoundConjunctionExpression>();
            res["op"] = "and";
            res["children"] = json::array();
            if (expr_node.children.size())
                res["children"].push_back(ParseExpressionHint(*expr_node.children[0]));
            if (expr_node.children.size() > 1)
                res["children"].push_back(ParseExpressionHint(*expr_node.children[1]));
        } break;
        case ExpressionType::CONJUNCTION_OR: {
            auto& expr_node = expr.Cast<BoundConjunctionExpression>();
            res["op"] = "or";
            res["children"] = json::array();
            if (expr_node.children.size())
                res["children"].push_back(ParseExpressionHint(*expr_node.children[0]));
            if (expr_node.children.size() > 1)
                res["children"].push_back(ParseExpressionHint(*expr_node.children[1]));
        } break;
        case ExpressionType::COMPARE_BETWEEN: {
            res["op"] = "and";
            res["children"] = json::array();
            auto& between = expr.Cast<BoundBetweenExpression>();
            auto lower_comp = between.LowerComparisonType();
            auto upper_comp = between.UpperComparisonType();
            auto left_expr = make_uniq<BoundComparisonExpression>(
                lower_comp, between.input->Copy(), between.lower->Copy());
            auto right_expr = make_uniq<BoundComparisonExpression>(
                upper_comp, between.input->Copy(), between.upper->Copy());
            res["children"].push_back(ParseExpressionHint(*left_expr));
            res["children"].push_back(ParseExpressionHint(*right_expr));
        }
        default:
            break;
    }
    return res;
}

static json GetPredicateHints(vector<unique_ptr<Expression>>& filters) {
    std::vector<json> hints;
    for (auto& expr: filters) {
        hints.push_back(ParseExpressionHint(*expr));
    }

    if (hints.empty()) return json{};
    if (hints.size() == 1) return hints[0];
    json combined_hints{};
    combined_hints["op"] = "and";
    combined_hints["children"] = json::array();

    for (auto& hint: hints) {
        combined_hints["children"].push_back(hint);
    }
    return combined_hints;
}

static LogicalType DeltaTypeToDuckDBType(const std::string &delta_type) {
    if (DeltaLogicalMap.find(delta_type) != DeltaLogicalMap.end())
        return DeltaLogicalMap[delta_type];
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
    if (!filters.empty()) {
        bind_data.predicate_hints = GetPredicateHints(filters);
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
        auto query_result = client.QueryTableMetadata(result->share_name, result->schema_name, result->table_name);
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
    try {
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
    } catch (const std::exception &e) {
        throw IOException("Failed to read Delta Share table with filters: " + std::string(e.what()));
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
    std::string query;

    // Apply predicate hints as WHERE clause, but exclude partition columns
    // that don't exist in the Parquet files
    if (!gstate.parquet_query.empty()) {
        query = gstate.parquet_query;
    } else {
        query = "SELECT * FROM read_parquet('" + file.url + "')";
        std::vector<std::string> parquet_predicates;
        for (const auto &hint : bind_data.filters) {
            if (parquet_predicates.empty() && (hint == "AND" || hint == "OR"))
                continue;
            std::string col_name = ExtractColumnNameFromHint(hint);
            // Only include predicates for columns that exist in the Parquet file
            if (col_name.empty() || bind_data.partition_columns.find(col_name) == bind_data.partition_columns.end()) {
                parquet_predicates.push_back(hint);
            }
        }

        if (!parquet_predicates.empty()) {
            query += " WHERE ";
            for (size_t i = 0; i < parquet_predicates.size(); i++) {
                if (parquet_predicates[i] == "OR" || parquet_predicates[i] == "AND") continue;
                if (i > 0) {
                    query+= ' ' + parquet_predicates[i - 1] + ' ';
                }
                query += parquet_predicates[i];
            }
        }
        gstate.parquet_query = query;
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

    // Unified list function that accepts 0, 1, or 2 arguments
    TableFunction list("delta_share_list", {}, ListFunction, ListBind);
    list.varargs = LogicalType::VARCHAR;

    TableFunction read_delta_share("delta_share_read",
                                   {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
                                   ReadDeltaShareFunction, ReadDeltaShareBind, ReadDeltaShareInit);
    read_delta_share.pushdown_complex_filter = ReadDeltaSharePushdownComplexFilter;
    loader.RegisterFunction(list);
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