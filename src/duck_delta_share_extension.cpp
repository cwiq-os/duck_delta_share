#define DUCKDB_EXTENSION_MAIN

#include "duck_delta_share_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

struct DeltaShareFilterBindData : public TableFunctionData {
    vector<string> filter_descriptions;
    string input_name;
    idx_t row_count = 10; // Generate some rows to actually filter
};

struct DeltaShareFilterLocalState : public LocalTableFunctionState {
    idx_t current_row = 0;
};

static unique_ptr<FunctionData> DeltaShareFilterBind(
		ClientContext &context,
		TableFunctionBindInput &input,
		vector<LogicalType> &return_types,
		vector<string> &names) {

    auto result = make_uniq<DeltaShareFilterBindData>();

    // Get the name parameter if provided
    if (input.inputs.size() > 0) {
        result->input_name = input.inputs[0].GetValue<string>();
    } else {
        result->input_name = "Duck";
    }

    // Define output schema: name, filter, and row_id
    names.push_back("name");
    names.push_back("filter");
    names.push_back("row_id");

    return_types.push_back(LogicalType::VARCHAR);
    return_types.push_back(LogicalType::VARCHAR);
    return_types.push_back(LogicalType::INTEGER);

    return std::move(result);
}

static unique_ptr<LocalTableFunctionState> DeltaShareFilterInitLocal(
		ExecutionContext &context,
		TableFunctionInitInput &input,
		GlobalTableFunctionState *global_state) {
	return make_uniq<DeltaShareFilterLocalState>();
}

// Helper function to convert TableFilter to string
static string FilterToString(const TableFilter &filter) {
    switch (filter.filter_type) {
        case TableFilterType::CONSTANT_COMPARISON: {
            auto &const_filter = filter.Cast<ConstantFilter>();
            string op;
            switch (const_filter.comparison_type) {
                case ExpressionType::COMPARE_EQUAL:
                    op = "=";
                    break;
                case ExpressionType::COMPARE_NOTEQUAL:
                    op = "!=";
                    break;
                case ExpressionType::COMPARE_LESSTHAN:
                    op = "<";
                    break;
                case ExpressionType::COMPARE_GREATERTHAN:
                    op = ">";
                    break;
                case ExpressionType::COMPARE_LESSTHANOREQUALTO:
                    op = "<=";
                    break;
                case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
                    op = ">=";
                    break;
                default:
                    op = "?";
            }
            return "column " + op + " " + const_filter.constant.ToString();
        }
        case TableFilterType::IS_NULL:
            return "column IS NULL";
        case TableFilterType::IS_NOT_NULL:
            return "column IS NOT NULL";
        case TableFilterType::CONJUNCTION_AND: {
            auto &conj = filter.Cast<ConjunctionAndFilter>();
            string result = "(";
            for (idx_t i = 0; i < conj.child_filters.size(); i++) {
                if (i > 0) result += " AND ";
                result += FilterToString(*conj.child_filters[i]);
            }
            result += ")";
            return result;
        }
        case TableFilterType::CONJUNCTION_OR: {
            auto &conj = filter.Cast<ConjunctionOrFilter>();
            string result = "(";
            for (idx_t i = 0; i < conj.child_filters.size(); i++) {
                if (i > 0) result += " OR ";
                result += FilterToString(*conj.child_filters[i]);
            }
            result += ")";
            return result;
        }
        default:
            return "unknown filter";
    }
}

inline void DeltaShare(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "DuckDeltaShare " + name.GetString() + " 🐥");
	});
}

static void DeltaShareFilterPushdown(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<column_t> &column_ids,
    optional_ptr<TableFilterSet> filters) {

    if (!filters) {
        return;
    }

    auto &bind_data = input.bind_data->Cast<DeltaShareFilterBindData>();

    // Capture all filters for display purposes
    for (auto &entry : filters->filters) {
        idx_t column_index = entry.first;
        auto &filter = *entry.second;

        string filter_desc = "Column[" + to_string(column_index) + "]: " +
                           FilterToString(filter);
        bind_data.filter_descriptions.push_back(filter_desc);
    }

    // IMPORTANT: Don't remove the filters from the TableFilterSet
    // By not modifying or returning anything, DuckDB will still apply the filters
}

static void DeltaShareFilterFunction(
    ClientContext &context,
    TableFunctionInput &data_p,
    DataChunk &output) {

    auto &bind_data = data_p.bind_data->Cast<DeltaShareFilterBindData>();
    auto &local_state = data_p.local_state->Cast<DeltaShareFilterLocalState>();

    idx_t count = 0;

    // Generate rows - DuckDB will apply filters after we return them
    while (local_state.current_row < bind_data.row_count && count < STANDARD_VECTOR_SIZE) {
        string name_value = "Quack " + bind_data.input_name + " 🐥";

        string filter_value;
        if (bind_data.filter_descriptions.empty()) {
            filter_value = "No filters applied";
        } else {
            // Show all filters in each row
            filter_value = "";
            for (idx_t i = 0; i < bind_data.filter_descriptions.size(); i++) {
                if (i > 0) filter_value += " AND ";
                filter_value += bind_data.filter_descriptions[i];
            }
        }

        output.SetValue(0, count, Value(name_value));
        output.SetValue(1, count, Value(filter_value));
        output.SetValue(2, count, Value::INTEGER(static_cast<int32_t>(local_state.current_row)));

        local_state.current_row++;
        count++;
    }

    output.SetCardinality(count);
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register a scalar function
	auto delta_share = ScalarFunction("delta_share", {LogicalType::VARCHAR}, LogicalType::VARCHAR, DeltaShare);
	loader.RegisterFunction(delta_share);

	// Register table function with filter pushdown
	TableFunction delta_share_filter("delta_share_filter", {LogicalType::VARCHAR}, DeltaShareFilterFunction, DeltaShareFilterBind, DeltaShareFilterInitLocal);
	delta_share_filter.filter_pushdown = true;
	delta_share_filter.pushdown_complex_filter = DeltaShareFilterPushdown;
	loader.RegisterFunction(delta_share_filter);
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
