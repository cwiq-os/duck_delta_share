#include "duck_delta_share_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/expression/between_expression.hpp"

namespace duckdb {

std::unordered_map<std::string, LogicalType> DeltaLogicalMap = {
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

std::string ExtractColumnNameFromHint(const std::string &hint) {
    size_t pos = hint.find_first_of(" =<>!");
    if (pos != std::string::npos) {
        return hint.substr(0, pos);
    }
    return "";
}

void ParseExpression(Expression& expr, std::vector<std::string>& res) {
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

json OperandJSON(Expression& expr) {
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

json BinaryOpJSON(Expression& expr, std::string op) {
    json res{};
    res["op"] = op;
    res["children"] = json::array();
    auto &comp_expr = expr.Cast<BoundComparisonExpression>();
    if (comp_expr.left) res["children"].push_back(OperandJSON(*comp_expr.left));
    if (comp_expr.right) res["children"].push_back(OperandJSON(*comp_expr.right));
    return res;
}

json ParseExpressionHint(Expression& expr) {
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

json GetPredicateHints(vector<unique_ptr<Expression>>& filters) {
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

LogicalType DeltaTypeToDuckDBType(const std::string &delta_type) {
    if (DeltaLogicalMap.find(delta_type) != DeltaLogicalMap.end())
        return DeltaLogicalMap[delta_type];
    return LogicalType::VARCHAR;
}

void ParseDeltaSchema(const std::string &schema_json, vector<string> &names, vector<LogicalType> &types, const json &partition_columns_json, std::unordered_set<std::string> &partition_columns) {
    try {
        auto schema = json::parse(schema_json);

        if (!schema.contains("fields") || !schema["fields"].is_array()) {
            throw IOException("ParseDeltaSchema error: missing or invalid 'fields' array");
        }

        // Get partition columns. These columns are to be excluded in read_parquet
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

            // Simple implementation for now.
            // Type can be a string or an object
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
        throw IOException("ParseDeltaSchema error: " + std::string(e.what()));
    }
}

// ParsedExpression to JSON helpers for string predicate parsing

json ParsedOperandJSON(ParsedExpression &expr) {
    json res{};

    if (expr.expression_class == ExpressionClass::COLUMN_REF) {
        auto &column = expr.Cast<ColumnRefExpression>();
        res["op"] = "column";
        res["name"] = column.GetColumnName();
        res["valueType"] = "string";  // Default type, will be inferred from context
    } else if (expr.expression_class == ExpressionClass::CONSTANT) {
        auto &literal = expr.Cast<ConstantExpression>();
        res["op"] = "literal";
        res["value"] = literal.value.ToString();
        // Infer valueType from Value type
        switch (literal.value.type().id()) {
            case LogicalTypeId::BOOLEAN:
            case LogicalTypeId::TINYINT:
            case LogicalTypeId::SMALLINT:
            case LogicalTypeId::INTEGER:
            case LogicalTypeId::BIGINT:
            case LogicalTypeId::FLOAT:
            case LogicalTypeId::DOUBLE:
                res["valueType"] = "int";
                break;
            case LogicalTypeId::VARCHAR:
            default:
                res["valueType"] = "string";
                break;
        }
    }
    return res;
}

json ParsedBinaryOpJSON(ParsedExpression &expr, std::string op) {
    json res{};
    res["op"] = op;
    res["children"] = json::array();
    auto &comp_expr = expr.Cast<ComparisonExpression>();
    if (comp_expr.left) res["children"].push_back(ParsedOperandJSON(*comp_expr.left));
    if (comp_expr.right) res["children"].push_back(ParsedOperandJSON(*comp_expr.right));
    return res;
}

json ParseParsedExpressionHint(ParsedExpression &expr) {
    json res{};
    std::string op;

    switch (expr.type) {
        case ExpressionType::COMPARE_EQUAL: {
            op = "equal";
            res = ParsedBinaryOpJSON(expr, op);
        } break;
        case ExpressionType::COMPARE_LESSTHAN: {
            op = "lessThan";
            res = ParsedBinaryOpJSON(expr, op);
        } break;
        case ExpressionType::COMPARE_GREATERTHAN: {
            op = "greaterThan";
            res = ParsedBinaryOpJSON(expr, op);
        } break;
        case ExpressionType::COMPARE_LESSTHANOREQUALTO: {
            op = "lessThanOrEqual";
            res = ParsedBinaryOpJSON(expr, op);
        } break;
        case ExpressionType::COMPARE_GREATERTHANOREQUALTO: {
            op = "greaterThanOrEqual";
            res = ParsedBinaryOpJSON(expr, op);
        } break;
        case ExpressionType::COMPARE_NOTEQUAL: {
            op = "equal";
            res["op"] = "not";
            res["children"] = json::array();
            json equal_predicate = ParsedBinaryOpJSON(expr, op);
            res["children"].push_back(equal_predicate);
        } break;
        case ExpressionType::OPERATOR_IS_NULL: {
            res["op"] = "isNull";
            auto &op_expr = expr.Cast<OperatorExpression>();
            res["children"] = json::array();
            if (!op_expr.children.empty()) {
                res["children"].push_back(ParsedOperandJSON(*op_expr.children[0]));
            }
        } break;
        case ExpressionType::OPERATOR_IS_NOT_NULL: {
            res["op"] = "not";
            auto &op_expr = expr.Cast<OperatorExpression>();
            res["children"] = json::array();
            json is_null_child{};
            is_null_child["op"] = "isNull";
            is_null_child["children"] = json::array();
            if (!op_expr.children.empty()) {
                is_null_child["children"].push_back(ParsedOperandJSON(*op_expr.children[0]));
            }
            res["children"].push_back(is_null_child);
        } break;
        case ExpressionType::CONJUNCTION_AND: {
            auto &expr_node = expr.Cast<ConjunctionExpression>();
            res["op"] = "and";
            res["children"] = json::array();
            for (auto &child : expr_node.children) {
                res["children"].push_back(ParseParsedExpressionHint(*child));
            }
        } break;
        case ExpressionType::CONJUNCTION_OR: {
            auto &expr_node = expr.Cast<ConjunctionExpression>();
            res["op"] = "or";
            res["children"] = json::array();
            for (auto &child : expr_node.children) {
                res["children"].push_back(ParseParsedExpressionHint(*child));
            }
        } break;
        case ExpressionType::COMPARE_BETWEEN: {
            res["op"] = "and";
            res["children"] = json::array();
            auto &between = expr.Cast<BetweenExpression>();
            // Create >= lower and <= upper comparisons
            json lower_cmp{};
            lower_cmp["op"] = "greaterThanOrEqual";
            lower_cmp["children"] = json::array();
            lower_cmp["children"].push_back(ParsedOperandJSON(*between.input));
            lower_cmp["children"].push_back(ParsedOperandJSON(*between.lower));

            json upper_cmp{};
            upper_cmp["op"] = "lessThanOrEqual";
            upper_cmp["children"] = json::array();
            upper_cmp["children"].push_back(ParsedOperandJSON(*between.input));
            upper_cmp["children"].push_back(ParsedOperandJSON(*between.upper));

            res["children"].push_back(lower_cmp);
            res["children"].push_back(upper_cmp);
        } break;
        default:
            break;
    }
    return res;
}

json ParsePredicateStringToJson(const std::string &predicate_string) {
    if (predicate_string.empty()) {
        return json{};
    }

    try {
        // Use DuckDB's parser to parse the predicate string
        auto expression_list = Parser::ParseExpressionList(predicate_string);

        if (expression_list.empty()) {
            return json{};
        }

        // Combine multiple expressions with AND if needed
        if (expression_list.size() == 1) {
            return ParseParsedExpressionHint(*expression_list[0]);
        }

        json combined{};
        combined["op"] = "and";
        combined["children"] = json::array();
        for (auto &expr : expression_list) {
            combined["children"].push_back(ParseParsedExpressionHint(*expr));
        }
        return combined;
    } catch (const std::exception &e) {
        throw IOException("ParsePredicateStringToJson error: failed to parse predicate '" +
                         predicate_string + "': " + std::string(e.what()));
    }
}

} // namespace duckdb
