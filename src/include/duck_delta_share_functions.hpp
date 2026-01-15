#pragma once

#include "duckdb.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "delta_sharing_json.hpp"
#include <nlohmann/json.hpp>
#include <unordered_set>

namespace duckdb {

using json = nlohmann::json;

// Delta type mapping
extern std::unordered_map<std::string, LogicalType> DeltaLogicalMap;

// Expression parsing helpers
std::string ExtractColumnNameFromHint(const std::string &hint);

void ParseExpression(Expression &expr, std::vector<std::string> &res);

json OperandJSON(Expression &expr);

json BinaryOpJSON(Expression &expr, std::string op);

json ParseExpressionHint(Expression &expr);

json GetPredicateHints(vector<unique_ptr<Expression>> &filters);

// ParsedExpression to JSON helpers (for string predicate parsing)
json ParsedOperandJSON(ParsedExpression &expr);

json ParsedBinaryOpJSON(ParsedExpression &expr, std::string op);

json ParseParsedExpressionHint(ParsedExpression &expr);

json ParsePredicateStringToJson(const std::string &predicate_string);

// Schema parsing helpers
LogicalType DeltaTypeToDuckDBType(const std::string &delta_type);

void ParseDeltaSchema(const std::string &schema_json,
                      vector<string> &names,
                      vector<LogicalType> &types,
                      const json &partition_columns_json,
                      std::unordered_set<std::string> &partition_columns);

} // namespace duckdb
