#pragma once

#include "duckdb.hpp"
#include <string>
#include <vector>
#include <memory>
#include <nlohmann/json.hpp>

namespace duckdb {

using json = nlohmann::json;

// Delta Sharing Profile structure
struct DeltaSharingProfile {
    int share_credentials_version;
    std::string endpoint;
    std::string bearer_token;
    std::string expiration_time; // Optional, ISO 8601 format

    static DeltaSharingProfile FromConfig(ClientContext &context);
};

// Delta Sharing API response structures
struct Share {
    std::string name;
    std::string id; // Optional
};

struct Schema {
    std::string name;
    std::string share;
    std::string id; // Optional
};

struct Table {
    std::string name;
    std::string schema;
    std::string share;
    std::string id; // Optional
    std::string share_id; // Optional
};

struct Protocol {
    int min_reader_version;
};

struct Format {
    std::string provider; // "parquet"
    json options;
};

struct TableMetadata {
    std::string id;
    std::string name;
    std::string description;
    Format format;
    std::string schema_string;
    json partition_columns;
    json configuration;
    int version;
};

struct FileAction {
    std::string url;
    std::string id;
    json partition_values;
    int64_t size;
    json stats; // Optional
    int64_t version; // Optional
    int64_t timestamp; // Optional
    std::string expiration_timestamp; // Optional
};

// HTTP Response structure
struct HttpResponse {
    long status_code;
    std::string body;
    std::string error_message;
    bool success;
};

// Delta Sharing Client
class DeltaSharingClient {
public:
    DeltaSharingClient(const DeltaSharingProfile &profile);
    ~DeltaSharingClient();

    // List all shares
    std::vector<Share> ListShares(int max_results = -1, const std::string &page_token = "");

    // Get a specific share
    Share GetShare(const std::string &share_name);

    // List schemas in a share
    std::vector<Schema> ListSchemas(const std::string &share_name, int max_results = -1, const std::string &page_token = "");

    // List tables in a schema
    std::vector<Table> ListTables(const std::string &share_name, const std::string &schema_name, int max_results = -1, const std::string &page_token = "");

    // List all tables in a share
    std::vector<Table> ListAllTables(const std::string &share_name, int max_results = -1, const std::string &page_token = "");

    // Get table metadata
    struct TableMetadataResponse {
        Protocol protocol;
        TableMetadata metadata;
    };
    TableMetadataResponse QueryTableMetadata(const std::string &share_name, const std::string &schema_name, const std::string &table_name);

    // Get table version
    int64_t QueryTableVersion(const std::string &share_name, const std::string &schema_name, const std::string &table_name);

    // Query table files
    struct QueryTableResult {
        Protocol protocol;
        TableMetadata metadata;
        std::vector<FileAction> files;
    };
    QueryTableResult QueryTable(
        const std::string &share_name,
        const std::string &schema_name,
        const std::string &table_name,
        const json &predicate_hints = {},
        int64_t limit_hint = -1,
        int64_t version = -1);

private:
    DeltaSharingProfile profile_;
    void *curl_; // CURL handle (opaque pointer to avoid including curl.h in header)

    // HTTP request helper
    HttpResponse PerformRequest(
        const std::string &method,
        const std::string &path,
        const std::string &query_params = "",
        const std::string &post_data = "");

    // Build URL with query parameters
    std::string BuildUrl(const std::string &path, const std::string &query_params = "");

    // Parse newline-delimited JSON response
    std::vector<json> ParseNDJson(const std::string &response);
};

} // namespace duckdb
