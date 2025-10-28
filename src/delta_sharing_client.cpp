#include "delta_sharing_client.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include <curl/curl.h>
#include <sstream>
#include <fstream>

namespace duckdb {

// Callback for libcurl to write response data
static size_t WriteCallback(void *contents, size_t size, size_t nmemb, void *userp) {
    ((std::string *)userp)->append((char *)contents, size * nmemb);
    return size * nmemb;
}

// DeltaSharingProfile implementation
DeltaSharingProfile DeltaSharingProfile::FromConfig(ClientContext &context) {
    DeltaSharingProfile profile;

    // Get endpoint from DuckDB configuration
    Value endpoint_value;
    if (!context.TryGetCurrentSetting("delta_sharing_endpoint", endpoint_value) ||
        endpoint_value.IsNull() || endpoint_value.ToString().empty()) {
        throw IOException("delta_sharing_endpoint configuration parameter is not set. Use: SET delta_sharing_endpoint='your_endpoint'");
    }
    profile.endpoint = endpoint_value.ToString();

    // Get bearer token from DuckDB configuration
    Value token_value;
    if (!context.TryGetCurrentSetting("delta_sharing_bearer_token", token_value) ||
        token_value.IsNull() || token_value.ToString().empty()) {
        throw IOException("delta_sharing_bearer_token configuration parameter is not set. Use: SET delta_sharing_bearer_token='your_token'");
    }
    profile.bearer_token = token_value.ToString();

    // Get optional fields
    profile.share_credentials_version = 1;
    Value version_value;
    if (context.TryGetCurrentSetting("delta_sharing_credentials_version", version_value) &&
        !version_value.IsNull()) {
        profile.share_credentials_version = version_value.GetValue<int>();
    }

    profile.expiration_time = "";
    Value expiration_value;
    if (context.TryGetCurrentSetting("delta_sharing_expiration_time", expiration_value) &&
        !expiration_value.IsNull()) {
        profile.expiration_time = expiration_value.ToString();
    }

    // Remove trailing slash from endpoint if present
    if (!profile.endpoint.empty() && profile.endpoint.back() == '/') {
        profile.endpoint.pop_back();
    }

    return profile;
}

// DeltaSharingClient implementation
DeltaSharingClient::DeltaSharingClient(const DeltaSharingProfile &profile)
    : profile_(profile) {
    curl_ = curl_easy_init();
    if (!curl_) {
        throw InternalException("Failed to initialize CURL");
    }
}

DeltaSharingClient::~DeltaSharingClient() {
    if (curl_) {
        curl_easy_cleanup((CURL *)curl_);
    }
}

std::string DeltaSharingClient::BuildUrl(const std::string &path, const std::string &query_params) {
    std::string url = profile_.endpoint + path;
    if (!query_params.empty()) {
        url += "?" + query_params;
    }
    return url;
}

HttpResponse DeltaSharingClient::PerformRequest(
    const std::string &method,
    const std::string &path,
    const std::string &query_params,
    const std::string &post_data) {

    HttpResponse response;
    response.success = false;
    response.status_code = 0;

    CURL *curl = (CURL *)curl_;
    std::string url = BuildUrl(path, query_params);
    std::string response_body;

    // Set URL
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());

    // Set method
    if (method == "GET") {
        curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);
    } else if (method == "POST") {
        curl_easy_setopt(curl, CURLOPT_POST, 1L);
        // Todo: load necessary request body.
        // For queries with WHERE clause, extract and load predicate hints
        if (post_data.empty() || post_data == "null") {
            const char* empty_body = "{}";
            curl_easy_setopt(curl, CURLOPT_POSTFIELDS, empty_body);
        } else {
            std::cout << post_data << '\n';
            curl_easy_setopt(curl, CURLOPT_POSTFIELDS, post_data.data());
        }

    } else if (method == "HEAD") {
        curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
    }

    // Set headers
    struct curl_slist *headers = nullptr;
    std::string auth_header = "Authorization: Bearer " + profile_.bearer_token;
    headers = curl_slist_append(headers, auth_header.c_str());
    headers = curl_slist_append(headers, "Content-Type: application/json; charset=utf-8");
    headers = curl_slist_append(headers, "delta-sharing-capabilities: responseformat=parquet");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

    // Set write callback
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response_body);

    // Perform request
    CURLcode res = curl_easy_perform(curl);

    // Get status code
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response.status_code);

    // Cleanup headers
    curl_slist_free_all(headers);

    if (res != CURLE_OK) {
        response.error_message = curl_easy_strerror(res);
        response.success = false;
        return response;
    }

    response.body = response_body;
    response.success = (response.status_code >= 200 && response.status_code < 300);

    if (!response.success && !response_body.empty()) {
        try {
            auto error_json = json::parse(response_body);
            if (error_json.contains("message")) {
                std::cout << error_json["message"].get<std::string>() << "\n";
                response.error_message = error_json["message"].get<std::string>();
            }
        } catch (...) {
            response.error_message = response_body;
        }
    }

    return response;
}

std::vector<json> DeltaSharingClient::ParseNDJson(const std::string &response) {
    std::vector<json> results;
    std::istringstream stream(response);
    std::string line;

    while (std::getline(stream, line)) {
        if (line.empty()) {
            continue;
        }
        try {
            results.push_back(json::parse(line));
        } catch (const std::exception &e) {
            throw IOException("Failed to parse JSON line: " + std::string(e.what()));
        }
    }

    return results;
}

std::vector<Share> DeltaSharingClient::ListShares(int max_results, const std::string &page_token) {
    std::string query_params;
    if (max_results > 0) {
        query_params += "maxResults=" + std::to_string(max_results);
    }
    if (!page_token.empty()) {
        if (!query_params.empty()) query_params += "&";
        query_params += "pageToken=" + page_token;
    }

    auto response = PerformRequest("GET", "/shares", query_params);
    if (!response.success) {
        throw IOException("Failed to list shares: " + response.error_message);
    }

    std::vector<Share> shares;
    try {
        auto j = json::parse(response.body);
        if (j.contains("items")) {
            for (const auto &item : j["items"]) {
                Share share;
                share.name = item.at("name").get<std::string>();
                share.id = item.value("id", "");
                shares.push_back(share);
            }
        }
    } catch (const std::exception &e) {
        throw IOException("Failed to parse shares response: " + std::string(e.what()));
    }

    return shares;
}

Share DeltaSharingClient::GetShare(const std::string &share_name) {
    auto response = PerformRequest("GET", "/shares/" + share_name);
    if (!response.success) {
        throw IOException("Failed to get share: " + response.error_message);
    }

    Share share;
    try {
        auto j = json::parse(response.body);
        share.name = j.at("share").at("name").get<std::string>();
        share.id = j.at("share").value("id", "");
    } catch (const std::exception &e) {
        throw IOException("Failed to parse share response: " + std::string(e.what()));
    }

    return share;
}

std::vector<Schema> DeltaSharingClient::ListSchemas(const std::string &share_name, int max_results, const std::string &page_token) {
    std::string query_params;
    if (max_results > 0) {
        query_params += "maxResults=" + std::to_string(max_results);
    }
    if (!page_token.empty()) {
        if (!query_params.empty()) query_params += "&";
        query_params += "pageToken=" + page_token;
    }

    auto response = PerformRequest("GET", "/shares/" + share_name + "/schemas", query_params);
    if (!response.success) {
        throw IOException("Failed to list schemas: " + response.error_message);
    }

    std::vector<Schema> schemas;
    try {
        auto j = json::parse(response.body);
        if (j.contains("items")) {
            for (const auto &item : j["items"]) {
                Schema schema;
                schema.name = item.at("name").get<std::string>();
                schema.share = item.at("share").get<std::string>();
                schema.id = item.value("id", "");
                schemas.push_back(schema);
            }
        }
    } catch (const std::exception &e) {
        throw IOException("Failed to parse schemas response: " + std::string(e.what()));
    }

    return schemas;
}

std::vector<Table> DeltaSharingClient::ListTables(const std::string &share_name, const std::string &schema_name, int max_results, const std::string &page_token) {
    std::string query_params;
    if (max_results > 0) {
        query_params += "maxResults=" + std::to_string(max_results);
    }
    if (!page_token.empty()) {
        if (!query_params.empty()) query_params += "&";
        query_params += "pageToken=" + page_token;
    }

    auto response = PerformRequest("GET", "/shares/" + share_name + "/schemas/" + schema_name + "/tables", query_params);
    if (!response.success) {
        throw IOException("Failed to list tables: " + response.error_message);
    }

    std::vector<Table> tables;
    try {
        auto j = json::parse(response.body);
        if (j.contains("items")) {
            for (const auto &item : j["items"]) {
                Table table;
                table.name = item.at("name").get<std::string>();
                table.schema = item.at("schema").get<std::string>();
                table.share = item.at("share").get<std::string>();
                table.id = item.value("id", "");
                table.share_id = item.value("shareId", "");
                tables.push_back(table);
            }
        }
    } catch (const std::exception &e) {
        throw IOException("Failed to parse tables response: " + std::string(e.what()));
    }

    return tables;
}

std::vector<Table> DeltaSharingClient::ListAllTables(const std::string &share_name, int max_results, const std::string &page_token) {
    std::string query_params;
    if (max_results > 0) {
        query_params += "maxResults=" + std::to_string(max_results);
    }
    if (!page_token.empty()) {
        if (!query_params.empty()) query_params += "&";
        query_params += "pageToken=" + page_token;
    }

    auto response = PerformRequest("GET", "/shares/" + share_name + "/all-tables", query_params);
    if (!response.success) {
        throw IOException("Failed to list all tables: " + response.error_message);
    }

    std::vector<Table> tables;
    try {
        auto j = json::parse(response.body);
        if (j.contains("items")) {
            for (const auto &item : j["items"]) {
                Table table;
                table.name = item.at("name").get<std::string>();
                table.schema = item.at("schema").get<std::string>();
                table.share = item.at("share").get<std::string>();
                table.id = item.value("id", "");
                table.share_id = item.value("shareId", "");
                tables.push_back(table);
            }
        }
    } catch (const std::exception &e) {
        throw IOException("Failed to parse all tables response: " + std::string(e.what()));
    }

    return tables;
}

DeltaSharingClient::TableMetadataResponse DeltaSharingClient::QueryTableMetadata(
    const std::string &share_name,
    const std::string &schema_name,
    const std::string &table_name) {

    auto response = PerformRequest("GET", "/shares/" + share_name + "/schemas/" + schema_name + "/tables/" + table_name + "/metadata");
    if (!response.success) {
        throw IOException("Failed to query table metadata: " + response.error_message);
    }

    TableMetadataResponse result;
    try {
        auto lines = ParseNDJson(response.body);
        if (lines.size() < 2) {
            throw IOException("Invalid metadata response: expected at least 2 lines");
        }

        // First line: protocol
        auto &protocol_obj = lines[0].at("protocol");
        result.protocol.min_reader_version = protocol_obj.at("minReaderVersion").get<int>();

        // Second line: metadata
        auto &metadata_obj = lines[1].at("metaData");
        result.metadata.id = metadata_obj.at("id").get<std::string>();
        result.metadata.name = metadata_obj.value("name", "");
        result.metadata.description = metadata_obj.value("description", "");
        result.metadata.schema_string = metadata_obj.at("schemaString").get<std::string>();
        result.metadata.partition_columns = metadata_obj.value("partitionColumns", json::array());
        result.metadata.configuration = metadata_obj.value("configuration", json::object());
        result.metadata.version = metadata_obj.value("version", 0);

        auto &format_obj = metadata_obj.at("format");
        result.metadata.format.provider = format_obj.at("provider").get<std::string>();
        result.metadata.format.options = format_obj.value("options", json::object());

    } catch (const std::exception &e) {
        throw IOException("Failed to parse table metadata: " + std::string(e.what()));
    }

    return result;
}

int64_t DeltaSharingClient::QueryTableVersion(
    const std::string &share_name,
    const std::string &schema_name,
    const std::string &table_name) {

    auto response = PerformRequest("HEAD", "/shares/" + share_name + "/schemas/" + schema_name + "/tables/" + table_name);
    if (!response.success) {
        throw IOException("Failed to query table version: " + response.error_message);
    }

    // The version is returned in the Delta-Table-Version header
    // For now, we'll parse it from metadata as HEAD might not be fully supported
    auto metadata = QueryTableMetadata(share_name, schema_name, table_name);
    return metadata.metadata.version;
}

DeltaSharingClient::QueryTableResult DeltaSharingClient::QueryTable(
    const std::string &share_name,
    const std::string &schema_name,
    const std::string &table_name,
    const json &predicate_hints,
    int64_t limit_hint,
    int64_t version) {

    // Build POST request body
    json request_body;
    if (!predicate_hints.empty()) {
        request_body["predicateHints"] = json::array();
        request_body["predicateHints"].push_back("string");
        request_body["version"] = 0;
        request_body["jsonPredicateHints"] = predicate_hints.dump();
    }
    if (limit_hint > 0) {
        request_body["limitHint"] = limit_hint;
    }
    if (version > 0) {
        request_body["version"] = version;
    }

    std::string post_data = request_body.dump();

    auto response = PerformRequest("POST", "/shares/" + share_name + "/schemas/" + schema_name + "/tables/" + table_name + "/query", "", post_data);
    if (!response.success) {
        throw IOException("Failed to query table: " + response.error_message);
    }

    QueryTableResult result;
    try {
        auto lines = ParseNDJson(response.body);
        if (lines.size() < 2) {
            throw IOException("Invalid query response: expected at least 2 lines");
        }

        // First line: protocol
        auto &protocol_obj = lines[0].at("protocol");
        result.protocol.min_reader_version = protocol_obj.at("minReaderVersion").get<int>();

        // Second line: metadata
        auto &metadata_obj = lines[1].at("metaData");
        result.metadata.id = metadata_obj.at("id").get<std::string>();
        result.metadata.name = metadata_obj.value("name", "");
        result.metadata.description = metadata_obj.value("description", "");
        result.metadata.schema_string = metadata_obj.at("schemaString").get<std::string>();
        result.metadata.partition_columns = metadata_obj.value("partitionColumns", json::array());
        result.metadata.configuration = metadata_obj.value("configuration", json::object());
        result.metadata.version = metadata_obj.value("version", 0);

        auto &format_obj = metadata_obj.at("format");
        result.metadata.format.provider = format_obj.at("provider").get<std::string>();
        result.metadata.format.options = format_obj.value("options", json::object());

        // Remaining lines: files
        for (size_t i = 2; i < lines.size(); i++) {
            if (lines[i].contains("file")) {
                auto &file_obj = lines[i].at("file");
                FileAction file;
                file.url = file_obj.at("url").get<std::string>();
                file.id = file_obj.at("id").get<std::string>();
                file.partition_values = file_obj.value("partitionValues", json::object());
                file.size = file_obj.at("size").get<int64_t>();
                file.stats = file_obj.value("stats", json::object());
                file.version = file_obj.value("version", 0);
                file.timestamp = file_obj.value("timestamp", 0);
                file.expiration_timestamp = file_obj.value("expirationTimestamp", "");
                result.files.push_back(file);
            }
        }

    } catch (const std::exception &e) {
        throw IOException("Failed to parse query table response: " + std::string(e.what()));
    }

    return result;
}

} // namespace duckdb
