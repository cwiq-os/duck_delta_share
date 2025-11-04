#pragma once

#include <string>
#include <memory>
#include <vector>

namespace duckdb {

// Forward declaration-friendly JSON wrapper class
// Hides nlohmann::json from header files using Pimpl idiom
class JsonValue {
public:
    JsonValue();
    explicit JsonValue(const std::string &json_str);
    JsonValue(const JsonValue &other);
    JsonValue(JsonValue &&other) noexcept;
    JsonValue& operator=(const JsonValue &other);
    JsonValue& operator=(JsonValue &&other) noexcept;
    ~JsonValue();

    // Serialization
    std::string Dump() const;
    std::string DumpPretty() const;

    // Query methods
    bool IsEmpty() const;
    bool IsNull() const;
    bool IsObject() const;
    bool IsArray() const;
    bool Contains(const std::string &key) const;

    // Get values
    std::string GetString(const std::string &key) const;
    std::string GetStringOr(const std::string &key, const std::string &default_val) const;
    int GetInt(const std::string &key) const;
    int64_t GetInt64(const std::string &key) const;
    int GetIntOr(const std::string &key, int default_val) const;
    JsonValue Get(const std::string &key) const;
    JsonValue GetOr(const std::string &key, const JsonValue &default_val) const;

    // Array access
    size_t Size() const;
    JsonValue At(size_t index) const;

    // Factory methods
    static JsonValue Array();
    static JsonValue Object();
    static JsonValue Parse(const std::string &json_str);

    // Internal access for .cpp files only
    // Returns pointer to nlohmann::json - only use in implementation files
    void *GetInternalPtr();
    const void *GetInternalPtr() const;

    // Create JsonValue from internal pointer (nlohmann::json*)
    static JsonValue FromInternal(const void *ptr);

private:
    struct Impl;
    std::unique_ptr<Impl> pImpl;

    // Private constructor for internal use
    explicit JsonValue(void *internal_ptr);
};

} // namespace duckdb
