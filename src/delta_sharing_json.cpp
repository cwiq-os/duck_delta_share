#include "delta_sharing_json.hpp"
#include <nlohmann/json.hpp>

namespace duckdb {

using json = nlohmann::json;

// Pimpl implementation
struct JsonValue::Impl {
    json data;

    Impl() : data(json::object()) {}
    explicit Impl(const json &j) : data(j) {}
    explicit Impl(json &&j) : data(std::move(j)) {}
};

// Constructors and destructor
JsonValue::JsonValue() : pImpl(new Impl()) {}

JsonValue::JsonValue(const std::string &json_str) : pImpl(new Impl()) {
    try {
        pImpl->data = json::parse(json_str);
    } catch (const json::parse_error &e) {
        // Keep as empty object on parse error
        pImpl->data = json::object();
    }
}

JsonValue::JsonValue(const JsonValue &other) : pImpl(new Impl(other.pImpl->data)) {}

JsonValue::JsonValue(JsonValue &&other) noexcept : pImpl(std::move(other.pImpl)) {
    if (!pImpl) {
        pImpl.reset(new Impl());
    }
}

JsonValue& JsonValue::operator=(const JsonValue &other) {
    if (this != &other) {
        pImpl.reset(new Impl(other.pImpl->data));
    }
    return *this;
}

JsonValue& JsonValue::operator=(JsonValue &&other) noexcept {
    if (this != &other) {
        pImpl = std::move(other.pImpl);
        if (!pImpl) {
            pImpl.reset(new Impl());
        }
    }
    return *this;
}

JsonValue::~JsonValue() = default;

// Private constructor from internal pointer
JsonValue::JsonValue(void *internal_ptr) : pImpl(new Impl()) {
    if (internal_ptr) {
        pImpl->data = *static_cast<json*>(internal_ptr);
    }
}

// Serialization
std::string JsonValue::Dump() const {
    return pImpl->data.dump();
}

std::string JsonValue::DumpPretty() const {
    return pImpl->data.dump(2);
}

// Query methods
bool JsonValue::IsEmpty() const {
    return pImpl->data.empty();
}

bool JsonValue::IsNull() const {
    return pImpl->data.is_null();
}

bool JsonValue::IsObject() const {
    return pImpl->data.is_object();
}

bool JsonValue::IsArray() const {
    return pImpl->data.is_array();
}

bool JsonValue::Contains(const std::string &key) const {
    return pImpl->data.contains(key);
}

// Get values
std::string JsonValue::GetString(const std::string &key) const {
    return pImpl->data.at(key).get<std::string>();
}

std::string JsonValue::GetStringOr(const std::string &key, const std::string &default_val) const {
    return pImpl->data.value(key, default_val);
}

int JsonValue::GetInt(const std::string &key) const {
    return pImpl->data.at(key).get<int>();
}

int64_t JsonValue::GetInt64(const std::string &key) const {
    return pImpl->data.at(key).get<int64_t>();
}

int JsonValue::GetIntOr(const std::string &key, int default_val) const {
    return pImpl->data.value(key, default_val);
}

JsonValue JsonValue::Get(const std::string &key) const {
    if (pImpl->data.contains(key)) {
        return JsonValue::FromInternal(&pImpl->data.at(key));
    }
    return JsonValue::Object();
}

JsonValue JsonValue::GetOr(const std::string &key, const JsonValue &default_val) const {
    if (pImpl->data.contains(key)) {
        return JsonValue::FromInternal(&pImpl->data.at(key));
    }
    return default_val;
}

// Array access
size_t JsonValue::Size() const {
    return pImpl->data.size();
}

JsonValue JsonValue::At(size_t index) const {
    return JsonValue::FromInternal(&pImpl->data.at(index));
}

// Factory methods
JsonValue JsonValue::Array() {
    JsonValue result;
    result.pImpl->data = json::array();
    return result;
}

JsonValue JsonValue::Object() {
    JsonValue result;
    result.pImpl->data = json::object();
    return result;
}

JsonValue JsonValue::Parse(const std::string &json_str) {
    return JsonValue(json_str);
}

// Internal access
void* JsonValue::GetInternalPtr() {
    return &pImpl->data;
}

const void* JsonValue::GetInternalPtr() const {
    return &pImpl->data;
}

JsonValue JsonValue::FromInternal(const void *ptr) {
    if (!ptr) {
        return JsonValue::Object();
    }
    JsonValue result;
    result.pImpl->data = *static_cast<const json*>(ptr);
    return result;
}

} // namespace duckdb
