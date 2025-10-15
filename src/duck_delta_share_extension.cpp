#define DUCKDB_EXTENSION_MAIN

#include "duck_delta_share_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

inline void DuckDeltaShareScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "DuckDeltaShare " + name.GetString() + " 🐥");
	});
}

inline void DuckDeltaShareOpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "DuckDeltaShare " + name.GetString() + ", my linked OpenSSL version is " +
		                                           OPENSSL_VERSION_TEXT);
	});
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register a scalar function
	auto duck_delta_share_scalar_function = ScalarFunction("duck_delta_share", {LogicalType::VARCHAR}, LogicalType::VARCHAR, DuckDeltaShareScalarFun);
	loader.RegisterFunction(duck_delta_share_scalar_function);

	// Register another scalar function
	auto duck_delta_share_openssl_version_scalar_function = ScalarFunction("duck_delta_share_openssl_version", {LogicalType::VARCHAR},
	                                                            LogicalType::VARCHAR, DuckDeltaShareOpenSSLVersionScalarFun);
	loader.RegisterFunction(duck_delta_share_openssl_version_scalar_function);
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
