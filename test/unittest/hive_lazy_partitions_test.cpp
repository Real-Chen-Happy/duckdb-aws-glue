#include "api/glue_api.hpp"
#include "catalog/glue_catalog.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/multi_file/multi_file_states.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "glue_test_util.hpp"
#include "planning/hive_multi_file_reader.hpp"

#include <map>

using namespace duckdb;

namespace {

int64_t Count(Connection &con, const string &sql) {
	auto result = Run(con, sql);
	auto chunk = result->Fetch();
	REQUIRE(chunk != nullptr);
	REQUIRE(chunk->size() == 1);
	return chunk->GetValue(0, 0).GetValue<int64_t>();
}

int64_t GetPartitionsRequests(Connection &con) {
	return Count(con, "SELECT count(*) FROM glue_requests() WHERE operation = 'AWSGlue.GetPartitions'");
}

int64_t ListRequests(Connection &con) {
	return Count(con, "SELECT count(*) FROM duckdb_logs_parsed('HTTP') WHERE request.url LIKE '%list-type=2%'");
}

//! The segmented load of every partition, which a paging list does not make until it stops paging
int64_t FullLoads(Connection &con) {
	return Count(con, "SELECT count(*) FROM duckdb_logs_parsed('GlueScan') WHERE event = 'partitions'");
}

//! The listings planned without paging
int64_t PlannedListings(Connection &con) {
	return Count(con, "SELECT count(*) FROM duckdb_logs_parsed('GlueScan') WHERE event = 'plan'");
}

void CollectScans(LogicalOperator &op, vector<reference<LogicalGet>> &scans) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		scans.push_back(op.Cast<LogicalGet>());
	}
	for (auto &child : op.children) {
		CollectScans(*child, scans);
	}
}

//! The file list of a query's only scan, planned without the optimizer and marked limit bound as the planner marks
//! a scan under a LIMIT: the cardinality estimate would load every partition, which the planner skips for such a scan
class PlannedList {
public:
	PlannedList(Connection &con, const string &query, bool limit_bound) {
		plan = con.ExtractPlan(query);
		vector<reference<LogicalGet>> scans;
		CollectScans(*plan, scans);
		REQUIRE(scans.size() == 1);
		list = &scans[0].get().bind_data->Cast<MultiFileBindData>().file_list->Cast<HiveMultiFileList>();
		if (limit_bound) {
			list->SetLimitBound();
		}
	}

public:
	const HiveMultiFileList &List() const {
		return *list;
	}
	const HiveScanInfo &Info() const {
		return list->ScanInfo();
	}
	//! Every file with the values of the partition it is attributed to
	std::map<string, vector<string>> Attributed(const vector<OpenFileInfo> &files) const {
		std::map<string, vector<string>> result;
		for (auto &file : files) {
			result[file.path] = Info().GetPartitionOfFile(file.path).values;
		}
		return result;
	}
	std::map<string, vector<string>> AllAttributed() const {
		return Attributed(list->GetAllFiles());
	}

private:
	unique_ptr<LogicalOperator> plan;
	optional_ptr<HiveMultiFileList> list;
};

//! The message 'read' fails with, empty when it does not
template <class F>
string ErrorOf(F read) {
	try {
		read();
	} catch (std::exception &ex) {
		return ErrorData(ex).RawMessage();
	}
	return string();
}

//! Registers a partition at 'location' through the API, which the SQL functions refuse for some locations. No REQUIRE
//! inside: RunFunctionInTransaction would not see Catch's failure exception and leave the transaction open
void RegisterPartition(Connection &con, const string &catalog_name, const string &database, const string &table,
                       const vector<string> &values, const string &location) {
	bool found = false;
	idx_t refused = 0;
	con.context->RunFunctionInTransaction([&]() {
		auto &catalog = Catalog::GetCatalog(*con.context, Identifier(catalog_name)).Cast<GlueCatalog>();
		GlueTableInfo table_info;
		found = GlueAPI::GetTable(*con.context, catalog, database, table, table_info);
		if (!found) {
			return;
		}
		GluePartitionInput partition;
		partition.values = values;
		partition.location = location;
		refused = GlueAPI::BatchCreatePartitions(*con.context, catalog, table_info, {partition}).size();
	});
	REQUIRE(found);
	REQUIRE(refused == 0);
}

string PartitionLocation(Connection &con, const string &table, const string &value) {
	auto result = Run(con, "SELECT location FROM glue_partitions(" + Quote(table) + ") WHERE k = " + Quote(value));
	auto chunk = result->Fetch();
	REQUIRE(chunk != nullptr);
	REQUIRE(chunk->size() == 1);
	auto location = chunk->GetValue(0, 0).ToString();
	StringUtil::RTrim(location, "/");
	return location;
}

//! A Glue catalog 'paged' through the fault proxy, which pages GetPartitions as AWS does, with a database
//! fault_paged_partitions_<unique> that is dropped when the test ends
class PagedDatabase {
public:
	PagedDatabase(Connection &con_p, const string &location, const string &unique)
	    : con(con_p), name("fault_paged_partitions_" + unique) {
		Run(con, "ATTACH " + Quote(Env("GLUE_CATALOG_ID", "123456789012")) + " AS paged (TYPE glue, ENDPOINT " +
		             Quote(Env("GLUE_FAULT_ENDPOINT", "http://localhost:5556")) + ", DEFAULT_LOCATION " +
		             Quote(location) + ")");
		Run(con, "CREATE SCHEMA paged." + name);
	}
	~PagedDatabase() {
		con.Query("DROP SCHEMA IF EXISTS paged." + name + " CASCADE");
	}

	Connection &con;
	string name;
};

void StartCounting(Connection &con) {
	Run(con, "CALL truncate_duckdb_logs()");
}

void SetUp(Connection &con) {
	// one GetPartitions request per load, so the requests count loads
	Run(con, "SET glue_get_partitions_segments = 1");
	// the listed prefix is in the URL's query string, which HTTP logs redact by default
	Run(con, "SET redact_http_logs = false");
	Run(con, "CALL enable_logging(['HTTP', 'GlueScan'], storage := 'memory')");
	Run(con, "PRAGMA disable_optimizer");
}

//! Every file of the table with its partition's values, listed without paging in a transaction of its own: the
//! partitions it loads stay in the transaction's cache, from which a scan takes them instead of paging
std::map<string, vector<string>> Reference(Connection &con, const string &table) {
	Run(con, "BEGIN");
	std::map<string, vector<string>> result;
	{
		PlannedList reference(con, "SELECT * FROM " + table, false);
		result = reference.AllAttributed();
	}
	Run(con, "COMMIT");
	return result;
}

} // namespace

TEST_CASE("A limit-bound scan lists its first file with one GetPartitions page and one listing",
          "[hive_scan][glue_catalog][lazy_partitions]") {
	auto endpoint = LocalGlueEndpoint();
	if (endpoint.empty()) {
		WARN("skipped: GLUE_ENDPOINT does not name the local Glue (make glue-fixture, make test-cpp)");
		return;
	}
	DuckDB db(nullptr);
	Connection con(db);
	auto unique = Unique();
	auto location = Env("DEFAULT_S3_LOCATION", "s3://glue-test/warehouse") + "/unittest_glue/" + unique;
	AttachLocalGlue(con, endpoint, location);
	PagedDatabase database(con, location, unique);
	auto table = "paged." + database.name + ".t";
	Run(con, "CREATE TABLE " + table + " (id INTEGER, k VARCHAR) PARTITIONED BY (k)");
	// 120 partitions of one file each: pages of 10, 100 and 1000 partitions hold them in three requests
	Run(con, "INSERT INTO " + table + " SELECT i, 'p' || lpad(i::VARCHAR, 3, '0') FROM range(120) t(i)");
	SetUp(con);
	auto expected = Reference(con, table);
	REQUIRE(expected.size() == 120);

	Run(con, "BEGIN");
	SECTION("the first file, as DuckScales' claim asks for it") {
		PlannedList scan(con, "SELECT * FROM " + table, true);
		StartCounting(con);
		auto count = scan.List().GetFileCount(1);
		REQUIRE(count.type == FileExpansionType::NOT_ALL_FILES_KNOWN);
		REQUIRE(count.count >= 1);
		REQUIRE(GetPartitionsRequests(con) == 1);
		REQUIRE(ListRequests(con) == 1);
		REQUIRE(FullLoads(con) == 0);
		REQUIRE(!scan.Info().PartitionsLoaded());
		REQUIRE(scan.Info().KnownPartitions()->size() == 10);
		auto first = scan.List().GetFirstFile();
		REQUIRE(expected.count(first.path) == 1);
		REQUIRE(scan.Info().GetPartitionOfFile(first.path).values == expected[first.path]);

		// more files than the first page holds fetch the second page, still without loading every partition
		count = scan.List().GetFileCount(15);
		REQUIRE(count.count >= 15);
		REQUIRE(GetPartitionsRequests(con) == 2);
		REQUIRE(FullLoads(con) == 0);
		auto known = scan.Info().KnownPartitions();
		REQUIRE(known->size() == 110);

		// every file stops paging: the segmented load lists the rest, and the partitions it loads start with those
		// the pages held, so the files attributed while paging keep their partitions
		auto all = scan.AllAttributed();
		REQUIRE(all == expected);
		REQUIRE(FullLoads(con) == 1);
		auto loaded = scan.Info().Partitions(*con.context);
		REQUIRE(loaded->size() == 120);
		for (idx_t i = 0; i < known->size(); i++) {
			REQUIRE((*loaded)[i].values == (*known)[i].values);
		}
		REQUIRE(scan.List().GetTotalFileCount() == 120);
	}
	SECTION("a limit of at least the table's rows pages through every partition") {
		PlannedList scan(con, "SELECT * FROM " + table, true);
		StartCounting(con);
		// as the scan asks for files one after another
		auto count = scan.List().GetFileCount(1000);
		REQUIRE(count.type == FileExpansionType::ALL_FILES_EXPANDED);
		REQUIRE(count.count == 120);
		// the third page is the last: the pages are the partitions, without a segmented load
		REQUIRE(GetPartitionsRequests(con) == 3);
		REQUIRE(FullLoads(con) == 0);
		REQUIRE(scan.Info().PartitionsLoaded());
		REQUIRE(scan.AllAttributed() == expected);
	}
	SECTION("an unmarked scan loads every partition") {
		PlannedList scan(con, "SELECT * FROM " + table, false);
		StartCounting(con);
		scan.List().GetFileCount(1);
		REQUIRE(FullLoads(con) == 1);
		REQUIRE(scan.Info().PartitionsLoaded());
	}
	Run(con, "COMMIT");
}

TEST_CASE("A limit-bound scan stops paging at a partition with files in a subdirectory",
          "[hive_scan][glue_catalog][lazy_partitions]") {
	auto endpoint = LocalGlueEndpoint();
	if (endpoint.empty()) {
		WARN("skipped: GLUE_ENDPOINT does not name the local Glue (make glue-fixture, make test-cpp)");
		return;
	}
	DuckDB db(nullptr);
	Connection con(db);
	auto unique = Unique();
	auto location = Env("DEFAULT_S3_LOCATION", "s3://glue-test/warehouse") + "/unittest_glue/" + unique;
	AttachLocalGlue(con, endpoint, location);
	PagedDatabase database(con, location, unique);
	auto table = "paged." + database.name + ".t";
	Run(con, "CREATE TABLE " + table + " (id INTEGER, k VARCHAR) PARTITIONED BY (k)");
	Run(con, "INSERT INTO " + table + " VALUES (1, 'a'), (2, 'c')");
	// ten more on the first page, after 'a': the pages are in the order of the values
	Run(con, "INSERT INTO " + table + " SELECT i, 'a' || lpad(i::VARCHAR, 2, '0') FROM range(1, 11) t(i)");
	auto a = PartitionLocation(con, table, "a");
	// 'b' is registered inside 'a', and on the second page: its files are in a subdirectory of 'a', and so is a file
	// of 'a' itself
	Run(con, "COPY (SELECT 3 AS id) TO " + Quote(a + "/nested/b.parquet") + " (FORMAT parquet)");
	Run(con, "COPY (SELECT 4 AS id) TO " + Quote(a + "/deeper/a.parquet") + " (FORMAT parquet)");
	RegisterPartition(con, "paged", database.name, "t", {"b"}, a + "/nested");
	SetUp(con);
	auto expected = Reference(con, table);
	REQUIRE(expected.size() == 14);
	REQUIRE(expected[a + "/nested/b.parquet"] == vector<string> {"b"});
	REQUIRE(expected[a + "/deeper/a.parquet"] == vector<string> {"a"});

	Run(con, "BEGIN");
	PlannedList scan(con, "SELECT * FROM " + table, true);
	StartCounting(con);
	scan.List().GetFileCount(1);
	// 'a' is listed first and has subdirectories: the list stops paging, loads every partition and plans the
	// listings as without paging
	REQUIRE(GetPartitionsRequests(con) == 2);
	REQUIRE(FullLoads(con) == 1);
	REQUIRE(PlannedListings(con) == 1);
	auto first = scan.List().GetFirstFile();
	REQUIRE(scan.Info().GetPartitionOfFile(first.path).values == expected[first.path]);
	REQUIRE(scan.AllAttributed() == expected);
	Run(con, "COMMIT");
}

TEST_CASE("A limit-bound scan refuses partitions it pages through at the same location",
          "[hive_scan][glue_catalog][lazy_partitions]") {
	auto endpoint = LocalGlueEndpoint();
	if (endpoint.empty()) {
		WARN("skipped: GLUE_ENDPOINT does not name the local Glue (make glue-fixture, make test-cpp)");
		return;
	}
	DuckDB db(nullptr);
	Connection con(db);
	auto unique = Unique();
	auto location = Env("DEFAULT_S3_LOCATION", "s3://glue-test/warehouse") + "/unittest_glue/" + unique;
	AttachLocalGlue(con, endpoint, location);
	PagedDatabase database(con, location, unique);
	auto table = "paged." + database.name + ".t";
	Run(con, "CREATE TABLE " + table + " (id INTEGER, k VARCHAR) PARTITIONED BY (k)");
	Run(con, "INSERT INTO " + table + " VALUES (1, 'a')");
	RegisterPartition(con, "paged", database.name, "t", {"b"}, PartitionLocation(con, table, "a"));
	SetUp(con);
	Run(con, "BEGIN");
	string expected;
	{
		PlannedList reference(con, "SELECT * FROM " + table, false);
		expected = ErrorOf([&]() { reference.List().GetAllFiles(); });
	}
	Run(con, "COMMIT");
	REQUIRE(StringUtil::Contains(expected, "are registered at the same location"));

	// both are on the first page: the paging list sees them and refuses as the full listing does
	Run(con, "BEGIN");
	{
		PlannedList scan(con, "SELECT * FROM " + table, true);
		StartCounting(con);
		REQUIRE(ErrorOf([&]() { scan.List().GetFileCount(1); }) == expected);
		REQUIRE(PlannedListings(con) == 0);
	}
	Run(con, "COMMIT");
}
