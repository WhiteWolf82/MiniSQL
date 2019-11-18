#ifndef API_H
#define API_H

#include <string>
#include <algorithm>
#include <iterator>
#include "RecordManager.h"
#include "IndexManager.h"
#include "BufferManager.h"
#include "CatalogManager.h"

using namespace std;

class API{
public:
	RecordManager *rm;
    IndexManager *im;
    BufferManager *bm;
    CatalogManager *cm;

	API();
	~API();

	int insert(const std::string &table_name, std::vector<SqlValue> &value_list);

    int delete_op(const std::string &table_name, const std::vector<Condition> &condition_list, string s);

	int select(const std::string &table_name, const std::vector<Condition> &condition_list, vector<char*> &result, string s);

	int select(const std::string &table_name,
		const std::vector<Condition> &condition_list,
		const std::vector<std::string> &attr_list,
		vector<char*> &result,
		string s);

    bool update(const std::string &table_name,
                const std::string &attr,
                const SqlValue &value,
                const std::vector<Condition> &condition_list);

    bool create_table(const std::string &table_name, const std::vector<Attribute> &attr_list);

    bool create_index(const std::string &table_name, const std::string &attribute_name, const std::string &index_name);

    bool drop_table(const std::string &table_name);

    bool drop_index(const std::string &index_name);

};
#endif