#include "API.h"
#include "RecordManager.h"
#include "CatalogManager.h"
#include "IndexManager.h"

using namespace std;

API::API()
{
	im = new IndexManager();
	bm = new BufferManager();
	cm = new CatalogManager();
	rm = new RecordManager(*bm, *im, 4096);
}

//可能存在bug
API::~API()
{
	delete im;
	delete bm;
	delete cm;
	delete rm;
}

int API::insert(const string &table_name, vector<SqlValue> &value_list)
{
	if (!cm->isTableExist(table_name))
	{
		cout << "Table not found!" << endl;
		return 0;
	}

	vector<pair<TYPE, int>> res;
	cm->getTableAttributeDataType(table_name, res);
	if (res.size() != value_list.size())
	{
		cout << "Number of values not fit!" << endl;
		return 0;
	}

	for (int i = 0; i < value_list.size(); ++i)
	{
		if (value_list[i].type != res[i].first || value_list[i].size > res[i].second)
		{
			cout << "Type mismatch!" << endl;
		}
	}

	Table t = cm->getTable(table_name);
	int num = rm->insertRecord(t, value_list);
	cm->update_total_record(table_name, 1);

	return num;
}

//只考虑condition_list只有一个元素
int API::delete_op(const string &table_name, const vector<Condition> &condition_list, string s)
{
	if (!cm->isTableExist(table_name))
	{
		cerr << "Table not found!" << endl;
		return -1;
	}
	else
	{
		if (condition_list.size() == 2)
		{
			Condition c1 = condition_list[0];
			Condition c2 = condition_list[1];
			vector<int> indices, indices1, indices2;
			Table t = cm->getTable(table_name);
			if (cm->getAttributeIndex(table_name, c1.attributeName) == "")
			{
				rm->findIndex(t, indices1, c1);
			}
			else
			{
				rm->bpFindIndex(t, indices1, c1);
			}
			if (cm->getAttributeIndex(table_name, c2.attributeName) == "")
			{
				rm->findIndex(t, indices2, c2);
			}
			else
			{
				rm->bpFindIndex(t, indices2, c2);
			}
			sort(indices1.begin(), indices1.end());
			sort(indices2.begin(), indices2.end());
			if (s == "AND")
			{
				set_intersection(indices1.begin(), indices1.end(), indices2.begin(), indices2.end(), inserter(indices, indices.begin()));
			}
			else if (s == "OR")
			{
				set_union(indices1.begin(), indices1.end(), indices2.begin(), indices2.end(), inserter(indices, indices.begin()));
			}
			int affectNum = rm->deleteRecord(t, indices);
			cm->update_total_record(table_name, -affectNum);
			return affectNum;
		}
		else if (condition_list.size() == 1)
		{
			Condition c = condition_list[0];
			vector<int> indices;
			Table t = cm->getTable(table_name);
			if (cm->getAttributeIndex(table_name, c.attributeName) == "")
			{
				rm->findIndex(t, indices, c);
			}
			else
			{
				rm->bpFindIndex(t, indices, c);
			}
			int affectNum = rm->deleteRecord(t, indices);
			cm->update_total_record(table_name, -affectNum);
			return affectNum;
		}
		else if (condition_list.size() == 0)
		{
			vector<int> indices;
			Table t = cm->getTable(table_name);
			rm->findAllIndex(t, indices);
			int affectNum = rm->deleteRecord(t, indices);
			cm->update_total_record(table_name, -affectNum);
			return affectNum;
		}
	}
}

int API::select(const string &table_name, const vector<Condition> &condition_list, vector<char*> &result, string s)
{
	if (!cm->isTableExist(table_name))
	{
		cerr << "Table not found!" << endl;
		return -1;
	}
	else
	{
		Table t = cm->getTable(table_name);
		vector<string> attr_list;
		for (auto attr : t.attrs)
		{
			attr_list.push_back(attr.attrName);
		}
		return select(table_name, condition_list, attr_list, result, s);
	}
}

int API::select(const string &table_name,
	const vector<Condition> &condition_list,
	const vector<string> &attr_list,
	vector<char*> &result,
	string s)
{
	if (!cm->isTableExist(table_name))
	{
		cerr << "Table not found!" << endl;
		return -1;
	}
	else
	{
		if (condition_list.size() == 2)
		{
			Condition c1 = condition_list[0];
			Condition c2 = condition_list[1];
			vector<int> indices, indices1, indices2;
			Table t = cm->getTable(table_name);
			if (cm->getAttributeIndex(table_name, c1.attributeName) == "")
			{
				rm->findIndex(t, indices1, c1);
			}
			else
			{
				rm->bpFindIndex(t, indices1, c1);
			}
			if (cm->getAttributeIndex(table_name, c2.attributeName) == "")
			{
				rm->findIndex(t, indices2, c2);
			}
			else
			{
				rm->bpFindIndex(t, indices2, c2);
			}
			sort(indices1.begin(), indices1.end());
			sort(indices2.begin(), indices2.end());
			if (s == "AND")
			{
				set_intersection(indices1.begin(), indices1.end(), indices2.begin(), indices2.end(), inserter(indices, indices.begin()));
			}
			else if (s == "OR")
			{
				set_union(indices1.begin(), indices1.end(), indices2.begin(), indices2.end(), inserter(indices, indices.begin()));
			}
			vector<int> attrIndices;
			for (auto attr : attr_list)
			{
				attrIndices.push_back(rm->getAttrIndex(t, attr));
			}
			int affectNum = rm->selectRecord(t, indices, attrIndices, result);
			return affectNum;
		}
		else if (condition_list.size() == 1)
		{
			Condition c = condition_list[0];
			vector<int> indices;
			Table t = cm->getTable(table_name);
			if (cm->getAttributeIndex(table_name, c.attributeName) == "")
			{
				rm->findIndex(t, indices, c);
			}
			else
			{
				rm->bpFindIndex(t, indices, c);
			}
			vector<int> attrIndices;
			for (auto attr : attr_list)
			{
				attrIndices.push_back(rm->getAttrIndex(t, attr));
			}
			int affectNum = rm->selectRecord(t, indices, attrIndices, result);
			return affectNum;
		}
		else
		{
			vector<int> indices;
			Table t = cm->getTable(table_name);
			rm->findAllIndex(t, indices);
			vector<int> attrIndices;
			for (auto attr : attr_list)
			{
				attrIndices.push_back(rm->getAttrIndex(t, attr));
			}
			int affectNum = rm->selectRecord(t, indices, attrIndices, result);
			return affectNum;
		}
	}
}

bool API::update(const string &table_name,
	const string &attr,
	const SqlValue &value,
	const vector<Condition> &condition_list)
{
	//TODO
	cerr << "Not supported to update." << endl;
	return false;
}

//小问题
bool API::create_table(const string &table_name, const vector<Attribute> &attr_list)
{
	Table t(table_name, attr_list);
	cm->create_table(t);
	rm->createTable(table_name);
	return true;
}

bool API::create_index(const string &table_name, const string &attribute_name, const string &index_name)
{
	bool b = cm->create_index(table_name, attribute_name, index_name);
	if (b)
	{
		Table t = cm->getTable(table_name);
		rm->createIndex(t, rm->getAttrIndex(t, attribute_name));
	}
	return b;
}

bool API::drop_table(const string &table_name)
{
	if (!cm->isTableExist(table_name))
	{
		cerr << "Table not found!" << endl;
		return false;
	}

	/*
	删除索引
	auto &tb = cm->GetTable(table_name);

	for (auto &it : tb.index)
	{
		auto ifn = it.first;
		rm->dropIndex(tb, ifn);
	}
	*/
	else
	{
		cm->delete_table(table_name);
		rm->dropTable(table_name);
		cout << "Table " << table_name << " dropped." << endl;
		return true;
	}
}

bool API::drop_index(const string &index_name)
{
	if (!cm->isIndexExist(index_name))
	{
		cerr << "Index not found!" << endl;
		return false;
	}
	else
	{
		cm->delete_index(index_name);
		cout << "Index " << index_name << " dropped." << endl;
		return true;
	}
}