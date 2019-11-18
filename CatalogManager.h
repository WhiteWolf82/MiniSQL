#pragma once

#ifndef CATALOG_MANAGER_H
#define CATALOG_MANAGER_H

#include <vector>
#include <string>
#include <set>
#include <fstream>
#include <map>

#include "Structure.h"

using namespace std;

class CatalogManager
{ 
public:
	vector<Table> AllTable;
	
public:
	CatalogManager();
	~CatalogManager();

	void SaveFile();//保存文件 
	void LoadFile();//载入文件 

	bool isTableExist(const string &tableName);//判断某表是否存在 
	bool isIndexExist(const string &indexName);//判断某索引是否存在 
	bool isAttributeExist(const string &tableName, const string &attrName);//判断某属性是否存在 
	bool isAttributeUnique(const string &tableName, const string &attrName);//判断某属性是否唯一 
	bool isAttributeIndex(const string &tableName, const string &indexName);//判断某属性是否存在索引 
	bool isAttributePrimary(const string &tableName, const string &attrName);//判断某属性是否为主码 
	
	void getAttributeDataType(const string &tableName, const string &attrName, pair<TYPE, int> &res);//得到某一属性对应的数据类型与占用的字节数 
	void getTableAttributeDataType(const string &tableName, vector<pair<TYPE, int>> &res);//得到某表所有属性对应的数据类型与占用的字节数 
	void getAIndexInfo(const string &indexName, pair<TYPE, int> &res);//得到某一索引的信息 
	void getAllIndexInfo(vector< pair<string, pair<TYPE, int>> > &res);//得到所有索引的信息 
	void getUniqueAttrPosition(const string &tableName, vector<int> &res);//得到某表中存在唯一性限制的属性的下标 
	int getAttributePosition(const string &tableName, const string &attrName);//得到某表中某一属性的下标 
	void getAttributeList(const string &tableName, vector<string> &res);//得到某一表的属性名表 
	string getAttributeIndex(const string &tableName, const string attributeName);//得到某一属性的索引
	void getTableIndex(const string &tableName, vector<string> &res);//得到某一表的索引名表 
	int getTableTotalRecord(const string &tableName) const;//得到某一表的记录数 
	Table getTable(const string &tableName) const;//得到某一表

	//API
	void create_table(const Table &table);		//创建表 
	bool create_index(const string &table_name, const string &attribute_name, const string &index_name);//创建索引 
	void delete_table(const string &table_name);//删除表 
	void delete_index(const string &index_name);//删除索引 
	void update_total_record(const string &table_name, const int num);//更新某表的总记录数 
};

#endif
