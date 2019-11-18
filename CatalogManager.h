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

	void SaveFile();//�����ļ� 
	void LoadFile();//�����ļ� 

	bool isTableExist(const string &tableName);//�ж�ĳ���Ƿ���� 
	bool isIndexExist(const string &indexName);//�ж�ĳ�����Ƿ���� 
	bool isAttributeExist(const string &tableName, const string &attrName);//�ж�ĳ�����Ƿ���� 
	bool isAttributeUnique(const string &tableName, const string &attrName);//�ж�ĳ�����Ƿ�Ψһ 
	bool isAttributeIndex(const string &tableName, const string &indexName);//�ж�ĳ�����Ƿ�������� 
	bool isAttributePrimary(const string &tableName, const string &attrName);//�ж�ĳ�����Ƿ�Ϊ���� 
	
	void getAttributeDataType(const string &tableName, const string &attrName, pair<TYPE, int> &res);//�õ�ĳһ���Զ�Ӧ������������ռ�õ��ֽ��� 
	void getTableAttributeDataType(const string &tableName, vector<pair<TYPE, int>> &res);//�õ�ĳ���������Զ�Ӧ������������ռ�õ��ֽ��� 
	void getAIndexInfo(const string &indexName, pair<TYPE, int> &res);//�õ�ĳһ��������Ϣ 
	void getAllIndexInfo(vector< pair<string, pair<TYPE, int>> > &res);//�õ�������������Ϣ 
	void getUniqueAttrPosition(const string &tableName, vector<int> &res);//�õ�ĳ���д���Ψһ�����Ƶ����Ե��±� 
	int getAttributePosition(const string &tableName, const string &attrName);//�õ�ĳ����ĳһ���Ե��±� 
	void getAttributeList(const string &tableName, vector<string> &res);//�õ�ĳһ����������� 
	string getAttributeIndex(const string &tableName, const string attributeName);//�õ�ĳһ���Ե�����
	void getTableIndex(const string &tableName, vector<string> &res);//�õ�ĳһ����������� 
	int getTableTotalRecord(const string &tableName) const;//�õ�ĳһ��ļ�¼�� 
	Table getTable(const string &tableName) const;//�õ�ĳһ��

	//API
	void create_table(const Table &table);		//������ 
	bool create_index(const string &table_name, const string &attribute_name, const string &index_name);//�������� 
	void delete_table(const string &table_name);//ɾ���� 
	void delete_index(const string &index_name);//ɾ������ 
	void update_total_record(const string &table_name, const int num);//����ĳ����ܼ�¼�� 
};

#endif
