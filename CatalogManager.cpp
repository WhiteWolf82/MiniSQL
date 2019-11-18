#include <algorithm>
#include <iostream>
#include <iterator>
#include <cstring>
#include <fstream>
#include <vector>
#include <string>

#include "CatalogManager.h"

CatalogManager::CatalogManager()
{
	LoadFile();
}

CatalogManager::~CatalogManager()
{
	SaveFile();
}

void CatalogManager::LoadFile()//载入文件 
{
	ifstream ifs("Minisql.catalog");
	if (!ifs.is_open())
    {
        ofstream touch("Minisql.catalog");
        return;
    }
    
    int tablenum;
    ifs >> tablenum;
    
    for (int i = 0; i < tablenum; ++i)
    {
    	Table tb;
    	ifs >> tb.tableName >> tb.totalRecordNum; 
    	
    	int attrnum;
    	ifs >> attrnum;
    	
    	for (int j = 0; j < attrnum; ++j)
    	{
    		Attribute attribute;
    		
    		string attr_name, type_name, index_name;
    		int attr_size, isunique, isindex, isprimary;
    		
			ifs >> attr_name >> type_name >> attr_size >> isunique >> isindex >> isprimary;
			getline(ifs, index_name);
			getline(ifs, index_name);
    		
			attribute.attrName = attr_name;
    		if (type_name == "int")
    		{
    			attribute.type = TYPE::INT;
			}
			else if (type_name == "varchar")
			{
				attribute.type = TYPE::VARCHAR;
			}
			else if (type_name == "float")
			{
				attribute.type = TYPE::FLOAT;
			}
			attribute.size = attr_size;
			attribute.uniqueFlag = isunique != 0;
			attribute.indexFlag = isindex != 0;
			attribute.primaryFlag = isprimary != 0;
			attribute.indexName = index_name;
    		
			tb.attrs.push_back(attribute);
		}
		AllTable.push_back(tb);
	}
}

void CatalogManager::SaveFile()//保存文件 
{
	ofstream ofs("Minisql.catalog");
	int tablenum = AllTable.size();
	ofs << tablenum << endl;
	
	for (int i = 0; i < tablenum; ++i)
	{
		Table tb = AllTable[i];
		ofs << tb.tableName << endl;
		ofs << tb.totalRecordNum << endl;
		
		int attrnum = tb.attrs.size();
    	ofs << attrnum << endl;
    	
    	for (int j = 0; j < attrnum; ++j)
    	{
    		Attribute attribute = tb.attrs[j];
    		
    		ofs << attribute.attrName << endl;
    		switch (attribute.type)
    		{
    			case TYPE::INT:
    				ofs << "int" << endl;
    				break;
    			case TYPE::VARCHAR:
    				ofs << "varchar" << endl;
    				break;
    			case TYPE::FLOAT:
    				ofs << "float" << endl;
    				break;
			}
			ofs << attribute.size << endl;
			ofs << (attribute.uniqueFlag ? 1 : 0) << endl;
			ofs << (attribute.indexFlag ? 1 : 0) << endl;
			ofs << (attribute.primaryFlag ? 1 : 0) << endl;
			ofs << attribute.indexName << endl;
		}
	}
	ofs.close();
}

bool CatalogManager::isTableExist(const string &tableName)//判断某表是否存在 
{
	for (int i = 0; i < AllTable.size(); i++)
		if (AllTable[i].tableName == tableName)
			return true;
	return false;
}

bool CatalogManager::isIndexExist(const string &indexName)//判断某索引是否存在 
{
	for (int i = 0; i < AllTable.size(); i++)
		for (int j = 0; j < AllTable[i].attrs.size(); j++)
			if (AllTable[i].attrs[j].indexName == indexName)
				return true;
	return false;
}

bool CatalogManager::isAttributeExist(const string &tableName, const string &attrName)//判断某属性是否存在 
{
	for (int i = 0; i < AllTable.size(); i++)
		if (AllTable[i].tableName == tableName)
			for (int j = 0; j < AllTable[i].attrs.size(); j++)
				if (AllTable[i].attrs[j].attrName == attrName)
					return true;
	return false;
}

bool CatalogManager::isAttributeUnique(const string &tableName, const string &attrName)//判断某属性是否唯一 
{
	for (int i = 0; i < AllTable.size(); i++)
		if (AllTable[i].tableName == tableName)
			for (int j = 0; j < AllTable[i].attrs.size(); j++)
				if (AllTable[i].attrs[j].uniqueFlag)
					return true;
	return false;
}

bool CatalogManager::isAttributeIndex(const string &tableName, const string &indexName)//判断某属性是否存在索引 
{
	for (int i = 0; i < AllTable.size(); i++)
		if (AllTable[i].tableName == tableName)
			for (int j = 0; j < AllTable[i].attrs.size(); j++)
				if (AllTable[i].attrs[j].indexFlag)
					return true;
	return false;
}

bool CatalogManager::isAttributePrimary(const string &tableName, const string &attrName)//判断某属性是否为主码 
{
	for (int i = 0; i < AllTable.size(); i++)
		if (AllTable[i].tableName == tableName)
			for (int j = 0; j < AllTable[i].attrs.size(); j++)
				if (AllTable[i].attrs[j].primaryFlag)
					return true;
	return false;
}

void CatalogManager::getAttributeDataType(const string &tableName, const string &attrName, pair<TYPE, int> &res)//得到某一属性对应的数据类型与占用的字节数 
{
	Table table;
	for (int i = 0; i < AllTable.size(); i++)
		if (AllTable[i].tableName == tableName)
		{
			table = AllTable[i];
			break;
		}
	for (int i = 0; i < table.attrs.size(); i++)
		if (table.attrs[i].attrName == attrName)
		{
			res = make_pair(table.attrs[i].type, table.attrs[i].size);
			break;
		}
} 

void CatalogManager::getTableAttributeDataType(const string &tableName, vector<pair<TYPE, int>> &res)//得到某表所有属性对应的数据类型与占用的字节数 
{
	Table table;
	for (int i = 0; i < AllTable.size(); i++)
		if (AllTable[i].tableName == tableName)
		{
			table = AllTable[i];
			break;
		}
	for (int i = 0; i < table.attrs.size(); i++)
		res.push_back(make_pair(table.attrs[i].type, table.attrs[i].size));
} 

void CatalogManager::getAIndexInfo(const string &indexName, pair<TYPE, int> &res)//得到某一索引的信息 
{
	for (int i = 0; i < AllTable.size(); i++)
		for (int j = 0; j < AllTable[i].attrs.size(); j++)
			if (AllTable[i].attrs[j].indexName == indexName)
			{
				res = make_pair(AllTable[i].attrs[j].type, AllTable[i].attrs[j].size);
				break;
			}
}

void CatalogManager::getAllIndexInfo(vector< pair<string, pair<TYPE, int>> > &res)//得到所有索引的信息 
{
	for (int i = 0; i < AllTable.size(); i++)
		for (int j = 0; j < AllTable[i].attrs.size(); j++)
			if (AllTable[i].attrs[j].indexFlag)
				res.push_back(make_pair(AllTable[i].attrs[j].indexName, make_pair(AllTable[i].attrs[j].type, AllTable[i].attrs[j].size)));
}

void CatalogManager::getUniqueAttrPosition(const string &tableName, vector<int> &res)//得到某表中存在唯一性限制的属性的下标 
{
	Table table;
	for (int i = 0; i < AllTable.size(); i++)
		if (AllTable[i].tableName == tableName)
		{
			table = AllTable[i];
			break;
		}
	for (int i = 0; i < table.attrs.size(); i++)
		if (table.attrs[i].uniqueFlag) 
			res.push_back(i);
}

int CatalogManager::getAttributePosition(const string &tableName, const string &attrName)//得到某表中某一属性的下标 
{
	for (int i = 0; i < AllTable.size(); i++)
		if (AllTable[i].tableName == tableName)
			for (int j = 0; j < AllTable[i].attrs.size(); j++)
				if (AllTable[i].attrs[j].attrName == attrName)
					return j;
}

void CatalogManager::getAttributeList(const string &tableName, vector<string> &res)//得到某一表的属性名表 
{
	Table table;
	for (int i = 0; i < AllTable.size(); i++)
		if (AllTable[i].tableName == tableName)
		{
			table = AllTable[i];
			break;
		}
	for (int i = 0; i < table.attrs.size(); i++)
		res.push_back(table.attrs[i].attrName); 
}

string CatalogManager::getAttributeIndex(const string &tableName, const string attributeName)//得到某一属性的索引
{
	Table table;
	for (int i = 0; i < AllTable.size(); i++)
		if (AllTable[i].tableName == tableName)
		{
			table = AllTable[i];
			break;
		}
	for (auto attr : table.attrs)
	{
		if (attr.attrName == attributeName)
		{
			return attr.indexName;
		}
	}
}

void CatalogManager::getTableIndex(const string &tableName, vector<string> &res)//得到某一表的索引名表
{
	Table table;
	for (int i = 0; i < AllTable.size(); i++)
		if (AllTable[i].tableName == tableName)
		{
			table = AllTable[i];
			break;
		}
	for (int i = 0; i < table.attrs.size(); i++)
		if (table.attrs[i].indexFlag) 
		{
			res.push_back(table.attrs[i].indexName); 
		}
} 

int CatalogManager::getTableTotalRecord(const string &tableName) const//得到某一表的记录数 
{
	for (int i = 0; i < AllTable.size(); i++)
		if (AllTable[i].tableName == tableName)
			return AllTable[i].totalRecordNum;
}

Table CatalogManager::getTable(const string &tableName) const //得到某一表
{
	for (auto t : AllTable)
	{
		if (t.tableName == tableName)
			return t;
	}
}

void CatalogManager::create_table(const Table &table)//创建表 
{
	for (auto t : AllTable)
	{
		if (t.tableName == table.tableName)
		{
			cerr << "This table already exists!!!" << endl;
			return;
		}
	}
	AllTable.push_back(table);
	SaveFile();
}

bool CatalogManager::create_index(const string &table_name, const string &attribute_name, const string &index_name)//创建索引 
{
	for (int i = 0; i < AllTable.size(); i++)
	{
		if (AllTable[i].tableName == table_name)
		{
			for (int j = 0; j < AllTable[i].attrs.size(); j++)
			{
				if (AllTable[i].attrs[j].attrName == attribute_name)
				{
					if (AllTable[i].attrs[j].uniqueFlag == true)
					{
						AllTable[i].attrs[j].indexFlag = true;
						AllTable[i].attrs[j].indexName = index_name;
						SaveFile();
						return true;
					}
					else
					{
						return false;
					}
				}
			}
		}
	}
}

void CatalogManager::delete_table(const string &table_name)//删除表 
{
	for (vector<Table>::iterator it = AllTable.begin(); it != AllTable.end(); it++)
		if (it->tableName == table_name)
		{
			AllTable.erase(it);
			SaveFile();
			return;
		}
}

void CatalogManager::delete_index(const string &index_name)//删除索引 
{
	for (int i = 0; i < AllTable.size(); i++)
		for (int j = 0; j < AllTable[i].attrs.size(); j++)
			if (AllTable[i].attrs[j].indexName == index_name)
			{
				AllTable[i].attrs[j].indexFlag = false;
				AllTable[i].attrs[j].indexName = "";
				SaveFile();
				return;
			}
}

void CatalogManager::update_total_record(const string &table_name, const int num)//更新某表的总记录数 
{
	for (int i = 0; i < AllTable.size(); i++)
		if (AllTable[i].tableName == table_name)
		{
			AllTable[i].totalRecordNum += num;
			SaveFile();
			return;
		}
}
