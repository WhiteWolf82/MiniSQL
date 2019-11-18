#ifndef RECORDMANAGER_H
#define RECORDMANAGER_H

#include<iostream>
#include<vector>
#include<cstring>
#include<fstream>
#include<stdlib.h>
#include"BufferManager.h"
#include"Structure.h"
#include"IndexManager.h"
#include"CatalogManager.h"

using namespace std;

class RecordManager
{
public:
	RecordManager(BufferManager& bm, IndexManager& im, int blockSize) : bm(bm), im(im), blockSize(blockSize) {}
	void createTable(string tableName);
	void dropTable(string tableName);
	void createIndex(Table& table, int attrIndex);
	void dropIndex(Table& table, int attrIndex);
	int getRecordLen(Table& table);
	int insertRecord(Table& table, vector<SqlValue>& values);		//return the new index
	int deleteRecord(Table& table, vector<int>& indices);	//return number of rows affected
	int selectRecord(Table& table, vector<int>& indices, vector<int>& attrIndices, vector<char*>& result);	//return number of rows affected
	int updateRecord(Table& table, vector<int>& indices, vector<int>& attrIndices, vector<SqlValue>& values);	//return number of rows affected
	void findIndex(Table& table, vector<int>& indices, Condition& con);
	void findAllIndex(Table& table, vector<int>& indices);
	void bpFindIndex(Table& table, vector<int>& indices, Condition& con);
	int getAttrIndex(Table& table, string attrName);

private:
	BufferManager& bm;
	IndexManager& im;
	int blockSize;
	bool isUnique(Table& table, char* record, int recordIndexInFile);
	void swapRecord(Table& table, int index);	//swap the record to be deleted with the last record of file
};

#endif // !RECORDMANAGER_H