#include"RecordManager.h"

void RecordManager::createTable(string tableName)
{
	string fileName = tableName + ".MYD";
	bm.createFile(fileName);
}

void RecordManager::dropTable(string tableName)
{
	string fileName = tableName + ".MYD";
	bm.deleteFile(fileName);
}

int RecordManager::getRecordLen(Table& table)
{
	int len = 0;
	for (int i = 0; i < table.attrs.size(); i++)
		len += table.attrs[i].size;
	return len;
}

bool RecordManager::isUnique(Table& table, char* record, int recordIndexInFile)
{
	vector<Attribute>& attrs = table.attrs;
	vector<int> uniqueAttrs;

	for (int i = 0; i < attrs.size(); i++)
	{
		if (attrs[i].uniqueFlag == true || attrs[i].primaryFlag == true)
			uniqueAttrs.push_back(i);
	}

	string fileName = table.tableName + ".MYD";
	int fileSize = bm.getFileSize(fileName);
	int recordLen = getRecordLen(table);
	int blockNum = fileSize / blockSize;
	int recordNum = blockSize / recordLen;	//maximum number of records in one block

	for (int i = 0; i < blockNum - 1; i++)
	{
		char* blockData = new char[blockSize];
		bm.readFileData(fileName, i, blockData);
		for (int j = 0; j < recordNum; j++)
		{
			if (i * recordNum + j == recordIndexInFile)		//it's itself
				continue;
			char* recordData = new char[recordLen];
			memcpy(recordData, blockData + j * recordLen, recordLen);
			for (int k = 0; k < uniqueAttrs.size(); k++)
			{
				int attrIndex = uniqueAttrs[k];
				int sizeOffset = 0;
				for (int l = 0; l < attrIndex; l++)
					sizeOffset += attrs[l].size;

				if (attrs[attrIndex].type == TYPE::INT)
				{
					int *thisInt, *thatInt;
					thisInt = reinterpret_cast<int*>(record + sizeOffset);
					thatInt = reinterpret_cast<int*>(recordData + sizeOffset);
					if (*thisInt == *thatInt)
						return false;
				}
				else if (attrs[attrIndex].type == TYPE::FLOAT)
				{
					float *thisFloat, *thatFloat;
					thisFloat = reinterpret_cast<float*>(record + sizeOffset);
					thatFloat = reinterpret_cast<float*>(recordData + sizeOffset);
					if (*thisFloat == *thatFloat)
						return false;
				}
				else if (attrs[attrIndex].type == TYPE::VARCHAR)
				{
					char* thisChar = new char[attrs[attrIndex].size];
					char* thatChar = new char[attrs[attrIndex].size];
					memcpy(thisChar, recordData + sizeOffset, attrs[attrIndex].size);
					memcpy(thatChar, record + sizeOffset, attrs[attrIndex].size);
					if (strcmp(thisChar, thatChar) == 0)
						return false;
					delete[] thisChar;
					delete[] thatChar;
				}
			}
			delete[] recordData;
		}
		delete[] blockData;
	}

	char* blockData = new char[blockSize];
	bm.readFileData(fileName, blockNum - 1, blockData);
	int recordNumInLastBlock = table.totalRecordNum % recordNum;
	for (int i = 0; i < recordNumInLastBlock; i++)
	{
		if ((blockNum - 1) * recordNum + i == recordIndexInFile)
			continue;
		char* recordData = new char[recordLen];
		memcpy(recordData, blockData + i * recordLen, recordLen);
		for (int k = 0; k < uniqueAttrs.size(); k++)
		{
			int attrIndex = uniqueAttrs[k];
			int sizeOffset = 0;
			for (int l = 0; l < attrIndex; l++)
				sizeOffset += attrs[l].size;

			if (attrs[attrIndex].type == TYPE::INT)
			{
				int *thisInt, *thatInt;
				thisInt = reinterpret_cast<int*>(record + sizeOffset);
				thatInt = reinterpret_cast<int*>(recordData + sizeOffset);
				if (*thisInt == *thatInt)
					return false;
			}
			else if (attrs[attrIndex].type == TYPE::FLOAT)
			{
				float *thisFloat, *thatFloat;
				thisFloat = reinterpret_cast<float*>(record + sizeOffset);
				thatFloat = reinterpret_cast<float*>(recordData + sizeOffset);
				if (*thisFloat == *thatFloat)
					return false;
			}
			else if (attrs[attrIndex].type == TYPE::VARCHAR)
			{
				char* thisChar = new char[attrs[attrIndex].size];
				char* thatChar = new char[attrs[attrIndex].size];
				memcpy(thisChar, recordData + sizeOffset, attrs[attrIndex].size);
				memcpy(thatChar, record + sizeOffset, attrs[attrIndex].size);
				if (strcmp(thisChar, thatChar) == 0)
					return false;
				delete[] thisChar;
				delete[] thatChar;
			}
		}
		delete[] recordData;
	}
	delete[] blockData;

	return true;
}

int RecordManager::insertRecord(Table& table, vector<SqlValue>& values)
{
	string fileName = table.tableName + ".MYD";
	int fileSize = bm.getFileSize(fileName);
	int recordLen = getRecordLen(table);
	int blockNum = fileSize / blockSize;
	int recordNum = blockSize / recordLen;	//maximum number of records in one block

	char* record = new char[recordLen];
	int curSize = 0;
	for (int i = 0; i < values.size(); i++)
	{
		if (values[i].type == TYPE::INT)
		{
			int value_int = values[i].i;
			char* value1 = new char[sizeof(int)];
			value1 = reinterpret_cast<char*>(&value_int);
			memcpy(record + curSize, value1, sizeof(int));
			curSize += sizeof(int);
			//delete[] value1;
		}
		else if (values[i].type == TYPE::FLOAT)
		{
			float value_float = values[i].r;
			char* value2 = new char[sizeof(float)];
			value2 = reinterpret_cast<char*>(&value_float);
			memcpy(record + curSize, value2, sizeof(float));
			curSize += sizeof(float);
			//delete[] value2;
		}
		else if (values[i].type == TYPE::VARCHAR)
		{
			const char* value3 = new char[table.attrs[i].size];
			value3 = values[i].str.c_str();
			memcpy(record + curSize, value3, table.attrs[i].size);
			curSize += table.attrs[i].size;
			//delete[] value3;
		}
	}

	if (isUnique(table, record, -1) == false)	//against integrity constraint
		return -1;

	if (table.totalRecordNum % recordNum == 0)	//all blocks are full
	{
		char* blockData = new char[blockSize];
		memcpy(blockData, record, recordLen);
		bm.writeFileData(fileName, blockNum, blockData);	//use blockNum to create a new block
		//delete[] blockData;
	}
	else
	{
		char* blockData = new char[blockSize];
		bm.readFileData(fileName, blockNum - 1, blockData);		//read last block
		/*it's impossible to cross blocks*/
		memcpy(blockData + (table.totalRecordNum % recordNum) * recordLen, record, recordLen);
		bm.writeFileData(fileName, blockNum - 1, blockData);
		//delete[] blockData;
	}
	table.totalRecordNum++;

	bool hasIndex = false;
	int index;
	for (int i = 0; i < table.attrs.size(); i++)
	{
		if (table.attrs[i].indexFlag == true)
		{
			hasIndex = true;
			index = i;
			break;
		}
	}
	if (hasIndex == true)	//the table has an index
	{
		string indexFileName = table.tableName + "_" + table.attrs[index].attrName;
		if (table.attrs[index].type == TYPE::INT)
		{
			int value_int = values[index].i;
			SqlValue val_int(value_int);
			im.insert(indexFileName, val_int, table.totalRecordNum - 1);
		}
		else if (table.attrs[index].type == TYPE::FLOAT)
		{
			float value_float = values[index].r;
			SqlValue val_float((float)value_float);
			im.insert(indexFileName, val_float, table.totalRecordNum - 1);
		}
		else if (table.attrs[index].type == TYPE::VARCHAR)
		{
			string value_str = values[index].str;
			SqlValue val_str(value_str);
			im.insert(indexFileName, val_str, table.totalRecordNum - 1);
		}
	}

	return (table.totalRecordNum - 1);
}

void RecordManager::swapRecord(Table& table, int index)
{
	string fileName = table.tableName + ".MYD";
	int fileSize = bm.getFileSize(fileName);
	int recordLen = getRecordLen(table);
	int blockNum = fileSize / blockSize;
	int recordNum = blockSize / recordLen;	//maximum number of records in one block

	int blockIndex = index / recordNum;		//index of block where this record is in
	int recordIndex = index % recordNum;	//index of this record in block

	char* lastBlock = new char[blockSize];
	bm.readFileData(fileName, blockNum - 1, lastBlock);

	int lastBlockIndex = table.totalRecordNum % recordNum - 1;		//index of last record in block
	if (lastBlockIndex < 0)
		lastBlockIndex += recordNum;
	char* lastRecord = new char[recordLen];
	memcpy(lastRecord, lastBlock + lastBlockIndex * recordLen, recordLen);

	char* thisBlock = new char[blockSize];
	bm.readFileData(fileName, blockIndex, thisBlock);
	memcpy(thisBlock + recordIndex * recordLen, lastRecord, recordLen);

	bm.writeFileData(fileName, blockIndex, thisBlock);		//overwrite this record with last record
	table.totalRecordNum--;		//last record cannot be visited now, which will be deleted later

	delete[] lastBlock;
	delete[] lastRecord;
}

int RecordManager::deleteRecord(Table& table, vector<int>& indices)
{
	//assume that indices are all correct, so no extra tests here
	string fileName = table.tableName + ".MYD";
	int recordLen = getRecordLen(table);
	int recordNum = blockSize / recordLen;	//maximum number of records in one block
	int recordNumInLastBlock = table.totalRecordNum % recordNum;	//number of records in last block
	if (recordNumInLastBlock == 0)
		recordNumInLastBlock = recordNum;

	int blockIndex;		//index of block where this record is in
	int recordIndexInFile;	//index of this record in file
	int recordIndexInBlock;	//index of this record in block
	int blockNum;
	int fileSize;

	bool hasIndex = false;
	int index;
	int sizeOffset = 0;
	string indexFileName;
	for (int i = 0; i < table.attrs.size(); i++)
	{
		if (table.attrs[i].indexFlag == true)
		{
			hasIndex = true;
			index = i;
			for (int j = 0; j < i; j++)
				sizeOffset += table.attrs[j].size;
			indexFileName = table.tableName + "_" + table.attrs[index].attrName;
			break;
		}
	}

	for (int i = 0; i < indices.size(); i++)
	{
		if (hasIndex == true)
		{
			blockIndex = indices[i] / recordNum;
			recordIndexInFile = indices[i];
			recordIndexInBlock = indices[i] % recordNum;
			fileSize = bm.getFileSize(fileName);
			blockNum = fileSize / blockSize;

			char* lastBlock = new char[blockSize];
			char* lastRecord = new char[recordLen];
			bm.readFileData(fileName, blockNum - 1, lastBlock);
			memcpy(lastRecord, lastBlock + (recordNumInLastBlock - 1) * recordLen, recordLen);

			char* readBuffer = new char[blockSize];
			bm.readFileData(fileName, blockIndex, readBuffer);

			char* thisRecord = new char[recordLen];
			memcpy(thisRecord, readBuffer + recordIndexInBlock * recordLen, recordLen);

			if (table.attrs[index].type == TYPE::INT)
			{
				char* value1 = new char[sizeof(int)];
				char* value11 = new char[sizeof(int)];
				memcpy(value1, thisRecord + sizeOffset, sizeof(int));
				memcpy(value11, lastRecord + sizeOffset, sizeof(int));
				int* value_int1;
				int* value_int2;
				value_int1 = reinterpret_cast<int*>(value1);	//the one to be deleted
				value_int2 = reinterpret_cast<int*>(value11);	//the last one
				SqlValue val_int1(*value_int1);
				SqlValue val_int2(*value_int2);
				im.removeKey(indexFileName, val_int1, val_int2);
			}
			else if (table.attrs[index].type == TYPE::FLOAT)
			{
				char* value2 = new char[sizeof(float)];
				char* value22 = new char[sizeof(float)];
				memcpy(value2, thisRecord + sizeOffset, sizeof(float));
				memcpy(value22, lastRecord + sizeOffset, sizeof(float));
				float* value_float1;
				float* value_float2;
				value_float1 = reinterpret_cast<float*>(value2);
				value_float2 = reinterpret_cast<float*>(value22);
				SqlValue val_float1((float)*value_float1);
				SqlValue val_float2((float)*value_float2);
				im.removeKey(indexFileName, val_float1, val_float2);
			}
			else if (table.attrs[index].type == TYPE::VARCHAR)
			{
				char* value3 = new char[table.attrs[index].size];
				char* value33 = new char[table.attrs[index].size];
				memcpy(value3, thisRecord + sizeOffset, table.attrs[index].size);
				memcpy(value33, lastRecord + sizeOffset, table.attrs[index].size);
				string value_str1 = value3;
				string value_str2 = value33;
				SqlValue val_str1(value_str1);
				SqlValue val_str2(value_str2);
				im.removeKey(indexFileName, val_str1, val_str2);
			}
			delete[] readBuffer;
			delete[] thisRecord;
			delete[] lastBlock;
			delete[] lastRecord;
		}

		swapRecord(table, indices[i]);
		recordNumInLastBlock--;
		if (recordNumInLastBlock == 0)
		{
			bm.deleteLastFileBlock(fileName);
			recordNumInLastBlock = recordNum;	//move to the previous block
		}
	}

	return indices.size();
}

int RecordManager::selectRecord(Table& table, vector<int>& indices, vector<int>& attrIndices, vector<char*>& result)
{
	string fileName = table.tableName + ".MYD";
	int fileSize = bm.getFileSize(fileName);
	int recordLen = getRecordLen(table);
	int blockNum = fileSize / blockSize;
	int recordNum = blockSize / recordLen;	//maximum number of records in one block

	int blockIndex;		//index of block where this record is in
	int recordIndexInFile;	//index of this record in file
	int recordIndexInBlock;	//index of this record in block

	int recordResultLen = 0;
	for (int i = 0; i < attrIndices.size(); i++)
	{
		recordResultLen += table.attrs[attrIndices[i]].size;
	}

	for (int i = 0; i < indices.size(); i++)
	{
		blockIndex = indices[i] / recordNum;
		recordIndexInFile = indices[i];
		recordIndexInBlock = indices[i] % recordNum;

		char* readBuffer = new char[blockSize];
		bm.readFileData(fileName, blockIndex, readBuffer);

		char* thisRecord = new char[recordLen];
		memcpy(thisRecord, readBuffer + recordIndexInBlock * recordLen, recordLen);

		char* recordResult = new char[recordResultLen];
		int sizeOffset;
		int nowSize = 0;	//current size of recordResult
		for (int j = 0; j < attrIndices.size(); j++)
		{
			sizeOffset = 0;
			for (int k = 0; k < attrIndices[j]; k++)
				sizeOffset += table.attrs[k].size;
			memcpy(recordResult + nowSize, thisRecord + sizeOffset, table.attrs[attrIndices[j]].size);
			nowSize += table.attrs[attrIndices[j]].size;
		}

		result.push_back(recordResult);
		//delete[] readBuffer;
		//delete[] thisRecord;
		//delete[] recordResult;
	}

	//printf("%s\n", result.back());
	return indices.size();
}

int RecordManager::getAttrIndex(Table& table, string attrName)
{
	for (int i = 0; i < table.attrs.size(); i++)
	{
		if (table.attrs[i].attrName == attrName)
			return i;
	}
	return -1;
}

int RecordManager::updateRecord(Table& table, vector<int>& indices, vector<int>& attrIndices, vector<SqlValue>& values)
{
	int flag = 0;	//whether against integrity constraint

	string fileName = table.tableName + ".MYD";
	int fileSize = bm.getFileSize(fileName);
	int recordLen = getRecordLen(table);
	int blockNum = fileSize / blockSize;
	int recordNum = blockSize / recordLen;	//maximum number of records in one block

	int blockIndex;		//index of block where this record is in
	int recordIndexInFile;	//index of this record in file
	int recordIndexInBlock;	//index of this record in block

	char* firstRecord = new char[recordLen];
	char* firstReadBuffer = new char[blockSize];
	int firstRecordIndexInBlock;

	for (int i = 0; i < indices.size(); i++)
	{
		blockIndex = indices[i] / recordNum;
		recordIndexInFile = indices[i];
		recordIndexInBlock = indices[i] % recordNum;

		char* readBuffer = new char[blockSize];
		bm.readFileData(fileName, blockIndex, readBuffer);

		char* thisRecord = new char[recordLen];
		memcpy(thisRecord, readBuffer + recordIndexInBlock * recordLen, recordLen);
		if (i == 0)
		{
			memcpy(firstRecord, thisRecord, recordLen);
			memcpy(firstReadBuffer, readBuffer, blockSize);
			firstRecordIndexInBlock = recordIndexInBlock;
		}

		int sizeOffset;
		for (int j = 0; j < attrIndices.size(); j++)
		{
			sizeOffset = 0;
			for (int k = 0; k < attrIndices[j]; k++)
				sizeOffset += table.attrs[k].size;
			if (values[j].type == TYPE::INT)
			{
				int value_int = values[j].i;
				char* value = new char[sizeof(int)];
				value = reinterpret_cast<char*>(&value_int);
				memcpy(thisRecord + sizeOffset, value, sizeof(int));
				//delete[] value;
			}
			else if (values[j].type == TYPE::FLOAT)
			{
				float value_float = values[j].r;
				char* value = new char[sizeof(float)];
				value = reinterpret_cast<char*>(&value_float);
				memcpy(thisRecord + sizeOffset, value, sizeof(float));
				//delete[] value;
			}
			else if (values[j].type == TYPE::VARCHAR)
			{
				const char* value = new char[table.attrs[attrIndices[j]].size];
				value = values[j].str.c_str();
				memcpy(thisRecord + sizeOffset, value, table.attrs[attrIndices[j]].size);
				//delete[] value;
			}
		}
		if (isUnique(table, thisRecord, recordIndexInFile) == false)
		{
			if (i == 0)		//it's the first record to be updated
			{
				delete[] readBuffer;
				delete[] thisRecord;
				delete[] firstRecord;
				delete[] firstReadBuffer;
				return -1;
			}
			else	//this record is not unique because of last updated record, we must restore last updated record
			{
				memcpy(firstReadBuffer + firstRecordIndexInBlock * recordLen, firstRecord, recordLen);
				delete[] readBuffer;
				delete[] thisRecord;
				//delete[] firstRecord;
				//delete[] firstReadBuffer;
				return -1;
			}
		}
		else
		{
			memcpy(readBuffer + recordIndexInBlock * recordLen, thisRecord, recordLen);
			bm.writeFileData(fileName, blockIndex, readBuffer);
		}

		delete[] readBuffer;
		delete[] thisRecord;
	}

	delete[] firstRecord;
	delete[] firstReadBuffer;
	return indices.size();
}

void RecordManager::findAllIndex(Table& table, vector<int>& indices)
{
	string fileName = table.tableName + ".MYD";
	int fileSize = bm.getFileSize(fileName);
	int recordLen = getRecordLen(table);
	int blockNum = fileSize / blockSize;
	int recordNum = blockSize / recordLen;	//maximum number of records in one block

	if (blockNum == 0)
		return;

	for (int i = 0; i < blockNum - 1; i++)
		for (int j = 0; j < recordNum; j++)
			indices.push_back(i * recordNum + j);

	int recordNumInLastBlock = table.totalRecordNum - (blockNum - 1) * recordNum;
	for (int i = 0; i < recordNumInLastBlock; i++)
		indices.push_back((blockNum - 1) * recordNum + i);
}

void RecordManager::findIndex(Table& table, vector<int>& indices, Condition& con)
{
	string fileName = table.tableName + ".MYD";
	int fileSize = bm.getFileSize(fileName);
	int recordLen = getRecordLen(table);
	int blockNum = fileSize / blockSize;
	int recordNum = blockSize / recordLen;	//maximum number of records in one block

	if (blockNum == 0)
		return;

	int attrIndex = getAttrIndex(table, con.attributeName);		//index of attribute in table
	int attrSize = table.attrs[attrIndex].size;
	TYPE attrType = table.attrs[attrIndex].type;
	int sizeOffset = 0;
	for (int i = 0; i < attrIndex; i++)
		sizeOffset += table.attrs[i].size;

	for (int i = 0; i < blockNum - 1; i++)
	{
		char* readBuffer = new char[blockSize];
		bm.readFileData(fileName, i, readBuffer);
		for (int j = 0; j < recordNum; j++)
		{
			char* recordData = new char[recordLen];
			memcpy(recordData, readBuffer + j * recordLen, recordLen);
			char* attrData = new char[attrSize];
			memcpy(attrData, recordData + sizeOffset, attrSize);
			if (attrType == TYPE::INT)
			{
				int* value_int;
				value_int = reinterpret_cast<int*>(attrData);
				if (con.ifRight(*value_int))
					indices.push_back(i * recordNum + j);
			}
			else if (attrType == TYPE::FLOAT)
			{
				float* value_float;
				value_float = reinterpret_cast<float*>(attrData);
				if (con.ifRight(*value_float) == true)
					indices.push_back(i * recordNum + j);
			}
			else if (attrType == TYPE::VARCHAR)
			{
				string str = attrData;
				if (con.ifRight(str) == true)
					indices.push_back(i * recordNum + j);
			}
			delete[] recordData;
			delete[] attrData;
		}
		delete[] readBuffer;
	}

	//last block might not be full, so consider it differently
	int recordNumInLastBlock = table.totalRecordNum % recordNum;
	char* readBuffer = new char[blockSize];
	bm.readFileData(fileName, blockNum - 1, readBuffer);
	for (int i = 0; i < recordNumInLastBlock; i++)
	{
		char* recordData = new char[recordLen];
		memcpy(recordData, readBuffer + i * recordLen, recordLen);
		char* attrData = new char[attrSize];
		memcpy(attrData, recordData + sizeOffset, attrSize);
		if (attrType == TYPE::INT)
		{
			int* value_int;
			value_int = reinterpret_cast<int*>(attrData);
			if (con.ifRight(*value_int))
				indices.push_back((blockNum - 1) * recordNum + i);
		}
		else if (attrType == TYPE::FLOAT)
		{
			float* value_float;
			value_float = reinterpret_cast<float*>(attrData);
			if (con.ifRight(*value_float) == true)
				indices.push_back((blockNum - 1) * recordNum + i);
		}
		else if (attrType == TYPE::VARCHAR)
		{
			string str = attrData;
			if (con.ifRight(str) == true)
				indices.push_back((blockNum - 1) * recordNum + i);
		}
		delete[] recordData;
		delete[] attrData;
	}
	delete[] readBuffer;
}

void RecordManager::createIndex(Table& table, int attrIndex)
{
	string fileName = table.tableName + "_" + table.attrs[attrIndex].attrName;
	fstream fp(fileName, ifstream::binary | ifstream::out);
	fp.close();
	im.create(fileName, table.attrs[attrIndex].type, table.attrs[attrIndex].size);
	table.attrs[attrIndex].indexFlag = 1;

	string tbFileName = table.tableName + ".MYD";
	int fileSize = bm.getFileSize(tbFileName);
	int recordLen = getRecordLen(table);
	int blockNum = fileSize / blockSize;
	int recordNum = blockSize / recordLen;	//maximum number of records in one block
	int attrSize = table.attrs[attrIndex].size;
	TYPE attrType = table.attrs[attrIndex].type;
	int sizeOffset = 0;
	for (int i = 0; i < attrIndex; i++)
		sizeOffset += table.attrs[i].size;

	int fileIndex;

	for (int i = 0; i < blockNum - 1; i++)
	{
		char* readBuffer = new char[blockSize];
		bm.readFileData(tbFileName, i, readBuffer);
		for (int j = 0; j < recordNum; j++)
		{
			fileIndex = i * recordNum + j;
			char* recordData = new char[recordLen];
			memcpy(recordData, readBuffer + j * recordLen, recordLen);
			char* attrData = new char[attrSize];
			memcpy(attrData, recordData + sizeOffset, attrSize);
			if (attrType == TYPE::INT)
			{
				int* value_int;
				value_int = reinterpret_cast<int*>(attrData);
				SqlValue val_int(*value_int);
				im.insert(fileName, val_int, fileIndex);
			}
			else if (attrType == TYPE::FLOAT)
			{
				float* value_float;
				value_float = reinterpret_cast<float*>(attrData);
				SqlValue val_float((float)*value_float);
				im.insert(fileName, val_float, fileIndex);
			}
			else if (attrType == TYPE::VARCHAR)
			{
				string str = attrData;
				SqlValue val_str(str);
				im.insert(fileName, val_str, fileIndex);
			}
			delete[] recordData;
			delete[] attrData;
		}
		delete[] readBuffer;
	}

	//last block might not be full, so consider it differently
	int recordNumInLastBlock = table.totalRecordNum - (blockNum - 1) * recordNum;
	char* readBuffer = new char[blockSize];
	bm.readFileData(tbFileName, blockNum - 1, readBuffer);
	for (int i = 0; i < recordNumInLastBlock; i++)
	{
		fileIndex = (blockNum - 1) * recordNum + i;
		char* recordData = new char[recordLen];
		memcpy(recordData, readBuffer + i * recordLen, recordLen);
		char* attrData = new char[attrSize];
		memcpy(attrData, recordData + sizeOffset, attrSize);
		if (attrType == TYPE::INT)
		{
			int* value_int;
			value_int = reinterpret_cast<int*>(attrData);
			SqlValue val_int(*value_int);
			im.insert(fileName, val_int, fileIndex);
		}
		else if (attrType == TYPE::FLOAT)
		{
			float* value_float;
			value_float = reinterpret_cast<float*>(attrData);
			SqlValue val_float((float)*value_float);
			im.insert(fileName, val_float, fileIndex);
		}
		else if (attrType == TYPE::VARCHAR)
		{
			string str = attrData;
			SqlValue val_str(str);
			im.insert(fileName, val_str, fileIndex);
		}
		delete[] recordData;
		delete[] attrData;
	}
	delete[] readBuffer;
}

void RecordManager::dropIndex(Table& table, int attrIndex)
{
	string fileName = table.tableName + "_" + table.attrs[attrIndex].attrName;
	remove(fileName.c_str());
	im.drop(fileName, table.attrs[attrIndex].type);
	table.attrs[attrIndex].indexFlag = 0;

}

void RecordManager::bpFindIndex(Table& table, vector<int>& indices, Condition& con)
{
	int attrIndex = getAttrIndex(table, con.attributeName);		//index of attribute in table
	int attrSize = table.attrs[attrIndex].size;
	TYPE attrType = table.attrs[attrIndex].type;
	int sizeOffset = 0;
	string indexFileName = table.tableName + "_" + table.attrs[attrIndex].attrName;
	for (int i = 0; i < attrIndex; i++)
		sizeOffset += table.attrs[i].size;

	int index = im.search(indexFileName, con.value);
	int headIndex = im.searchHead(indexFileName, attrType);
	int nextIndex;
	if (con.operate == 0)	//=
	{
		if (index != -1)
			indices.push_back(index);
	}
	else if (con.operate == 1)	//!=
	{
		if (headIndex != index)
			indices.push_back(headIndex);
		for (int i = 0; i < table.totalRecordNum - 1; i++)
		{
			nextIndex = im.searchNext(indexFileName, attrType);
			if (nextIndex != index)
				indices.push_back(nextIndex);
		}
	}
	/*else if (con.operate == 2)	//<
	{
		if (index != -1 && headIndex != index)
			indices.push_back(headIndex);
		for (int i = 0; i < table.totalRecordNum - 1; i++)
		{
			nextIndex = im.searchNext(indexFileName, attrType);
			if (nextIndex != index)
				indices.push_back(nextIndex);
			else
				break;
		}
	}
	else if (con.operate == 3)	//>
	{
		//headIndex is impossible to satisfy this condition, since its key must be smaller than or equal to that key
		int startPos = -1;
		for (int i = 0; i < table.totalRecordNum - 1; i++)
		{
			nextIndex = im.searchNext(indexFileName, attrType);
			if (nextIndex == index)
			{
				startPos = i;
				break;
			}
		}
		if (startPos != -1)
		{
			for (int i = startPos; i < table.totalRecordNum; i++)
			{
				nextIndex = im.searchNext(indexFileName, attrType);
				indices.push_back(nextIndex);
			}
		}
	}
	else if (con.operate == 4)	//<=
	{
		for (int i = 0; i < table.totalRecordNum; i++)
		{
			nextIndex = im.searchNext(indexFileName, attrType);
			if (nextIndex != index)
				indices.push_back(getIndex(indexFileName, i));
			else
				break;
		}
		if (index != -1)
			indices.push_back(index);
	}
	else if (con.operate == 5)	//>=
	{
		int startPos = -1;
		for (int i = 0; i < table.totalRecordNum; i++)
		{
			nextIndex = im.searchNext(indexFileName, attrType);
			if (nextIndex == index)
			{
				startPos = i;
				break;
			}
		}
		if (startPos != -1)
		{
			for (int i = startPos; i < table.totalRecordNum; i++)
				indices.push_back(getIndex(indexFileName, i));
		}
		if (index != -1)
			indices.push_back(index);
	}*/
	else
	{
		cout << "Error: Unsupported condition!" << endl;
		return;
	}
}