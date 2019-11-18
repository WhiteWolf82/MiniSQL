#ifndef BUFFERMANAGER_H
#define BUFFERMANAGER_H

#include<iostream>
#include<fstream>
#include<vector>
#include<string>
#include<algorithm>

using namespace std;

class Block
{
public:
	int blockSize;
	int index;	//index in file
	int clockFlag;	//whether recently visited
	int lockFlag;	//whether locked
	int modifyFlag;	//whether modified
	int deleteFlag; //whether deleted
	char* data;
	Block()
	{
		blockSize = 4096;	//4KB
		index = -1;
		clockFlag = 0;
		lockFlag = 0;
		modifyFlag = 0;
		deleteFlag = 0;
		data = new char[4096];
	}
};

class BufferUnit
{
public:
	BufferUnit(int bufferBlockNum_max, int blockSize, string fileName);
	int bufferBlockNum;		//current number of blocks in buffer
	int fileBlockNum;	//current number of blocks in file
	void swapBlock(int bufferIndex, int fileIndex);
	void readBlock(int index, char* readBuffer);
	void writeBlock(int index, char* writeBuffer);
	void lockBlock(int index);
	void deleteLastBlock();
	int getValidBlock();	//find the block to be swapped or a new block

private:
	int bufferBlockNum_max;		//maximum number of blocks in buffer
	int blockSize;
	string fileName;
	int fileSize;
	vector<int> bufferBlockIndex;	//the index of the block in buffer
	vector<Block> bufferBlocks;		//the blocks in buffer
	inline int upperFloor(double val)
	{
		if ((double)(int)(val) == val)
			return (int)val;
		else
			return (int)(val + 1);
	}
};

class BufferManager
{
public:
	BufferManager()
	{
		blockSize = 4096;
		bufferBlockNum = 256;
		fileNum = 0;
	}
	int getFileSize(string fileName);
	void createFile(string fileName);	//create buffer for the file as well
	void deleteFile(string fileName);	//delete buffer of the file as well
	void readFileData(string fileName, int blockIndex, char* readBuffer);
	void readAllFileData(string fileName, vector<char*> readBuffers);	//used when requesting a "select *"
	void writeFileData(string fileName, int blockIndex, char* writeBuffer);
	void lockFileBlock(string fileName, int blockIndex);
	void deleteLastFileBlock(string fileName);

private:
	vector<BufferUnit> buffers;
	vector<string> files;
	vector<int> fileSizes;
	int blockSize;
	int bufferBlockNum;
	int fileNum;	//always equal to block number
	int getFileIndex(string fileName);
};

#endif // !BUFFERMANAGER_H
