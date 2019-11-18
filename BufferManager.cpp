#include"BufferManager.h"

BufferUnit::BufferUnit(int bufferBlockNum_max, int blockSize, string fileName) :
	bufferBlockNum_max(bufferBlockNum_max), blockSize(blockSize), fileName(fileName)
{
	ifstream fp(fileName, ifstream::binary | ifstream::ate);
	int tmpFileSize = (int)fp.tellg();

	if (tmpFileSize == -1)
	{
		cout << "Error: File does not exist!" << endl;
		return;
	}

	//file size must be an integer multiple of block size
	fileSize = upperFloor((tmpFileSize * 1.0 / blockSize) * blockSize);

	if (fileSize != tmpFileSize)	//insert random data into file in order to align file size
	{
		char* tmp = new char[tmpFileSize - fileSize];	//random data
		ofstream out(fileName, ifstream::binary | ifstream::ate | ifstream::out);
		out.write(tmp, tmpFileSize - fileSize);
		out.close();
		delete[] tmp;
	}

	fileBlockNum = fileSize / blockSize;	//must be an integer
	bufferBlockIndex = vector<int>(fileBlockNum, -1);	//initialize it to -1, representing that it is not in buffer now
	bufferBlockNum = 0;		//no block in buffer
	//initialize
	for (int i = 0; i < bufferBlockNum_max; i++)	//allocate maximum space for the vector
	{
		Block block;
		bufferBlocks.emplace_back(block);
	}
}

void BufferUnit::swapBlock(int bufferIndex, int fileIndex)
{
	if (bufferBlocks[bufferIndex].modifyFlag == 1)	//be modified before
	{
		fstream tmp(fileName);
		tmp.seekp(bufferBlocks[bufferIndex].index * blockSize);
		tmp.write(bufferBlocks[bufferIndex].data, blockSize);
		tmp.close();
	}

	fstream fp(fileName, ifstream::binary | ifstream::in);
	fp.seekp(fileIndex * blockSize);
	fp.read(bufferBlocks[bufferIndex].data, blockSize);
	fp.close();

	bufferBlocks[bufferIndex].index = fileIndex;
	bufferBlocks[bufferIndex].clockFlag = 1;	//visited recently
	bufferBlocks[bufferIndex].lockFlag = 0;
	bufferBlocks[bufferIndex].modifyFlag = 0;
	bufferBlocks[bufferIndex].deleteFlag = 0;
}

int BufferUnit::getValidBlock()
{
	if (bufferBlockNum < bufferBlockNum_max)	//there is still space in buffer
	{
		for (int i = 0; i < bufferBlockNum_max; i++)	//if there is a deleted block, find the first such one
		{
			if (bufferBlocks[i].deleteFlag == 1)
			{
				bufferBlockNum++;
				return i;
			}
		}
		//else, insert the block at the end of buffer
		bufferBlockNum++;
		return (bufferBlockNum - 1);
	}
	else	//buffer is full
	{
		int i = 0;
		while (1)
		{
			if (bufferBlocks[i].lockFlag == 0)		//block not locked
			{
				if (bufferBlocks[i].clockFlag == 1)		//if this block has been visited recently, reset its clockFlag = 0
					bufferBlocks[i].clockFlag = 0;
				else if (bufferBlocks[i].clockFlag == 0)	//found the block
					return i;
			}
			//if not found, return the first block
			i = (i + 1) % bufferBlockNum_max;
		}
	}
}

void BufferUnit::readBlock(int index, char* readBuffer)
{
	if (index < 0 || index >= fileBlockNum)
	{
		//cout << "Error: Memory exceed!" << endl;
		return;
	}

	if (bufferBlockIndex[index] != -1)	//block is in buffer
	{
		int bufferIndex = bufferBlockIndex[index];
		memcpy(readBuffer, bufferBlocks[bufferIndex].data, blockSize);
		bufferBlocks[bufferIndex].clockFlag = 1;	//be visited recently
	}
	else	//block is not in buffer
	{
		int place = getValidBlock();
		if (place != bufferBlockNum - 1)	//it's swapping not inserting
			bufferBlockIndex[bufferBlocks[place].index] = -1;
		bufferBlockIndex[index] = place;
		swapBlock(place, index);	//if it's a new place in buffer, just insert it instead of swapping
		memcpy(readBuffer, bufferBlocks[place].data, blockSize);
	}

	/*if (bufferBlockNum < bufferBlockNum_max)
	{
		fstream tmp(fileName);
		tmp.seekp(bufferBlocks[index].index * blockSize);
		tmp.write(bufferBlocks[index].data, blockSize);
		tmp.close();
	}*/
}

void BufferUnit::writeBlock(int index, char* writeBuffer)
{
	if (index < 0 || index > fileBlockNum)
	{
		//cout << "Error: Memory exceed!" << endl;
		return;
	}

	if (index == fileBlockNum)	//write a new block into file
	{
		fileBlockNum++;
		fstream tmp(fileName, ifstream::binary | ifstream::app | ifstream::out);
		tmp.write(writeBuffer, blockSize);
		tmp.close();
		fileSize += blockSize;
		//insert or swap it into buffer
		int place = getValidBlock();
		bufferBlockIndex.push_back(place);
		if (place != bufferBlockNum - 1)	//it's swapping not inserting
			bufferBlockIndex[bufferBlocks[place].index] = -1;
		bufferBlockIndex[index] = place;
		swapBlock(place, index);
	}
	else  //modify an existed block
	{
		if (bufferBlockIndex[index] != -1)	//block is in buffer
		{
			int bufferIndex = bufferBlockIndex[index];
			memcpy(bufferBlocks[bufferIndex].data, writeBuffer, blockSize);
			bufferBlocks[bufferIndex].clockFlag = 1;
			bufferBlocks[bufferIndex].modifyFlag = 1;
		}
		else	//block is not in buffer
		{
			int place = getValidBlock();
			if (place != bufferBlockNum - 1)	//it's swapping not inserting
				bufferBlockIndex[bufferBlocks[place].index] = -1;
			bufferBlockIndex[index] = place;
			swapBlock(place, index);
			memcpy(bufferBlocks[place].data, writeBuffer, blockSize);
			bufferBlocks[place].clockFlag = 1;
			bufferBlocks[place].modifyFlag = 1;
		}
	}
	/*if (bufferBlockNum < bufferBlockNum_max)
	{
		fstream tmp(fileName);
		tmp.seekp(bufferBlocks[index].index * blockSize);
		tmp.write(bufferBlocks[index].data, blockSize);
		tmp.close();
	}*/
}

void BufferUnit::lockBlock(int index)
{
	if (index < 0 || index >= fileBlockNum)
	{
		cout << "Error: Memory exceed!" << endl;
		return;
	}
	if (bufferBlockIndex[index] == -1)
	{
		cout << "Error: Block not in buffer!" << endl;
		return;
	}

	bufferBlocks[bufferBlockIndex[index]].lockFlag = 1;
}

void BufferUnit::deleteLastBlock()
{
	fileSize -= blockSize;
	fileBlockNum--;
	int bufferIndex = bufferBlockIndex.back();
	if (bufferIndex != -1)	//block is in buffer, then delete
	{
		bufferBlocks[bufferIndex].deleteFlag = 1;	//shallow deletion
		bufferBlocks[bufferIndex].index = -1;
		bufferBlocks[bufferIndex].lockFlag = 0;
		bufferBlocks[bufferIndex].modifyFlag = 0;
		bufferBlockNum--;
	}

	bufferBlockIndex.erase(bufferBlockIndex.end() - 1);

	//delete the last block in file
	char* tmp = new char[fileSize];
	fstream fp(fileName, ifstream::binary | ifstream::in);
	fp.read(tmp, fileSize);
	fp.close();
	remove(fileName.c_str());	//delete old file	
	fstream op(fileName, ifstream::binary | ifstream::out);		//create new file
	op.write(tmp, fileSize);
	op.close();
	delete[] tmp;
}

int BufferManager::getFileIndex(string fileName)
{
	for (int i = 0; i < fileNum; i++)
	{
		if (files[i] == fileName)
			return i;
	}
	return -1;
}

int BufferManager::getFileSize(string fileName)
{
	return (int)(ifstream(fileName, ifstream::binary | ifstream::ate).tellg());
}

void BufferManager::createFile(string fileName)
{
	//create file
	fstream fp(fileName, ifstream::binary | ifstream::out);
	fp.close();
	files.push_back(fileName);
	fileNum++;

	//create buffer
	BufferUnit buffer(bufferBlockNum, blockSize, fileName);
	buffers.push_back(buffer);
}

void BufferManager::deleteFile(string fileName)
{
	int fileIndex = getFileIndex(fileName);
	if (fileIndex == -1)
	{
		cout << "Error: File does not exist!" << endl;
		return;
	}
	else
	{
		//delete file
		remove(fileName.c_str());
		vector<string>::iterator fileIter = find(files.begin(), files.end(), fileName);
		files.erase(fileIter);
		fileNum--;

		//delete buffer
		vector<BufferUnit>::iterator bufferIter = buffers.begin();
		while (fileIndex--)
		{
			bufferIter++;
		}
		buffers.erase(bufferIter);
	}
}

void BufferManager::readFileData(string fileName, int blockIndex, char* readBuffer)
{
	int fileIndex = getFileIndex(fileName);
	if (fileIndex == -1)
	{
		cout << "Error: File does not exist!" << endl;
		return;
	}

	buffers[fileIndex].readBlock(blockIndex, readBuffer);
}

void BufferManager::readAllFileData(string fileName, vector<char*> readBuffers)
{
	int fileIndex = getFileIndex(fileName);
	if (fileIndex == -1)
	{
		cout << "Error: File does not exist!" << endl;
		return;
	}

	for (int i = 0; i < buffers[fileIndex].fileBlockNum; i++)
	{
		char* tmp = new char[blockSize];
		buffers[fileIndex].readBlock(i, tmp);
		readBuffers.emplace_back(tmp);
		delete[] tmp;
	}
}

void BufferManager::writeFileData(string fileName, int blockIndex, char* writeBuffer)
{
	int fileIndex = getFileIndex(fileName);
	if (fileIndex == -1)
	{
		cout << "Error: File does not exist!" << endl;
		return;
	}

	buffers[fileIndex].writeBlock(blockIndex, writeBuffer);
}

void BufferManager::lockFileBlock(string fileName, int blockIndex)
{
	int fileIndex = getFileIndex(fileName);
	if (fileIndex == -1)
	{
		cout << "Error: File does not exist!" << endl;
		return;
	}

	buffers[fileIndex].lockBlock(blockIndex);
}

void BufferManager::deleteLastFileBlock(string fileName)
{
	int fileIndex = getFileIndex(fileName);
	if (fileIndex == -1)
	{
		cout << "Error: File does not exist!" << endl;
		return;
	}

	buffers[fileIndex].deleteLastBlock();
}