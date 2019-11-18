#include "IndexManager.h"

IndexManager::~IndexManager() {}

bool IndexManager::create(const string &filename, const TYPE &type, const int &size)
{
    int itemSize = size;
    int treeDegree = 4096 / (itemSize + sizeof(int));
    
    switch (type) 
	{
        case TYPE::INT:
            intBPTree = new BPTree<int>(filename, itemSize, treeDegree);
            intIndexMap.insert(intMap::value_type(filename, intBPTree));
			return true;
            break;
        case TYPE::FLOAT:
            floatBPTree = new BPTree<float>(filename, itemSize, treeDegree);
            floatIndexMap.insert(floatMap::value_type(filename, floatBPTree));
			return true;
			break;
        case TYPE::VARCHAR:
            varcharBPTree = new BPTree<string>(filename, itemSize, treeDegree);
            varcharIndexMap.insert(varcharMap::value_type(filename, varcharBPTree));
			return true;
			break;
        default:
            cerr << "Undefined type!" << endl;
			return false;
			break;
    }
}

bool IndexManager::drop(const string &filename, const TYPE &type)
{
    switch (type) 
	{
        case TYPE::INT:
            intBPIterator = intIndexMap.find(filename);
            delete intBPIterator->second;
            intIndexMap.erase(intBPIterator);
			return true;
            break;
        case TYPE::FLOAT:
            floatBPIterator = floatIndexMap.find(filename);
            delete floatBPIterator->second;
            floatIndexMap.erase(floatBPIterator);
			return true;
            break;
        case TYPE::VARCHAR:
            varcharBPIterator = varcharIndexMap.find(filename);
            delete varcharBPIterator->second;
            varcharIndexMap.erase(varcharBPIterator);
			return true;
            break;
        default:
            cerr << "Undefined type!" << endl;
			return false;
            break;
    }
}

int IndexManager::search(const string &filename, const struct SqlValue &e)
{
    NodeSearchParse<int> intNode;
    NodeSearchParse<float> floatNode;
    NodeSearchParse<string> varcharNode;
    
    switch (e.type)
	{
        case TYPE::INT:
            intNode = intIndexMap.find(filename)->second->findNode(e.i);
            intOffsetMap[filename] = intNode;
            return intNode.node->indices[intNode.index];
        case TYPE::FLOAT:
            floatNode = floatIndexMap.find(filename)->second->findNode(e.r);
            floatOffsetMap[filename] = floatNode;
            return floatNode.node->indices[floatNode.index];
        case TYPE::VARCHAR:
            varcharNode = varcharIndexMap.find(filename)->second->findNode(e.str);
            varcharOffsetMap[filename] = varcharNode;
            return varcharNode.node->indices[varcharNode.index];
        default:
            cerr << "Undefined type!" << endl;
            return -1;
    }
}

int IndexManager::searchHead(const string &filename, TYPE &type)
{
    NodeSearchParse<int> intNode;
    NodeSearchParse<float> floatNode;
    NodeSearchParse<string> varcharNode;
    
    switch (type) 
	{
        case TYPE::INT:
            intNode.node = intIndexMap.find(filename)->second->getHeadNode();
            intNode.index = 0;
            intOffsetMap[filename] = intNode;
            return intNode.node->indices[intNode.index];
        case TYPE::FLOAT:
            floatNode.node = floatIndexMap.find(filename)->second->getHeadNode();
            floatNode.index = 0;
            floatOffsetMap[filename] = floatNode;
            return floatNode.node->indices[floatNode.index];
        case TYPE::VARCHAR:
            varcharNode.node = varcharIndexMap.find(filename)->second->getHeadNode();
            varcharNode.index = 0;
            varcharOffsetMap[filename] = varcharNode;
            return varcharNode.node->indices[varcharNode.index];
        default:
            cerr << "Undefined type!" << endl;
            break;
    }
}

int IndexManager::searchNext(const string &filename, TYPE &type)
{
    NodeSearchParse<int> intNode;
    NodeSearchParse<float> floatNode;
    NodeSearchParse<string> varcharNode;

    switch (type) 
	{
        case TYPE::INT:
            intNode = intOffsetMap.find(filename)->second;
            intNode.index++;
            if (intNode.index == intNode.node->cnt) 
			{
                intNode.node = intNode.node->sibling;
                intNode.index = 0;
            }
            intOffsetMap[filename] = intNode;
            if (intNode.node != nullptr) 
			{
                return intNode.node->indices[intNode.index];
            }
            break;
        case TYPE::FLOAT:
            floatNode = floatOffsetMap.find(filename)->second;
            floatNode.index++;
            if (floatNode.index == floatNode.node->cnt) 
			{
                floatNode.node = floatNode.node->sibling;
                floatNode.index = 0;
            }
            floatOffsetMap[filename] = floatNode;
            if (floatNode.node != nullptr) 
			{
                return floatNode.node->indices[floatNode.index];
            }
            break;
        case TYPE::VARCHAR:
            varcharNode = varcharOffsetMap.find(filename)->second;
            varcharNode.index++;
            if (varcharNode.index == varcharNode.node->cnt) 
			{
                varcharNode.node = varcharNode.node->sibling;
                varcharNode.index = 0;
            }
            varcharOffsetMap[filename] = varcharNode;
            if (varcharNode.node != nullptr) 
			{
                return varcharNode.node->indices[varcharNode.index];
            }
            break;
        default:
            cerr << "Undefined type!" << endl;
            return -1;
    }
    return -1;
}

bool IndexManager::finishSearch(const string &filename, TYPE &type)
{
    switch (type) 
	{
        case TYPE::INT:
            intOffsetMap.erase(filename);
            break;
        case TYPE::FLOAT:
            floatOffsetMap.erase(filename);
            break;
        case TYPE::VARCHAR:
            varcharOffsetMap.erase(filename);
            break;
        default:
            cerr << "Undefined type!" << endl;
            break;
    }
    return true;
}

bool IndexManager::insert(const string &filename, const struct SqlValue &e, int indice)
{
    switch (e.type) 
	{
        case TYPE::INT:
            return intIndexMap.find(filename)->second->insert(e.i, indice);
        case TYPE::FLOAT:
            return floatIndexMap.find(filename)->second->insert(e.r, indice);
        case TYPE::VARCHAR:
            return varcharIndexMap.find(filename)->second->insert(e.str, indice);
        default:
            cerr << "Undefined type!" << endl;
            break;
    }
}

bool IndexManager::removeKey(const string &filename, const struct SqlValue &e, const struct SqlValue &last)
{
    switch (e.type) 
	{
        case TYPE::INT:
            return intIndexMap.find(filename)->second->remove(e.i, last.i);
        case TYPE::FLOAT:
            return floatIndexMap.find(filename)->second->remove(e.r, last.r);
        case TYPE::VARCHAR:
            return varcharIndexMap.find(filename)->second->remove(e.str, last.str);
        default:
            cerr << "Undefined type!" << endl;
            break;
    }
}
