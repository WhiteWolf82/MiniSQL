#ifndef INDEX_MANAGER_H
#define INDEX_MANAGER_H

#include <string>
#include <map>
#include <vector>
#include "Structure.h"
#include "BPTree.h"

using namespace std;

class IndexManager 
{
public:
    IndexManager() = default;

    ~IndexManager();

    bool create(const string &filename, const TYPE &type, const int &size);

    bool drop(const string &filename, const TYPE &type);

    int search(const string &filename, const struct SqlValue &e);

    int searchHead(const string &filename, TYPE &type);

    int searchNext(const string &filename, TYPE &type);

    bool finishSearch(const string &filename, TYPE &type);

    bool insert(const string &filename, const struct SqlValue &e, int indice);

    bool removeKey(const string &filename, const struct SqlValue &e, const struct SqlValue &last);

private:
    typedef map<string, BPTree<int> *> intMap;
    typedef map<string, BPTree<float> *> floatMap;
    typedef map<string, BPTree<string> *> varcharMap;
    
    typedef map<string, NodeSearchParse<int>> intOMap;
    typedef map<string, NodeSearchParse<float>> floatOMap;
    typedef map<string, NodeSearchParse<string>> varcharOMap;

    intMap intIndexMap;
    floatMap floatIndexMap;
    varcharMap varcharIndexMap;
    
    intOMap intOffsetMap;
    floatOMap floatOffsetMap;
    varcharOMap varcharOffsetMap;

    BPTree<int> *intBPTree;
    BPTree<float> *floatBPTree;
    BPTree<string> *varcharBPTree;

    intMap::iterator intBPIterator;
    floatMap::iterator floatBPIterator;
    varcharMap::iterator varcharBPIterator;
};


#endif
