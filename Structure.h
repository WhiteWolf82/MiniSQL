#ifndef STRUCTURE_H
#define STRUCTURE_H

#include <string>

using namespace std;

enum class TYPE
{
	INT,
	VARCHAR,
	FLOAT
};

struct SqlValue
{
	TYPE type;
	int size;
	int i;
	float r;
	string str;

	SqlValue() = default;
	SqlValue(int i) : type(TYPE::INT), i(i), size(sizeof(int)) {}
	SqlValue(float r) : type(TYPE::FLOAT), r(r), size(sizeof(float)) {}
	SqlValue(string s) : type(TYPE::VARCHAR), str(s), size(s.size()) {}

	bool operator<(const SqlValue &e) const
	{
		switch (type)
		{
		case TYPE::INT:
			return i < e.i;
		case TYPE::FLOAT:
			return r < e.r;
		case TYPE::VARCHAR:
			return str < e.str;
		default:
			throw runtime_error("Undefined Type!");
		}
	}

	bool operator==(const SqlValue &e) const
	{
		switch (type)
		{
		case TYPE::INT:
			return i == e.i;
		case TYPE::FLOAT:
			return r == e.r;
		case TYPE::VARCHAR:
			return str == e.str;
		default:
			throw runtime_error("Undefined Type!");
		}
	}

	bool operator!=(const SqlValue &e) const { return !operator==(e); }

	bool operator>(const SqlValue &e) const { return !operator<(e) && operator!=(e); }

	bool operator<=(const SqlValue &e) const { return operator<(e) || operator==(e); }

	bool operator>=(const SqlValue &e) const { return !operator<(e); }

	void reset()
	{
		str.clear();
		i = 0;
		r = 0;
	}

	string toStr() const
	{
		switch (type)
		{
		case TYPE::INT:
			return to_string(i);
		case TYPE::FLOAT:
			return to_string(r);
		case TYPE::VARCHAR:
			return this->str;
		}
	}

	inline int getSize() const {
		return size;
	}
};

class Condition
{
public:
	const static int OPERATOR_EQUAL = 0;      //0代表"="
	const static int OPERATOR_NOT_EQUAL = 1;  //1代表"<>"（不等于）
	const static int OPERATOR_LESS = 2;       //2代表"<"
	const static int OPERATOR_MORE = 3;       //3代表">"
	const static int OPERATOR_LESS_EQUAL = 4; //4代表"<="
	const static int OPERATOR_MORE_EQUAL = 5; //5代表">="

	Condition() = default;
	Condition(string a, SqlValue s, int o) : attributeName(a), value(s), operate(o) {} //构造函数

	string attributeName; //关于哪个属性的条件
	SqlValue value;       //被比较的值
	int operate;          //比较的符号

	//无错误处理
	bool ifRight(int content)	    //content op value == true or false?
	{
		switch (operate)
		{
		case 0:
			return content == value.i;
		case 1:
			return content != value.i;
		case 2:
			return content < value.i;
		case 3:
			return content > value.i;
		case 4:
			return content <= value.i;
		case 5:
			return content >= value.i;
		}
	}
	bool ifRight(float content)		//同上理
	{
		switch (operate)
		{
		case 0:
			return content == value.r;
		case 1:
			return content != value.r;
		case 2:
			return content < value.r;
		case 3:
			return content > value.r;
		case 4:
			return content <= value.r;
		case 5:
			return content >= value.r;
		}
	}
	bool ifRight(string content)	//同上理
	{
		switch (operate)
		{
		case 0:
			return content == value.str;
		case 1:
			return content != value.str;
		case 2:
			return content < value.str;
		case 3:
			return content > value.str;
		case 4:
			return content <= value.str;
		case 5:
			return content >= value.str;
		}
	}
};

class Attribute
{
public:
	string attrName;
	TYPE type;
	int size;         //fixed size
	bool uniqueFlag;  //whether is unique
	bool indexFlag;   //whether is index
	bool primaryFlag; //whether is primary key
	string indexName;

	Attribute() = default;
	Attribute(string attrName, TYPE type, int size, bool uniqueFlag, bool indexFlag, bool primaryFlag, string s = "")
		: attrName(attrName), type(type), size(size), uniqueFlag(uniqueFlag), indexFlag(indexFlag), primaryFlag(primaryFlag), indexName(s)
	{
	}
};

class Table
{
public:
	string tableName;
	int totalRecordNum;
	vector<Attribute> attrs;

	Table() = default;
	Table(const string tableName, const vector<Attribute> &attrs) : tableName(tableName), attrs(attrs)
	{
		totalRecordNum = 0;
	}
};

#endif
