//
// Created by csq on 6/10/19.
//

#include "Interpreter.h"

using namespace std;

Interpreter::Interpreter()
{
	cm = new CatalogManager();
	api = new API();
	pattern = "[[:graph:]]+ ";
	r = regex(pattern);
	isQuit = false;
}

Interpreter::~Interpreter()
{
	delete cm;
	delete api;
}

void Interpreter::main_loop(istream &input)
{
	bool isFirstLine = true;
	string statement, temp;
	QueryRequest *ptrQuery = nullptr;
	vector<string> words; //存储一个命令中的所有单词

	cout.setf(ios::fixed);

	while (!isQuit)
	{
		if (isFirstLine)
		{
			words.clear();
			statement = temp = "";
			cout << "MiniSQL> ";
			isFirstLine = false;
		}
		else
		{
			cout << "      -> ";
		}

		getline(input, temp);
		if (temp == "")
			continue;
		while (temp.size() >= 1 && temp[temp.size() - 1] == ' ')
		{
			temp.pop_back();
		}
		statement += temp;

		if (temp[temp.size() - 1] != ';')
		{
			statement += " ";
		}
		else if (temp[temp.size() - 1] == ';')
		{
			statement.pop_back(); //delete ';'
			statement += " ";

			for (sregex_iterator it(statement.begin(), statement.end(), r), end_it; it != end_it; ++it)
			{
				words.push_back(it->str());
			}

			for (auto &s : words)
			{
				s.pop_back();
			}
			try
			{
				ptrQuery = parse(words);
			}
			catch (out_of_range)
			{
				cerr << "Not a valid input" << endl;
				isFirstLine = true;
				continue;
			}
			executeAndCountTime(ptrQuery);
			//delete ptrQuery;

			isFirstLine = true;
		}
	}
}

void Interpreter::executeAndCountTime(QueryRequest * q)
{
	double start_time = clock();
	execute(q);
	double finish_time = clock();
	cout << " (" << fixed << setprecision(3) << (finish_time - start_time) / CLK_TCK << " sec)" << endl;
}

//失败情况的输出直接在API中进行，成功情况在execute输出，计时在mainloop
void Interpreter::execute(QueryRequest *query)
{
	if (query == nullptr)
	{
		cerr << "Not a valid input!!!" << endl;
		return;
	}
	
	auto e_file = dynamic_cast<ExecFileQuery *>(query);
	if (e_file)
	{
		exec_file(e_file->fileName);
		delete e_file;
		query = nullptr;
		return;
	}

	auto insert_query = dynamic_cast<InsertQuery *>(query);
	if (insert_query)
	{
		int num = api->insert(insert_query->table_name, insert_query->value_list);
		
		if (num == -1)
		{
			cout << "Insert Query fails, the unique key repeats";
		}
		else
		{
			cout << "Insert Query OK, 1 row affected";
		}
		delete insert_query;
		query = nullptr;
		return;
	}

	auto select_query = dynamic_cast<SelectQuery *>(query);
	if (select_query)
	{
		cm = new CatalogManager();
		int num;
		vector<char*> result;
		vector<string> attr_list;	//属性名
		Table t = cm->getTable(select_query->table_name);

		if (select_query->isSelectAll)
		{
			num = api->select(select_query->table_name, select_query->condition_list, result, select_query->s);
			for (auto attr : t.attrs)
			{
				attr_list.push_back(attr.attrName);
			}
		}
		else
		{
			num = api->select(select_query->table_name, select_query->condition_list, select_query->attrName_list, result, select_query->s);
			attr_list = select_query->attrName_list;
		}

		vector<int> attrNameSize;
		for (auto s : attr_list)
		{
			attrNameSize.push_back(s.size());
		}

		vector<pair<TYPE, int>> dataType;
		pair<TYPE, int> temp;
		for (string name : attr_list)
		{
			cm->getAttributeDataType(select_query->table_name, name, temp);
			dataType.push_back(temp);
		}

		if (num == 0)
		{
			cout << "Empty set";
		}
		else if (num > 0)
		{
			printAttrName(attr_list, attrNameSize);
			for (int i = 0; i < num; ++i)
			{
				printARecord(result[i], dataType);
			}
			for (int i = 0; i < attrNameSize.size(); ++i)
			{
				print('-', attrNameSize[i] + 2);
				cout << '+';
			}
			cout << '\n';
			if (num == 1)
			{
				cout << num << " row in set";
			}
			else
			{
				cout << num << " rows in set";
			}
		}

		delete select_query;
		query = nullptr;
		return;
	}

	auto delete_query = dynamic_cast<DeleteQuery *>(query);
	if (delete_query)
	{
		int affectNum = api->delete_op(delete_query->table_name, delete_query->condition_list, delete_query->s);

		if (affectNum <= 1)
			cout << "Delete Query OK, " << affectNum << " row affected";
		else if (affectNum > 1)
			cout << "Delete Query OK, " << affectNum << " rows affected";

		delete delete_query;
		query = nullptr;
		return;
	}
	/*
	auto update_query = dynamic_cast<UpdateQuery *>(query);
	if (update_query)
	{
		Api::update(Api::get_db_name_prefix() + update_query->table_name, update_query->attr, update_query->value,
			update_query->condition_list);
		delete update_query;
		query = nullptr;
		return;
	}
	*/
	auto create_table_query = dynamic_cast<CreateTableQuery *>(query);
	if (create_table_query)
	{
		bool b = api->create_table(create_table_query->table_name, create_table_query->attribute_list);

		if (b)
		{
			cout << "Create Table Query OK";
		}
		else
		{
			cout << "Create Table Query failed, it is not a unique key!";
		}
		delete create_table_query;
		query = nullptr;
		return;
	}

	auto create_index_query = dynamic_cast<CreateIndexQuery *>(query);
	if (create_index_query)
	{
		bool b = api->create_index(create_index_query->table_name, create_index_query->attr_name, create_index_query->index_name);

		if (b)
		{
			cout << "Create Index Query OK";
		}
		else
		{
			cout << "Create Index Query failed";
		}
		delete create_index_query;
		query = nullptr;
		return;
	}

	auto drop_table_query = dynamic_cast<DropTableQuery *>(query);
	if (drop_table_query)
	{
		bool b = api->drop_table(drop_table_query->table_name);

		if (b)
		{
			cout << "Drop Table Query OK";
		}
		else
		{
			cout << "Drop Table Query failed";
		}
		delete drop_table_query;
		query = nullptr;
		return;
	}

	auto drop_index_query = dynamic_cast<DropIndexQuery *>(query);
	if (drop_index_query)
	{
		bool b = api->drop_index(drop_index_query->index_name);

		if (b)
		{
			cout << "Drop Index Query OK";
		}
		else
		{
			cout << "Drop Index Query failed";
		}
		delete drop_index_query;
		query = nullptr;
		return;
	}

	auto quit = dynamic_cast<QuitQuery *>(query);
	{
		isQuit = true;
		return;
	}
	return;
}

//这里简单假设输入文件一行一句代码
void Interpreter::exec_file(const string &file_name)
{
	ifstream file(file_name);

	if (!file.is_open())
	{
		cerr << "File not found!" << endl;
		return;
	}
	cout << "Executing SQL file: " << file_name << endl;

	string statement, temp;
	vector<string> words;
	QueryRequest *ptrQuery = nullptr;

	while (file >> temp)
	{
		//file >> temp;
		if (temp == "")
			continue;
		while (temp.size() >= 1 && temp[temp.size() - 1] == ' ')
		{
			temp.pop_back();
		}
		statement += temp;

		if (temp[temp.size() - 1] != ';')
		{
			statement += " ";
		}
		else if (temp[temp.size() - 1] == ';')
		{
			statement.pop_back(); //delete ';'
			statement += " ";

			for (auto &s : words)
			{
				s.pop_back();
			}

			for (sregex_iterator it(statement.begin(), statement.end(), r), end_it; it != end_it; ++it)
			{
				words.push_back(it->str());
			}

			for (auto &s : words)
			{
				s.pop_back();
			}

			try
			{
				ptrQuery = parse(words);
			}
			catch(out_of_range)
			{
				cerr << "Not a valid input" << endl;
				words.clear();
				statement = temp = "";
				continue;
			}
			executeAndCountTime(ptrQuery);

			words.clear();
			statement = temp = "";
		}
	}
}

void print(char c, int num)
{
	for (int i = 0; i < num; ++i)
	{
		cout << c;
	}
}

void printAttrName(vector<string>& name, vector<int>& size)
{
	int num = name.size();
	cout << '+';
	for (int i = 0; i < num; ++i)
	{
		print('-', size[i] + 2);
		cout << '+';
	}
	cout << '\n';

	cout << '|';
	for (int i = 0; i < num; ++i)
	{
		cout << ' ' << name[i] << " \t|";
	}
	cout << '\n';

	cout << '+';
	for (int i = 0; i < num; ++i)
	{
		print('-', size[i] + 2);
		cout << '+';
	}
	cout << '\n';
}

void printARecord(char* record, vector<pair<TYPE, int>>& dataType)
{
	int intValue;
	float floatValue;
	string s;
	int sizeOffset;

	for (int i = 0; i < dataType.size(); ++i)
	{
		sizeOffset = 0;
		for (int j = 0; j < i; j++)
			sizeOffset += dataType[j].second;
		cout << '|';
		switch (dataType[i].first)
		{
		case TYPE::INT:
			memcpy(reinterpret_cast<char*>(&intValue), record + sizeOffset, sizeof(int));
			cout << ' ' << intValue << "\t|";
			break;
		case TYPE::FLOAT:
			memcpy(reinterpret_cast<char*>(&floatValue), record + sizeOffset, sizeof(float));
			cout << ' ' << floatValue << "\t|";
			break;
		case TYPE::VARCHAR:
			char* varcharValue = new char[dataType[i].second];
			memcpy(varcharValue, record + sizeOffset, dataType[i].second);
			s = string(varcharValue);
			cout << ' ' << s << "\t|";
			delete[] varcharValue;
			break;
		}
	}
	cout << '\n';

	return;
}
/*
void printARecord(char * record, vector<pair<TYPE, int>>& dataType)
{
	char *ptr = record;
	void *value = (void*)malloc(256);
	int *intValue = (int*)malloc(sizeof(int));
	float *floatValue = (float*)malloc(sizeof(float));
	char *varcharValue = (char*)malloc(256);
	string s;

	for (int i = 0; i < dataType.size(); ++i)
	{
		cout << '|';
		memcpy(value, ptr, dataType[i].second);
		ptr += dataType[i].second;
		switch (dataType[i].first)
		{
		case TYPE::INT:
			intValue = (int*)value;
			cout << ' ' << *intValue << '\t|';
			break;
		case TYPE::FLOAT:
			floatValue = (float*)value;
			cout << ' ' << *floatValue << '\t|';
			break;
		case TYPE::VARCHAR:
			varcharValue = (char*)value;
			varcharValue[dataType[i].second] = '\0';
			s = string(varcharValue);
			cout << ' ' << s << '\t|';
			break;
		}
	}
	cout << '\n';

	delete value;
	//delete intValue;
	//delete floatValue;
	//delete varcharValue;

	return;
}
*/