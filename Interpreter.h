//
// Created by csq on 6/10/19.
//
#pragma once

#ifndef INTERPRETER_H
#define INTERPRETER_H

#include <iostream>
#include <string>
#include <iomanip>
#include <cstring>
#include <vector>
#include <regex>
#include <ctime>
#include <stdexcept>
#include "QueryRequest.h"
#include "API.h"

using namespace std;

class Interpreter
{
public:
	API *api;
	string pattern;
	regex r;
	CatalogManager *cm;
	bool isQuit;

    void main_loop (istream& input);
	void executeAndCountTime(QueryRequest*);
    void execute (QueryRequest *);
	void exec_file(const string &file_name);

	Interpreter();
	~Interpreter();
};

void print(char c, int num);
void printAttrName(vector<string>&, vector<int>&);
void printARecord(char *, vector<pair<TYPE, int>>&);

#endif
