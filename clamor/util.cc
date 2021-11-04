#include "util.h"

#include <cassert>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <utility>

using namespace std;

string Util::read( string infile ) {
  ifstream data( infile );

  assert ( data.good() );

  string read = "";
  char next;
  while ( data.good() and data.get( next ) ) {
    read += next;
  }

  return read;
}

vector<string> Util::readlines( string infile ) {
  ifstream data( infile );

  assert ( data.good() );

  vector<string> lines;
  string next_line = "";
  while ( data.good() and getline( data, next_line ) ) {
    lines.push_back( next_line );
  }

  return lines;
}

vector<string> Util::readlines_from_char( const char* input ) {
  string str_input( input );
  istringstream data( str_input );

  assert ( data.good() );

  vector<string> lines;
  string next_line = "";
  while ( data.good() and getline( data, next_line ) ) {
    if ( next_line.length() > 0 ) lines.push_back( next_line );
  }

  return lines;
}

vector<string> Util::readlines_strip_empty( string infile ) {
  ifstream data( infile );

  assert ( data.good() );

  vector<string> lines;
  string next_line = "";
  while ( data.good() and getline( data, next_line ) ) {
    if ( next_line.length() > 0 ) lines.push_back( next_line );
  }

  return lines;
}

vector<string> Util::split( char* input, char* delimiter ) {
  vector<string> ret;

  char* tok = strtok(input, delimiter);
  while ( tok != NULL ) {
    ret.push_back(tok);
    tok = strtok(NULL, delimiter);
  }

  return ret;
}

vector<string> Util::split(string input, char delimiter) {
  vector<string> ret;
  istringstream data(input);
  string s;
  while (getline(data, s, delimiter)) {
    ret.push_back(s);
  }
  
  return ret;
}

string Util::join( vector<string> vec, string delimiter ) {
  string res = "";
  for ( unsigned int i = 0; i < vec.size() - 1; i++ ) {
    res += vec[i] + delimiter;
  }
  res += vec[vec.size() - 1];
  return res;
}

string Util::join( pair<string, string> vec, string delimiter ) {
  return vec.first + delimiter + vec.second;
}

vector<size_t> Util::shard_units(size_t n_elements, uint32_t nshards) {
  return shard(n_elements, nshards, 1);
}

vector<size_t> Util::shard(size_t data_size, uint32_t nshards, size_t increment) {
  vector<size_t> ret;
  for ( uint32_t i = 0; i < nshards; i++ ) { ret.push_back(0); }
  
  size_t rem = data_size;
  uint32_t i = 0;
  while ( rem >= increment ) {
    ret[i] += increment;
    rem -= increment;
    i = (i+1) % nshards;
  }

  ret[(ret.size() - 1)] += rem;

  return ret;
}

uint64_t Util::time_diff_ms(const struct timeval & start_ts, const struct timeval & end_ts) {
  struct timeval diff;
  timersub(&end_ts, &start_ts, &diff);
  uint64_t millis = (diff.tv_sec * (uint64_t)1000) + (diff.tv_usec / 1000);
  return millis;
}
