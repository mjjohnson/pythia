
/*
 * Copyright 2011, Pythia authors (see AUTHORS file).
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 
 * 1. Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * 
 * 2. Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * 
 * 3. Neither the name of the copyright holder nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
 * FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
 * COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "operators.h"
#include "operators_priv.h"

#include "../util/numaallocate.h"
#include "../util/numaasserts.h"

#ifdef ENABLE_NUMA
#include <numa.h>
#else
#include <iostream>
using std::cerr;
using std::endl;
#endif

using std::make_pair;

void CubeOp::init(libconfig::Config& root, libconfig::Setting& cfg)
{
	Operator::init(root, cfg);

	// Read aggregation fields. This is a list of numbers (for cubing on
	// all of the fields listed).
	//
	libconfig::Setting& field = cfg["fields"];
	dbgassert(field.isAggregate());
	for (int i=field.getLength()-1; i>=0; --i)
	{
	    vector<unsigned short> fields;
	    for (int j=0; j<=i; ++j)
	    {
		int fieldno = field[j];
		fields.push_back(fieldno);
	    }
	    aggfields.push_back(fields);
	}

	// Read user-defined schema.
	//
	Schema& uds = foldinit(root, cfg);
	dbgassert(uds.columns() != 0);

	// Get data type of aggregation attributes, then add user-defined schema.
	//
	for (unsigned int i=0; i<aggfields[0].size(); ++i)
	{
		ColumnSpec cs = nextOp->getOutSchema().get(aggfields[0][i]);
		schema.add(cs);
	}
	for (unsigned int i=0; i<uds.columns(); ++i)
	{
		schema.add(uds.get(i));
	}

	// Create object for comparisons.
	//
	vector <unsigned short> tempvec;	// [0,1,2,...]
	for (unsigned int i=0; i<aggfields[0].size(); ++i)
	{
		tempvec.push_back(i);
	}
	comparator.init(schema, nextOp->getOutSchema(), tempvec, aggfields[0]);

	assert(aggregationmode == Unset);

	hashfn = TupleHasher::create(nextOp->getOutSchema(), cfg["hash"]);
	if (cfg.exists("global"))
	{
		aggregationmode = Global;

		int thr = cfg["threads"];
		threads = thr;

		vector<char> allocpolicy;

#ifdef ENABLE_NUMA
		int maxnuma = numa_max_node() + 1;

		// Stripe on all NUMA nodes.
		//
		for (char i=0; i<maxnuma; ++i)
		{
			allocpolicy.push_back(i);
		}
#else
		cerr << " ** NUMA POLICY WARNING: Memory policy is ignored, "
			<< "NUMA disabled at compile." << endl;
#endif

		vector<HashTable> hashtable;
		hashtable.push_back( HashTable() );
		hashtables.push_back( hashtable );
		hashtables[0][0].init(
			hashfn.buckets(),	// number of hash buckets
			schema.getTupleSize()*4, // space for each bucket
			schema.getTupleSize(),	 // size of each tuple
			allocpolicy,			 // stripe across all
			this);

		barrier.init(threads);
	}
	else
	{
		aggregationmode = ThreadLocal;
		for (int i=0; i<MAX_THREADS; ++i) 
		{
			vector<HashTable> hashtable;
			hashtable.push_back( HashTable() );
			hashtables.push_back( hashtable );
		}
	}

	for (int i=0; i<MAX_THREADS; ++i) 
	{
		output.push_back(NULL);
		state.push_back( State(HashTable::Iterator()) );
	}
}

void CubeOp::threadInit(unsigned short threadid)
{
	void* space = numaallocate_local("GnAg", sizeof(Page), this);
	output[threadid] = new(space) Page(buffsize, schema.getTupleSize(), this);
	switch(aggregationmode)
	{
		case ThreadLocal:
			for (int i=0; i<hashtables[threadid].size(); i++) {
				hashtables[threadid][i].init(
					hashfn.buckets(),	// number of hash buckets
					schema.getTupleSize()*4, // space for each bucket
					schema.getTupleSize(),	 // size of each tuple
					vector<char>(),	  // always allocate locally
					this);
				hashtables[threadid][0].bucketclear(0, 1);
				}
			break;

		case Global:
			for (int i=0; i<hashtables[0].size(); i++) {
				hashtables[0][i].bucketclear(threadid, threads);
				barrier.Arrive();
			}
			break;

		default:
			throw NotYetImplemented();
	}
	unsigned short htid = 0;
	if (aggregationmode == ThreadLocal)
	{
		htid = threadid;
	}
	state[threadid] = State(hashtables[htid][0].createIterator());
}

void CubeOp::threadClose(unsigned short threadid)
{
	if (output[threadid]) {
		numadeallocate(output[threadid]);
	}
	output[threadid] = NULL;

	switch(aggregationmode)
	{
		case ThreadLocal:
			for (int i=0; i<hashtables[threadid].size(); i++) {
				hashtables[threadid][i].bucketclear(0, 1);
				hashtables[threadid][i].destroy();
			}
			break;

		case Global:
			barrier.Arrive();
			for (int i=0; i<hashtables[0].size(); i++) {
				hashtables[0][i].bucketclear(threadid, threads);
			}
			break;

		default:
			throw NotYetImplemented();
	}
}

void CubeOp::destroy()
{
	if (aggregationmode == Global) {
		for (int i=0; i<hashtables[0].size(); i++) {
			hashtables[0][i].destroy();
		}
	}
	hashfn.destroy();
	for (int i=0; i<hashtables.size(); i++) {
		hashtables[i].clear();
	}
	hashtables.clear();
	aggregationmode = Unset;
}

void CubeOp::remember(void* tuple, HashTable::Iterator& it, unsigned short htid)
{
	void* candidate;
	int totalaggfields = aggfields[0].size();
	Schema& inschema = nextOp->getOutSchema();

	// Identify key and hash it to find the hashtable bucket.
	//
	unsigned int h = hashfn.hash(tuple);
	if (aggregationmode == Global)
	{
		hashtables[htid][0].lockbucket(h);
	}
	hashtables[htid][0].placeIterator(it, h);

	// Scan bucket.
	//
	while ( (candidate = it.next()) ) 
	{
		// Compare keys of tuple stored in hash chain with input tuple.
		// If match found, increment count and return immediately.
		//
		if (comparator.eval(candidate, tuple)) {
			fold(schema.calcOffset(candidate, totalaggfields), tuple);
			goto unlockandexit;
		}
	}

	// If no match found on hash chain, allocate space and add tuple.
	//
	candidate = hashtables[htid][0].allocate(h, this);
	for (int i=0; i<totalaggfields; ++i)
	{
		schema.writeData(candidate, i, inschema.calcOffset(tuple, aggfields[0][i]));
	}
	foldstart(schema.calcOffset(candidate, totalaggfields), tuple);

unlockandexit:
	if (aggregationmode == Global)
	{
		hashtables[htid][0].unlockbucket(h);
	}
}

Operator::ResultCode CubeOp::scanStart(unsigned short threadid,
		Page* indexdatapage, Schema& indexdataschema)
{
	// Read and aggregate until source depleted.
	//
	Page* in;
	Operator::GetNextResultT result; 
	unsigned short htid = 0;
	if (aggregationmode == ThreadLocal)
	{
		htid = threadid;
	}
	HashTable::Iterator htit = hashtables[htid][0].createIterator();
	ResultCode rescode;
	
	rescode = nextOp->scanStart(threadid, indexdatapage, indexdataschema);
	if (rescode != Operator::Ready) {
		return rescode;
	}

	do {
		result = nextOp->getNext(threadid);

		in = result.second;

		Page::Iterator it = in->createIterator();
		void* tuple;
		while ( (tuple = it.next()) ) {
			remember(tuple, htit, htid);
		}
	} while(result.first == Operator::Ready);

	rescode = nextOp->scanStop(threadid);

#ifdef ENABLE_NUMA
	unsigned int maxnuma = numa_max_node() + 1;
#else
	unsigned int maxnuma = 1;
#endif

	switch (aggregationmode)
	{
		case ThreadLocal:
			state[threadid].bucket = 0;
			break;

		case Global:
			{
			if (threads > maxnuma)
			{
				// Threads from this NUMA node participating 
				//
				unsigned int participants = threads/maxnuma;
				if ((threadid % maxnuma) < (threads % maxnuma))
				{
					participants++;
				}

				// Compute offset
				//
				unsigned int startoffset = threadid % maxnuma;
				startoffset += (threadid/maxnuma) 
					* (((hashfn.buckets()/maxnuma)/participants)*maxnuma);

				state[threadid].bucket = startoffset;
			}
			else
			{
				state[threadid].bucket = threadid;
			}

			barrier.Arrive();

			break;
			}

		default:
			throw NotYetImplemented();
	}
	state[threadid].startoffset = state[threadid].bucket;


	unsigned int step;
	switch (aggregationmode)
	{
		case Global:
			step = (threads > maxnuma) ? maxnuma : threads;
			break;

		default:
			step = 1;
			break;
	}
	state[threadid].step = step;


	unsigned int hashbuckets = hashfn.buckets();
	unsigned int endoffset;
	if (aggregationmode == Global)
	{
		if (threadid >= (threads - maxnuma))
		{
			endoffset = hashbuckets;
		}
		else
		{
			// Threads from this NUMA node participating 
			//
			unsigned int participants = threads/maxnuma;
			if ((threadid % maxnuma) < (threads % maxnuma))
			{
				participants++;
			}

			// Compute offset
			//
			endoffset = (threadid % maxnuma);
			endoffset += ((threadid+maxnuma)/maxnuma)
						* (((hashbuckets/maxnuma)/participants)*maxnuma);
		}
	}
	else
	{
		endoffset = hashbuckets;
	}
	state[threadid].endoffset = endoffset;


	hashtables[htid][0].placeIterator(state[threadid].iterator, state[threadid].bucket);

	// If scan failed, return Error. Otherwise return what scanClose returned.
	//
	return ((result.first != Operator::Error) ? rescode : Operator::Error);
}

Operator::GetNextResultT CubeOp::getNext(unsigned short threadid)
{
	void* tuple;

	// Restore iterator from saved state.
	//
	HashTable::Iterator& it = state[threadid].iterator;

	Page* out = output[threadid];
	out->clear();

	unsigned short htid = 0;
	if (aggregationmode == ThreadLocal)
	{
		htid = threadid;
	}

	const unsigned int endoffset = state[threadid].endoffset;
	const unsigned int step = state[threadid].step;

	for (unsigned int i=state[threadid].bucket; i<endoffset; i+=step)
	{
		// Output aggregates, page-by-page.
		//
		while ( (tuple = it.next()) ) 
		{
#ifdef DEBUG2
#ifdef ENABLE_NUMA
#warning Hardcoded NUMA socket number.
			unsigned int maxnuma = 4;
// Disabled for perf.
//			unsigned int maxnuma = numa_max_node() + 1;
#else
			unsigned int maxnuma = 1;
#endif
			if (threads >= maxnuma)
			{
				assertaddresslocal(tuple);
			}
#endif

			// Do copy to output buffer.
			//
			void* dest = out->allocateTuple();
			dbgassert(out->isValidTupleAddress(dest));
			schema.copyTuple(dest, tuple);

			// If output buffer full, record state and return.
			// 
			if (!out->canStoreTuple()) {
				state[threadid].bucket = i; 
				return make_pair(Ready, out);
			}
		}

		// Making iterator wrap-around so as to avoid past-the-end placement.
		//
		if ((i+step) < endoffset)
		{
			hashtables[htid][0].placeIterator(it, (i+step));
		}
		else
		{
			hashtables[htid][0].placeIterator(it, 0);
		}
	}

	return make_pair(Finished, out); 
}

vector<unsigned int> CubeOp::statAggBuckets()
{
	vector<unsigned int> ret;

	for (unsigned int i=0; i<hashtables[0].size(); ++i)
	{
		vector<unsigned int> htstats = hashtables[0].at(i).statBuckets();
		const unsigned int htstatsize = htstats.size();

		if (htstatsize >= ret.size())
			ret.resize(htstatsize, 0);

		for (unsigned int j=0; j<htstatsize; ++j)
		{
			ret.at(j) += htstats.at(j);
		}
	}

	return ret;
}

Schema& CubeOp::foldinit(libconfig::Config& root, libconfig::Setting& cfg)
{
	sumfieldno = cfg["sumfield"];
	inschema = nextOp->getOutSchema();

	// Assert we sum numeric types.
	//
	assert(inschema.getColumnType(sumfieldno) == CT_DECIMAL
			|| inschema.getColumnType(sumfieldno) == CT_INTEGER
			|| inschema.getColumnType(sumfieldno) == CT_LONG);

	aggregateschema = Schema();
	aggregateschema.add(inschema.getColumnType(sumfieldno));
	return aggregateschema;
}

void CubeOp::foldstart(void* output, void* tuple)
{
	switch (aggregateschema.getColumnType(0))
	{
		case CT_INTEGER:
		{
			CtInt val = inschema.asInt(tuple, sumfieldno);
			*(CtInt*)output = val;
			break;
		}
		case CT_LONG:
		{
			CtLong val = inschema.asLong(tuple, sumfieldno);
			*(CtLong*)output = val;
			break;
		}
		case CT_DECIMAL:
		{
			CtDecimal val = inschema.asDecimal(tuple, sumfieldno);
			*(CtDecimal*)output = val;
			break;
		}
		default:
			throw NotYetImplemented();
			break;
	};
}

void CubeOp::fold(void* partialresult, void* tuple)
{
	switch (aggregateschema.getColumnType(0))
	{
		case CT_INTEGER:
		{
			CtInt nextval = inschema.asInt(tuple, sumfieldno);
			*(CtInt*)partialresult += nextval;
			break;
		}
		case CT_LONG:
		{
			CtLong nextval = inschema.asLong(tuple, sumfieldno);
			*(CtLong*)partialresult += nextval;
			break;
		}
		case CT_DECIMAL:
		{
			CtDecimal nextval = inschema.asDecimal(tuple, sumfieldno);
			*(CtDecimal*)partialresult += nextval;
			break;
		}
		default:
			throw NotYetImplemented();
			break;
	};
}

