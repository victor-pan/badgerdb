/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"
#include <cstring>
namespace badgerdb { 

int NumBufs = 0;//static variable for iteration.
int HTsize = 0;//static variable for ht size and destructor.

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

	HTsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (HTsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
  NumBufs = bufs;
}

/**
Clean up the class; delete dynamically allocated memory.
*/
// used to delete a single column
// of the ht
/*void deleteBuckChain(hashBucket *tmpBuck)
{
	//check for invalid input
	if (tmpBuck == NULL) return;

	hashBucket *this_buck = tmpBuck;
	//don't hurt my c oriented brain by writing
	// `hashBucket* prev,next`
	hashBucket* prev;
//	hashBucket* next;...actually, dont need this.

	while (this_buck->next)
	{
		prev = this_buck;
		this_buck = this_buck->next;

		//and delete it
		delete prev;
	}
	if (this_buck) delete this_buck;
}*/

/**
destructor
*/
BufMgr::~BufMgr() {
	delete [] bufDescTable;
	delete [] bufPool;
	// ht destructor will be called for you.
	// ht requires special handling
	//hashTable.~BufHashTbl();
	/*for (int i=0; i< HTsize; i++)
	{
		deleteBuckChain(&hashTable[i]);
	}*/
}

/**
Iterate the clockHand with wraparound
*/
void BufMgr::advanceClock()
{
	//the clockhand starts at the last
	//position, so first iteration
	//will advance to position 0
	clockHand = (clockHand+1) % numBufs;
}

/**
Allocate a frame using the clock algorithm
Can write a page back to disk
If all frames pinned, throws BufferExceeded
Exception
This is a private method
*/
void BufMgr::allocBuf(FrameId & frame) 
{
	int numPinned=0;//If all frames pinned, can't proceed.
//	int bufs=sizeof (bufDescTable);
int bufs = NumBufs;
	for (int i=0; i < bufs; i++) {
		if (bufDescTable[i].pinCnt > 0)
			numPinned++;
	}
	if (numPinned == bufs) {
		throw BufferExceededException();
	}

	//find a free frame
	BufDesc* tmpbuf;
	int numScanned = 0;
	bool found = 0;
	while (numScanned < 2*bufs)
	{
		advanceClock();
		tmpbuf = &bufDescTable[clockHand];
		// if invalid, use frame
		if (! tmpbuf->valid )
		{
			break;
		}

		// if invalid, check referenced bit
		if (! tmpbuf->refbit)
		{
			// check to see if someone has it pinned
			if (tmpbuf->pinCnt == 0)
			{
				// no one has it pinned, so use it.

				hashTable->remove(tmpbuf->file,tmpbuf->pageNo);
				// increment pin and set referenced
				found = true;
				break;
			}
		}
		else
		{
			// has been referenced, clear bit and continue
			tmpbuf->refbit = false;
		}

	}

	// throw if buffer has no available slots
	if (!found && numScanned >= 2*bufs)
	{
		throw BufferExceededException();
	}

	// flush page changes to disk
	if (tmpbuf->dirty)
	{
		tmpbuf->file->writePage(bufPool[clockHand]);
	}

	// return frame number
	frame = clockHand;
}


/**
Given a file object and page number, will return addr of page in buf pool
Caller won't know if it had to be io'd
*/
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
	FrameId frameNo=0;

	try
	{
		//find this page in the existing buffer pool

		hashTable->lookup(file, pageNo, frameNo);//got frame no

		bufDescTable[frameNo].refbit = true;
		bufDescTable[frameNo].pinCnt++;
		page=&bufPool[frameNo];//ret val

	}
	catch (HashNotFoundException)
	{
		//allocate new space for this file and page.

		//io pg first which tests file and pg for validity
		//we'll let the InvalidPageException to percolate up.
		Page p;
		p = file->readPage(pageNo);//io pg

		//Valid page:
		BufMgr::allocBuf(frameNo);//get frameno
		bufPool[frameNo] = p;//store page
		hashTable->insert(file, pageNo, frameNo);//know where it is
		
		//set new frame up
		bufDescTable[frameNo].Set(file, pageNo);
		page = &bufPool[frameNo];//ret val
	}
}

/*
Given the file and pageNo, will unpin the page and set dirty
If the file and page isn't in the buffer pool, throw HashNotFoundException
*/
void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
	FrameId frameNo = 0; 
	hashTable->lookup(file, pageNo, frameNo);
/*	try //find the page
	{
		hashTable->lookup(file, pageNo, frameNo);
	}
	catch (HashNotFoundException)
	{
		throw HashNotFoundException(file->filename, pageNo);
		//return;//silently quit if page not found
	}*/

	//Check input
	if (bufDescTable[frameNo].pinCnt == 0)
	{
		throw PageNotPinnedException(file->filename(),pageNo,frameNo);
	}


	( bufDescTable[frameNo].pinCnt )--;//unpin

	if (dirty)
	{
		bufDescTable[frameNo].dirty = true;
	}


}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
	Page oPg = file->allocatePage();
	

	FrameId frameNo = 0;
	allocBuf(frameNo);


	pageNo = oPg.page_number();
	hashTable->insert(file, pageNo, frameNo); //record that we have this
	bufDescTable[frameNo].Set(file, pageNo); //record this usage in our buffer pool.

//	memcpy((void*) &bufPool[frameNo], (void*) &oPg, sizeof(Page));
	bufPool[frameNo] = oPg;
	page = &bufPool[frameNo];

}

/**
Deletes a particular page from
a file. If there was a frame,
clears the frame and hashtable first
*/
void BufMgr::disposePage(File* file, const PageId pageNo)
{
	FrameId frameNo=0;
	hashTable->lookup(file, pageNo, frameNo);//don't handle exception

	// if execution reaches this point, the page is in the buffer pool
	bufDescTable[frameNo].Clear();//clear frame
	hashTable->remove(file, pageNo);//forget this page
	file->deletePage(pageNo);//delete
}

/**
Writes the file to the disk.
Iterates over all page frames, flushing the (dirty)
ones that belong to the file to disk.
*/
void BufMgr::flushFile(const File* file) 
{

	BufDesc* tmpbuf;
	for (std::uint32_t i=0; i < numBufs; i++)
	{
	        tmpbuf = &(bufDescTable[i]);

		// make sure we have a page in correct file
		bool invalid = !tmpbuf->valid;
		bool correctFile = (tmpbuf->file == file);
		if (!correctFile) {
			continue;
		}


		// Correct file, proceed
		// buffer can't be invalid and have a file
		if (invalid) {
			throw BadBufferException(tmpbuf->frameNo,
						tmpbuf->dirty,
						tmpbuf->valid,
						tmpbuf->refbit);
		}


		//valid and correct
		// Good to go write this page.
		
		if (tmpbuf->pinCnt > 0) //Can't write a pinned page!
		{
			throw PagePinnedException(
				file->filename(),
				tmpbuf->pageNo,
				tmpbuf->frameNo);
		}

		if (tmpbuf->dirty)
		{
			FrameId frameNo = tmpbuf->frameNo;
			File* f = bufDescTable[frameNo].file;
			f->writePage(bufPool[frameNo]);
			tmpbuf->dirty = false;
		}
		hashTable->remove(file, tmpbuf->pageNo);//no longer keep page
		bufDescTable[i].Clear();//clear buffer frame
	}


}



void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
