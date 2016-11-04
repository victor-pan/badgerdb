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

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

	int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}

/**
 *  Clean up the class; delete dynamically allocated memory.
*/
BufMgr::~BufMgr() {
	delete [] bufDescTable;
	delete [] bufPool;
	delete [] hashTable;
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
	int bufs=sizeof (bufDescTable);
	for (int i=0; i < bufs; i++) {
		if (bufDescTable[i].pinCnt > 0)
			numPinned++;
	}
	if (numPinned == bufs) {
		throw BufferExceededException();
	}

	//find a free frame
	BufDesc* tmpbuf;
	for (int i=0; i < bufs*2; i++)
	{
		advanceClock();
		tmpbuf = &bufDescTable[clockHand];
		if ( !tmpbuf->valid ) { //Use this; done.
			tmpbuf->Set(NULL,0);
			/*tmpbuf->Set(NULL, Page::INVALID_NUMBER);//Don't have/need file/pageNo now*/
			frame = tmpbuf->frameNo;//return frame no.
			return;
		} else if (tmpbuf->refbit) {

			tmpbuf->refbit = false;
			continue;
		} else if (tmpbuf->pinCnt > 0) {
			continue;
		} else {
			if (tmpbuf->dirty) {
				FrameId frameNo = tmpbuf->frameNo;
				tmpbuf->file->writePage(bufPool[frameNo]);
			}

			tmpbuf->Set(NULL,0);
			frame = tmpbuf->frameNo;//ret val
		}
	}




/*		if (tmpbuf->pinCnt > 0) { // buffer in use
			++numPinned;
			continue;
		} else if (tmpbuf->refbit) {//buffer recently used
			tmpbuf->refbit = false;
			continue;
		} else { //means this is a free, old frame
			if (tmpbuf->dirty) {//write to disk
				FrameId frameNo = tmpbuf->frameNo;
				tmpbuf->file->writePage(bufPool[frameNo]);
			}
			tmpbuf->Set(NULL, Page::INVALID_NUMBER);//Don't have/need file/pageNo now
			frame = tmpbuf->frameNo;
		}*/

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
		hashTable->lookup(file, pageNo, frameNo);//got frame no

		bufDescTable[frameNo].refbit = true;
		bufDescTable[frameNo].pinCnt++;
		page=&bufPool[frameNo];//ret val

	}
	catch (HashNotFoundException)
	{
		BufMgr::allocBuf(frameNo);//get frameno

		bufPool[frameNo] = file->readPage(pageNo);//io pg
		hashTable->insert(file, pageNo, frameNo);//know where it is
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

		bool invalid = !tmpbuf->valid;
		bool correctFile = (tmpbuf->file == file);
		if (!correctFile) {
			continue;
		}


		// Correct file
		if (invalid) {
			throw BadBufferException(tmpbuf->frameNo,
						tmpbuf->dirty,
						tmpbuf->valid,
						tmpbuf->refbit);
		}


		//valid and correct
		// Good to go write this page.
		
		if (tmpbuf->pinCnt != 0) //Can't write a pinned page!
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
