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
 *  Clean up the class
*/
BufMgr::~BufMgr() {
	delete bufPool;
	delete bufDescTable;
	delete hashTable;
}

/**
 * Iterate the clockHand with wraparound
*/
void BufMgr::advanceClock()
{
	clockHand = (clockHand+1) % bufs;
}

/**
 * Call advanceClock looking for a free frame.
 * If none unpinned, will spin until one has a pinct of 0
*/
void BufMgr::allocBuf(FrameId & frame) 
{
	BufDesc* tmpbuf;
	advanceClock();
	if (!tmpbuf->valid) {
		// set frame
		return;
	} else if (tmpbuf->refbit) {
		tmpbuf->refbit = false;
		continue;
	} else if (tmpbuf->pinCnt > 0) {
		continue;
	} else {
		File* fileptr = tmpbuf->file;
		if (tmpbuf->dirty)
			fileptr->writePage(tmpbuf->pageNo);
		tmpbuf->set(fileptr, tmpbuf->pageNo);
	}
}

/**
 * Will hash into a page using `file` and `pageNo`.
 *   If not found, will read the page in (allocBuf), then return the page.
*/

void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
	FrameId frameNo = -1;
	hashTable.lookup(file, pageNo, frameNo);
	// Found the page in the buffer pool.
	if (frameId != -1)
		return;
	// Implicit else: read it into the buffer pool
	BufMgr::allocBuf(frameNo);
	page = file::readPage(pageNo);
}

/*
 Given the file and pageNo, will unpin the page and set dirty.
 If the file and page isn't in the buffer pool, throw fileexistexception
 or invalidpageexception.
*/
void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
	bool found = false;
	for (std::uint32_t i = 0; i < numBufs; i++)
	{
		tmpbuf = &(bufDescTable[i]);
		if (tmpbuf->file == file && tmpbuf->pageNo == pageNo)
		{
			found = true;
			(tmpbuf->pinCnt)--;
		}
	}
	if (!found)
		throw InvalidPageException();
}

/*
 Writes the file to the disk.
 Iterate over all page frames, flushing the dirty ones to disk.
*/
void BufMgr::flushFile(const File* file) 
{
  BufDesc* tmpbuf;
//      int validFrames = 0;

  for (std::uint32_t i = 0; i < numBufs; i++)
        {
        tmpbuf = &(bufDescTable[i]);

        if (tmpbuf->valid != true)
		continue; // no need to examine this
	if (tmpbuf->file != file)
		continue;
	if (tmpbuf->dirty)
		file.writePage(tmpbuf.pageNo);
  }

  //      std::cout << "Total Number of Valid Frames:" << validFrames << "\n";	
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
	page = &file->allocatePage();
	pageNo = page->page_number();
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
    file.deletePage(PageNo);
    for (std::uint32_t i = 0; i < numBufs; i++)
    {
      tmpbuf = &(bufDescTable[i]);
      if (!tmpbuf->valid)
        continue;
      if (tmpbuf->file == file && tmpbuf->pageNo == PageNo)
        tmpbuf->clear();
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
