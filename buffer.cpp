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
 *  Clean up the class; delete dynamically allocated memory.
*/
BufMgr::~BufMgr() {
	delete [] bufDescTable;
	delete [] bufPool;
	delete [] hashTable;
}

/**
 * Iterate the clockHand with wraparound
*/
void BufMgr::advanceClock()
{
	clockHand = (clockHand+1) % numBufs;
}

/**
 * If all buffer frames are pinned, throw an exception.
 * Otherwise, call advanceClock doing a full 2 sweeps
 *  (to allow unsetting the reference bit.)
 * The first frame that is not pinned, not recently referenced
 *  will be selected and initialized.
 *
 * @throws BufferExceededException
*/
void BufMgr::allocBuf(FrameId & frame) 
{
	// Check that all frames aren't pinned.
	int numPinned=0;
	int bufs = sizeof ( bufDescTable );
	for (int i=0; i < bufs; i++) {
		if (bufDescTable[i].pinCnt > 0)
			numPinned++;
	}
	if (numPinned == bufs) {
		throw BufferExceededException();
	}

	// Run the clock algorithm.
	BufDesc* tmpbuf;
	for (int i=0; i < bufs*2; i++) {
		advanceClock();
		tmpbuf = &bufDescTable[clockHand];
		if ( !tmpbuf->valid ) { // Use this; done.
			// Leave blank; will be set by readPage()
			tmpbuf->Set(NULL, Page::INVALID_NUMBER);
			return;
		} // look for free buffer.
		// skip pinned ones..
		if (  tmpbuf->pinCnt > 0 ) {
			++numPinned;
			continue;
		} // ..and referenced ones.
		else if ( tmpbuf->refbit ) {
			tmpbuf->refbit = false;
			continue;
		} // found a free one.
		else {
			if (tmpbuf->dirty) {
				// it has to have a page and file.
				FrameId frameNo = tmpbuf->frameNo;
				tmpbuf->file->writePage(bufPool[frameNo]);
			}
			frame = tmpbuf->frameNo;
			// Leave blank for ReadPage()
			tmpbuf->Set(NULL, Page::INVALID_NUMBER);
		}

		

	}


}

/**
 * Given a file object and page number, will return a page object
 *  in memory. Whether the page had to be read from disk or was already there
 *  will be transparent to the caller.
 *
 * If the read was successful, `page` will be populated with the page object.
*/

void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
	FrameId frameNo = -1;
	try {
		hashTable->lookup(file, pageNo, frameNo);
		if (frameNo == -1) { // not found
			return;
		}
		bufDescTable[frameNo].pinCnt++;
		bufDescTable[frameNo].refbit = true;
		page = &bufPool[frameNo];
	} catch (HashNotFoundException) {
		BufMgr::allocBuf(frameNo);
		hashTable->insert(file, pageNo, frameNo);
		bufDescTable[frameNo].Set(file, pageNo);
		File theFile = *file;
		*page = theFile.readPage(pageNo);
	}
}

/*
 Given the file and pageNo, will unpin the page and set dirty.
 If the file and page isn't in the buffer pool, throw fileexistexception
 or invalidpageexception.
*/
void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
	// Lookup the page.
	FrameId frameNo = -1;
	hashTable->lookup(file, pageNo, frameNo);
	if (frameNo == -1) {
		return; // Not found.
	}
	if ( bufDescTable[frameNo].pinCnt == 0 ) {
		throw PageNotPinnedException(
			file->filename(),
			pageNo,
			frameNo
		);
	}
	(bufDescTable[frameNo].pinCnt)--;
	if (dirty) bufDescTable[frameNo].dirty = true;
/*
// This is ok, but it is slow. Why iterate over the entire table when we can use a hash?

// Also, we don't need to throw an exception if not found...

	bool found = false;
	for (std::uint32_t i = 0; i < numBufs; i++)
	{
		BufDesc* tmpbuf = &(bufDescTable[i]);
		if (tmpbuf->file == file && tmpbuf->pageNo == pageNo)
		{
			if (tmpbuf->pinCnt == 0) {
				throw PageNotePinedException;
			}
			found = true;
			(tmpbuf->pinCnt)--;
			if (dirty) tmpbuf->dirtyBit = true;
			
		}
	}
	if (!found) {
		throw InvalidPageException();
	}
*/
}

/*
 * Writes the file to the disk.
 *
 * Iterates over all page frames, flushing the (dirty)
 *  ones that belong to the file to disk.
*/
void BufMgr::flushFile(const File* file) 
{
	BufDesc* tmpbuf;

	for (int i=0; i < numBufs; i++) {
	        tmpbuf = &(bufDescTable[i]);

		// no need to examine this
	        if ( !tmpbuf->valid || tmpbuf->file != file) {	
			// ERROR! Page can't belong to
			// a file if it's invalid!
			if (tmpbuf->file == file) {
				throw BadBufferException(
					tmpbuf->frameNo,
					tmpbuf->dirty,
					tmpbuf->valid,
					tmpbuf->refbit
				);
			}
			// We're good. Ignore this invalid buffer frame.
			else continue;

		}

		// ERROR! Cannot write a pinned page!
		if (tmpbuf->pinCnt != 0) {
			throw PagePinnedException(
				file->filename(),
				tmpbuf->pageNo,
				tmpbuf->frameNo
			);
		}
		// Implicit else: deal with this file's page.
		if (tmpbuf->dirty) {
			FrameId frameNo = tmpbuf->frameNo;
			File f = bufDescTable[frameNo].file;
			f->writePage(bufPool[frameNo]);
		}
		// Remove page from the hash table.
		hashTable->remove(file, tmpbuf->pageNo);

		// Reset the buffer frame.
		bufDescTable[i].Clear();
	}


}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
	*page = file->allocatePage();
	FrameId frameNo;
	allocBuf(frameNo);
	pageNo = page->page_number();

	hashTable->insert(file, pageNo, frameNo); // reference this page in our hash table.
	bufDescTable[frameNo].Set(file, pageNo); // record this usage in our buffer pool.

/*
// Where is the new buffer frame ID?
	page = &file->allocatePage();
	pageNo = page->page_number();
*/
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
// Free the buffer frame.
	FrameId frameNo = -1;
        hashTable->lookup(file, PageNo, frameNo);
	if (frameNo == -1) return;  // not found
	bufDescTable[frameNo].Clear();
	hashTable->remove(file, PageNo);
// Delete the page.
	// don't need to check valid since we're deleting.
	file->deletePage(PageNo);
/*
//Should check metadata before deleting the page.
//Should remove the hash table entry.
    file.deletePage(PageNo);
    for (std::uint32_t i = 0; i < numBufs; i++)
    {
      tmpbuf = &(bufDescTable[i]);
      if (!tmpbuf->valid)
        continue;
      if (tmpbuf->file == file && tmpbuf->pageNo == PageNo)
        tmpbuf->clear();
    }
*/
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
