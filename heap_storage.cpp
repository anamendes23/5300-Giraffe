#include "heap_storage.h"
#include <cstring>
#include <bitset>
#include <iostream>
#include "db_cxx.h"

using namespace std;

typedef u_int16_t u16;

/* -------------SlotttedPage::DbBlock-------------*/
SlottedPage::SlottedPage(Dbt& block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new) {
    if (is_new) {
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
         put_header();
    }
    else {
         get_header(this->num_records, this->end_free);
    }
}

// Add a new record to the block. Return its id.
RecordID SlottedPage::add(const Dbt* data) {
    if (!has_room(data->get_size()))
        throw DbBlockNoRoomError("not enough room for new record");

    u16 id = ++this->num_records;
    u16 size = (u16)data->get_size();
    this->end_free -= size;
    u16 loc = this->end_free + 1;
    put_header();
    put_header(id, size, loc);
    memcpy(this->address(loc), data->get_data(), size);
    return id;
}


// Get an exisiting record by record id.
Dbt* SlottedPage::get(RecordID record_id) {
    u16  size, loc;
    get_header(size, loc, record_id);

    if (loc == 0) {
        return nullptr;
    }
  
   return new Dbt(this->address(loc), loc + size);
}

void SlottedPage::del(RecordID record_id) {
    u16  size, loc;
    get_header(size, loc, record_id);

    put_header(record_id, 0, 0);
    slide(loc, loc+size);
}

void SlottedPage::put(RecordID record_id, const Dbt &data) {

    u16 size, loc;
    get_header(size, loc, record_id);
    u16 new_size = (u16)data.get_size();

    if (new_size > size) {
        u16 extra_size = new_size - size;

        if (!has_room(extra_size)) {
            throw new DbBlockNoRoomError("Not enough room in block");
        }

        slide(loc + new_size, loc + size);
        memcpy(this->address(loc - extra_size), data.get_data(), new_size);
    }
    else {
        memcpy(this->address(loc), data.get_data(), new_size);
        slide(loc + new_size, loc + size);
    }

    get_header(size, loc, record_id);
    put_header(record_id, new_size, loc);
}

RecordIDs* SlottedPage::ids(void) {
    RecordIDs* record_ids = new RecordIDs;
    u16 size, loc;

    for (u16 i = 1; i <= this->num_records; i++) {
        get_header(size, loc, i);

        if (loc != 0) {
            record_ids->push_back(i);
        }
    }
    return record_ids;
}

bool SlottedPage::has_room(u16 size) {
    u16 available = this->end_free - (this->num_records + 1) * 4;
    return size <= available;
}

void SlottedPage::get_header(u16 &size, u16 &loc, RecordID record_id) {
    size = get_n(4 * record_id);
    loc = get_n(4 * record_id + 2);
}

// Store the size and offset for given id. For id of zero, store the block header.
void SlottedPage::put_header(RecordID id, u16 size, u16 loc) {
    if (id == 0) { // called the put_header() version and using the default params
        size = this->num_records;
        loc = this->end_free;
    }
    put_n(4 * id, size);
    put_n(4 * id + 2, loc);
}

void SlottedPage::slide(u16 start, u16 end) {
}
// Get 2-byte integer at given offset in block.
u16 SlottedPage::get_n(u16 offset) {
    return *(u16*)this->address(offset);
}

// Put a 2-byte integer at given offset in block.
void SlottedPage::put_n(u16 offset, u16 n) {
    *(u16*)this->address(offset) = n;
}

// Make a void* pointer for a given offset into the data block.
void* SlottedPage::address(u16 offset) {
    return (void*)((char*)this->block.get_data() + offset);
}

// test function -- returns true if all tests pass
bool test_heap_storage()
{
    char block[DbBlock::BLOCK_SZ];
    Dbt data(block, sizeof(block));
    SlottedPage slotted_page(data, 0, true);

    char hello[] = "hello";
    char bye[] = "bye";
    Dbt hello_data(hello, sizeof(hello));
    Dbt bye_data(bye, sizeof(bye));
    string expected = hello;

    RecordID id = slotted_page.add(&hello_data);

    if (id != 1) {
        return false;
    }
    
    Dbt* result_data = slotted_page.get(id);
    string actual = (char*)result_data->get_data();

    if (expected != actual) {
        return false;
    }

    RecordIDs* recordIds = slotted_page.ids();

    if (recordIds->size() != 1) {
        return false;
    }
    
    slotted_page.put(1, bye_data);
    result_data = slotted_page.get(id);
    expected = bye;
    actual = (char*)result_data->get_data();

    if (expected != actual) {
        return false;
    }

    slotted_page.del(id);

    recordIds = slotted_page.ids();

    if (recordIds->size() != 0) {
        return false;
    }

    return true;
}


/* Heap File*/

void HeapFile::create() {
    cout << "Inside HeapFile::create"<< endl;
    this->db_open(DB_CREATE);
    SlottedPage *slottedPage = this->get_new();
    delete slottedPage;
}

void HeapFile::close() {
    this->db.close(0);
    closed = true;
}

BlockIDs *HeapFile::block_ids() {
    BlockIDs *blockIDs = new BlockIDs();
    for(BlockID i = 1; i <= this->last; i++)
    {
        blockIDs->push_back(i);
    }
    return blockIDs;
}
void HeapFile::open() {
    this->db_open();
}

void HeapFile::db_open(uint flags) {
    cout << "HeapFile::db_open start"<< endl;
    cout << "flags"<<  flags << endl;
    cout << "closed "<<  this->closed << endl;

    if (this->closed) {
        this->db.set_re_len(DbBlock::BLOCK_SZ);
       /* const char *path = nullptr;
        _DB_ENV->get_home(&path);
        */
        cout << "dbfilename " << this->dbfilename << endl;
        this->dbfilename = "./" + this->dbfilename + ".db";
        this->db.open(nullptr, (this->dbfilename).c_str(), nullptr, DB_RECNO, flags, 0644);
        DB_BTREE_STAT *stat;
        this->db.stat(nullptr, &stat, DB_FAST_STAT);

        this->last = flags != 0 ? 0 : stat->bt_ndata;
        cout << "last " << this->last << endl;
        this->closed = false;
    }
    cout << "HeapFile::db_open end"<< endl;
}

void HeapFile::put(DbBlock *block) {
    BlockID blockId = block->get_block_id();
    cout << "blockId" << blockId << endl;
    //Dbt block(&blockId,sizeof(blockId));
    //this->db.put(nullptr, &block, block->get_block(),0);
}


void HeapFile::drop() {

}

SlottedPage *HeapFile::get(BlockID block_id) {

    SlottedPage *slottedPage = NULL;
    return slottedPage;
}


SlottedPage *HeapFile::get_new() {
    cout << "HeapFile::get_new start"<< endl;
    char block[SlottedPage::BLOCK_SZ];
    Dbt data(block, sizeof(block));
    cout << "block "<< block << endl;
    cout << "block sizeof "<< sizeof(block) << endl;
    int block_number;
    Dbt key(&block_number, sizeof(block_number));
    block_number = this->last + 1;
    this->last++;
    cout << "block_number"<< block_number << endl;
    SlottedPage *slottedPage = new SlottedPage(data, this->last, true);
//    this->db.put(NULL, &key, &data, 0);
    cout << "HeapFile::get_new end"<< endl;
    return  slottedPage;
}


// test function -- returns true if all tests pass
bool test_heap_file()
{
    cout << "test_heap_file called: " << endl;
    HeapFile heapFile("sample_sonali");
    cout << "test_heap_file ::  heapFile initialised and db created" << endl;
    //heapFile.create();
    //cout << "\n test_heap_file heapFile create() called" << endl;
    //heapFile.close();
    heapFile.open();
    SlottedPage *slottedPage = heapFile.get_new();
    heapFile.put(slottedPage);
    heapFile.close();
    cout << "close called";
    return true;
}