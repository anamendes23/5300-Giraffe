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
HeapFile::HeapFile(std::string name) : DbFile(name), dbfilename(""), last(0), closed(true){
    const char *home = std::getenv("HOME");
    std::string envdir = std::string(home) + "/" + this->dbfilename;

    DbEnv env(0U);
    env.set_message_stream(&std::cout);
    env.set_error_stream(&std::cerr);
    env.open(envdir.c_str(), DB_CREATE | DB_INIT_MPOOL, 0);
    this->db(&env, 0);

}

void HeapFile::create() {
    cout << "Inside HeapFile::create";
    this->db_open(0);
    SlottedPage *slottedPage = this->get_new();
  //  delete slottedPage;
}

void HeapFile::close() {
    this->db.close(0);
    this->closed = true;
}

BlockIDs *HeapFile::block_ids() {
    BlockIDs *blockIDs = new BlockIDs();
    for(BlockID i = 1; i <= this->last; i++)
    {
        //
    }
    return blockIDs;
}
void HeapFile::open() {
    this->db_open();
}

void HeapFile::db_open(uint flags) {
    cout << "Inside HeapFile::create ";
    cout << "closed :: " << this->closed;
    if (this->closed) {
    //    db.set_message_stream(_DB_ENV->get_message_stream());
    //    db.set_error_stream(_DB_ENV->get_error_stream());
        db.set_re_len(DbBlock::BLOCK_SZ);
        this->dbfilename =  this->name + ".db";
        int result = this->db.open(NULL, this->dbfilename.c_str(), NULL, DB_RECNO, DB_CREATE, 0644);
        DB_BTREE_STAT stat;
        this->db.stat(NULL, &stat, DB_FAST_STAT);
        this->last = stat.bt_ndata;
        if(result != 0)
        {
            this->close();
        }
        this->closed = false;


    }
}

void HeapFile::put(DbBlock *block) {

}


void HeapFile::drop() {

}

SlottedPage *HeapFile::get(BlockID block_id) {

    SlottedPage *slottedPage = NULL;
    return slottedPage;
}


SlottedPage *HeapFile::get_new() {

    char block[SlottedPage::BLOCK_SZ];
    Dbt data(block, sizeof(block));
    int block_number;
    Dbt key(&block_number, sizeof(block_number));
    block_number = this->last + 1;
    SlottedPage *slottedPage = new SlottedPage(data, this->last, true);
 //   this->db.put(NULL, &key, &data, 0);
    this->db.put(NULL , &key, &data, 0);

    // return  slottedPage;
   return NULL;
}


// test function -- returns true if all tests pass
bool test_heap_file()
{
    cout << "test_heap_file called: " << endl;
    HeapFile heapFile("sample_db1");
    cout << "test_heap_file ::  heapFile initialised and db created" << endl;

    heapFile.create();
    cout << "test_heap_file heapFile create() called" << endl;
    heapFile.close();
    return true;
}