#include "heap_storage.h"
#include <cstring>
#include <bitset>
#include <iostream>
#include "db_cxx.h"

using namespace std;
typedef u_int16_t u16;

/*----------------------------------------SlotttedPage---------------------------------------*/
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

/**
 *  Add a new record to the block. Return its id.
 * @param data to be added
 */
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

/**
 *  Get an existing record by record id.
 * @param record_id the record id
 */
Dbt* SlottedPage::get(RecordID record_id) {
    u16  size, loc;
    get_header(size, loc, record_id);

    if (loc == 0) {
        return nullptr;
    }

    return new Dbt(this->address(loc), loc + size);
}

/**
 *  Delete an existing record by record id.
 * @param record_id the record id
 */
void SlottedPage::del(RecordID record_id) {
    u16  size, loc;
    get_header(size, loc, record_id);

    put_header(record_id, 0, 0);
    slide(loc, loc+size);
}

/**
 *  Update an existing record by record id and updated data.
 * @param record_id the record id
 * @param data the updated data
 */
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

/**
 *  Get record ids.
 */
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

/**
 *  Check if there is room available for another record.
 * @param size the size
 */
bool SlottedPage::has_room(u16 size) {
    u16 available = this->end_free - (this->num_records + 1) * 4;
    return size <= available;
}

void SlottedPage::get_header(u16 &size, u16 &loc, RecordID record_id) {
    size = get_n(4 * record_id);
    loc = get_n(4 * record_id + 2);
}

/**
 *  Store the size and offset for given id. For id of zero, store the block header.
 * @param id the record id
 * @param size the size
 * @param loc
 */
void SlottedPage::put_header(RecordID id, u16 size, u16 loc) {
    if (id == 0) { // called the put_header() version and using the default params
        size = this->num_records;
        loc = this->end_free;
    }
    put_n(4 * id, size);
    put_n(4 * id + 2, loc);
}

//sliding the records if a record is deleted or updated.
void SlottedPage::slide(u16 start, u16 end) {
    u16 shift = end - start;

    if (shift == 0) {
        return;
    }

    u16 bytes_to_move = start - this->end_free - 1;
    memcpy(this->address(this->end_free + 1 + shift), this->address(this->end_free + 1), bytes_to_move);

    // fix headers
    u16 size, loc;
    RecordIDs* currentIds = ids();

    for (RecordID &current_id : *currentIds) {
        get_header(size, loc, current_id);

        if (loc <= start) {
            loc += shift;
            put_header(current_id, size, loc);
        }
    }

    this->end_free += shift;
    put_header();
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

/*------------------------------------------- Heap File---------------------------------------*/

void HeapFile::create() {
    this->db_open(DB_CREATE | DB_EXCL);
    SlottedPage *slottedPage = this->get_new();
    this->put(slottedPage);
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
    if (this->closed) {
        this->db.set_re_len(DbBlock::BLOCK_SZ);
        this->dbfilename =  this->name + ".db";
        cout << "HeapFile::db_open -> database filename " << this->name << endl;
        this->db.open(nullptr, (this->dbfilename).c_str(), nullptr, DB_RECNO, flags, 0644);
        DB_BTREE_STAT *stat;
        this->db.stat(nullptr, &stat, DB_FAST_STAT);
        this->last = flags != 0 ? 0 : stat->bt_ndata;
        delete stat;
        this->closed = false;
    }
}

void HeapFile::put(DbBlock *block) {
    BlockID blockId = block->get_block_id();
    Dbt key(&blockId,sizeof(blockId));
    cout << "HeapFile::put with blockid = "<< blockId << endl;
    this->db.put(nullptr, &key, block->get_block(),0);
}


void HeapFile::drop() {
    this->close();
    Db db1(_DB_ENV,0);
    cout << "HeapFile::drop -> dbfilename : " << this->dbfilename << endl;
    db1.remove(this->dbfilename.c_str(), nullptr, 0);
}

SlottedPage *HeapFile::get(BlockID block_id) {
    Dbt key(&block_id,sizeof(block_id));
    Dbt rdata;
    db.get(NULL, &key, &rdata, 0); // read block from the database
    cout << "HeapFile::get -> block_id : " << block_id << " data : " << (char *)rdata.get_data() << endl;
    SlottedPage *slottedPage = new SlottedPage(rdata, block_id, true);
    return slottedPage;
}


SlottedPage *HeapFile::get_new() {
    char block[SlottedPage::BLOCK_SZ];
    Dbt data(block, sizeof(block));
    this->last++;
    SlottedPage *slottedPage = new SlottedPage(data, this->last, true);
    cout << "HeapFile::get_new ending with last = "<< this->last << endl;
    return  slottedPage;
}

// test function -- returns true if all tests pass
bool test_heap_file(const char *filename)
{
    HeapFile heapFile(filename);
    heapFile.create();
    SlottedPage *slottedPage = heapFile.get_new();
    heapFile.put(slottedPage);
    heapFile.get(slottedPage->get_block_id());
    delete slottedPage;
    heapFile.get_last_block_id();
    heapFile.drop();
    return true;
}


/* --------------------------------HeapTable---------------------------------------*/

HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes)
        :DbRelation(table_name, column_names, column_attributes), file(table_name)
{}

void HeapTable::create()
{
    file.create();
}

void HeapTable::create_if_not_exists() {
    try {
        file.create();
    } catch (DbRelationError &e) {
        file.open();
    }
}

void HeapTable::open()
{
    file.open();
}

void HeapTable::close()
{
    file.close();
}

void HeapTable::drop() {
    file.drop();
}

Handle HeapTable::insert(const ValueDict* row) {
    this->open();
    return this->append(this->validate(row));
}

void HeapTable::update(const Handle handle, const ValueDict* new_values) {
    throw DbRelationError("Not implemented");
}

void HeapTable::del(const Handle handle) {
    throw DbRelationError("Not implemented");
}

Handles* HeapTable::select() {
    Handles* handles = new Handles();
    BlockIDs* block_ids = file.block_ids();

    for (auto const& block_id : *block_ids) {
        SlottedPage* block = file.get(block_id);
        RecordIDs* record_ids = block->ids();
        for (auto const& record_id : *record_ids)
            handles->push_back(Handle(block_id, record_id));
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}

Handles* HeapTable::select(const ValueDict* where) {
    Handles* handles = new Handles();
    BlockIDs* block_ids = file.block_ids();

    for (auto const& block_id : *block_ids) {
        SlottedPage* block = file.get(block_id);
        RecordIDs* record_ids = block->ids();
        for (auto const& record_id : *record_ids)
            handles->push_back(Handle(block_id, record_id));
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}

ValueDict* HeapTable::project(Handle handle) {
    BlockID block_id = handle.first;
    RecordID record_id = handle.second;
    SlottedPage* block = this->file.get(block_id);
    Dbt* data = block->get(record_id);

    ValueDict* row = this->unmarshal(data);
    delete block;
    delete data;
    return row;
}

ValueDict* HeapTable::project(Handle handle, const ColumnNames* column_names) {
    ValueDict* row = project(handle);

    if (column_names == nullptr) {
        return row;
    }

    ValueDict* new_row = new ValueDict();
    for (auto const& column_name : this->column_names) {
        new_row->insert(pair<Identifier, Value>(column_name, row->at(column_name)));
    }

    delete row;
    return new_row;
}

ValueDict* HeapTable::validate(const ValueDict* row) {
    cout << "start validation" << endl;
    ValueDict* full_row = new ValueDict();

    for (auto const& column_name : this->column_names) {
        ValueDict::const_iterator column = row->find(column_name);
        if (column == row->end()) {
            throw DbRelationError("don't know how to handle NULLs, defaults, etc. yet");
        }

        Value value = column->second;
        full_row->insert(pair<Identifier, Value>(column_name, value));
    }

    return full_row;
}

Handle HeapTable::append(const ValueDict* row) {
    cout << "start appending" << endl;
    Dbt *data = this->marshal(row);

    SlottedPage *block = this->file.get(this->file.get_last_block_id());
    RecordID recordID;

    try {
        recordID = block->add(data);
    }catch (DbBlockNoRoomError& e) {
        delete block;
        block = this->file.get_new();
        recordID = block->add(data);
    }

    this->file.put(block);
    delete block;
    delete data;
    return pair<BlockID, RecordID>(file.get_last_block_id(), recordID);
}

// return the bits to go into the file
// caller responsible for freeing the returned Dbt and its enclosed ret->get_data().
Dbt* HeapTable::marshal(const ValueDict* row) {
    cout << "start marshalling" << endl;

    char* bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
    uint offset = 0;
    uint col_num = 0;

    for (auto const& column_name : this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        ValueDict::const_iterator column = row->find(column_name);
        Value value = column->second;
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            *(int32_t*)(bytes + offset) = value.n;
            offset += sizeof(int32_t);
        }
        else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            uint size = value.s.length();
            *(u16*)(bytes + offset) = size;
            offset += sizeof(u16);
            memcpy(bytes + offset, value.s.c_str(), size); // assume ascii for now
            offset += size;
        }
        else {
            throw DbRelationError("Only know how to marshal INT and TEXT");
        }
    }
    char* right_size_bytes = new char[offset];
    memcpy(right_size_bytes, bytes, offset);

    delete[] bytes;

    Dbt* data = new Dbt(right_size_bytes, offset);
    return data;
}

//return row values
ValueDict* HeapTable::unmarshal(Dbt* data) {
    ValueDict* row = new ValueDict();
    uint offset = 0;
    uint col_num = 0;
    char* result_data = (char*)data->get_data();

    for (auto const& column_name : this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {

            int32_t n = *(int32_t*)(result_data + offset);
            Value val(n);
            offset += sizeof(int32_t);
            row->insert(pair<Identifier, Value>(column_name, val));
        }
        else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {

            u16 size = *(u16*)(result_data + offset);
            offset += sizeof(u16);
            string s = string(result_data + offset);
            Value val(s);
            offset += size;
            row->insert(pair<Identifier, Value>(column_name, val));
        }
        else {
            throw DbRelationError("Only know how to unmarshal INT and TEXT");
        }
    }

    return row;
}

// test function for heap storage -- returns true if all tests pass
/*
 * bool test_heap_storage() {

    ColumnNames column_names;
    column_names.push_back("a");
    column_names.push_back("b");
    ColumnAttributes column_attributes;
    ColumnAttribute ca(ColumnAttribute::INT);
    column_attributes.push_back(ca);
    ca.set_data_type(ColumnAttribute::TEXT);
    column_attributes.push_back(ca);
    HeapTable table1("_test_create_drop", column_names, column_attributes);
    table1.create();
    std::cout << "create ok" << std::endl;
    table1.drop();  // drop makes the object unusable because of BerkeleyDB restriction -- maybe want to fix this some day
    std::cout << "drop ok" << std::endl;

    HeapTable table("_test_data_cpp", column_names, column_attributes);
    table.create_if_not_exists();
    std::cout << "create_if_not_exsts ok" << std::endl;



    ValueDict row;
    row["a"] = Value(12);
    row["b"] = Value("Hello!");
    std::cout << "try insert" << std::endl;
    table.insert(&row);
    std::cout << "insert ok" << std::endl;



    * Handles* handles = table.select();
    std::cout << "select ok " << handles->size() << std::endl;
    * ValueDict* result = table.project((*handles)[0]);
    std::cout << "project ok" << std::endl;
    Value value = (*result)["a"];
    if (value.n != 12)
        return false;
    value = (*result)["b"];
    if (value.s != "Hello!")
        return false;


    table.drop();

    return true;
}
 */
