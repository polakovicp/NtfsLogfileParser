'''Module contains NTFS structures.

Author:
    P. Polakovic
'''

import ctypes


def dofixup(page, sector_size=0x200):
    '''Does usn fixups.'''

    msh = MultiSectorHeader.from_buffer_copy(page)

    usa_length = msh.usa_count * ctypes.sizeof(ctypes.c_uint16)

    pos = msh.usa_ofs
    valid_usn = page[pos:pos + 2]
    pos += 2

    for i in range(1, msh.usa_count):
        tmp = page[pos:pos + 2]
        page[pos:pos + 2] = page[(sector_size * i) - ctypes.sizeof(ctypes.c_uint16):\
                                    (sector_size * i)]
        page[(sector_size * i) - ctypes.sizeof(ctypes.c_uint16):\
                (sector_size * i)] = tmp

        stored_usn = page[pos:pos + 2]

        if stored_usn != valid_usn:
            raise ValueError('page invalid')

        pos += 2

    return page


class MultiSectorHeader(ctypes.LittleEndianStructure):
    _pack_ = 1
    _fields_ = [("magic", ctypes.c_uint32),
                ("usa_ofs", ctypes.c_uint16),
                ("usa_count", ctypes.c_uint16)]


class BiosParameterBlock(ctypes.LittleEndianStructure):
    '''BiosParameterBlock.'''
    _pack_ = 1
    _fields_ = [("bytes_per_sector", ctypes.c_uint16),
                ("sectors_per_cluster", ctypes.c_uint8),
                ("reserved_sectors", ctypes.c_uint16),
                ("fats", ctypes.c_uint8),
                ("root_entries", ctypes.c_uint16),
                ("sectors", ctypes.c_uint16),
                ("media_type", ctypes.c_uint8),
                ("sectors_per_fat", ctypes.c_uint16),
                ("sectors_per_track", ctypes.c_uint16),
                ("heads", ctypes.c_uint16),
                ("hidden_sectors", ctypes.c_uint32),
                ("large_sectors", ctypes.c_uint32)]


class NtfsBootSector(ctypes.LittleEndianStructure):
    '''NtfsBootSector.'''
    _pack_ = 1
    _fields_ = [("jmp", ctypes.c_ubyte * 3),
                ("oem_id", ctypes.c_uint64),
                ("bpb", BiosParameterBlock),
                ("physical_drive", ctypes.c_uint8),
                ("current_head", ctypes.c_uint8),
                ("extended_boot_signature", ctypes.c_uint8),
                ("reserved2", ctypes.c_uint8),
                ("number_of_sectors", ctypes.c_int64),
                ("mft_lcn", ctypes.c_int64),
                ("mftmirr_lcn", ctypes.c_int64),
                ("clusters_per_mft_record", ctypes.c_int8),
                ("reserved0", ctypes.c_ubyte * 3),
                ("clusters_per_index_record", ctypes.c_int8),
                ("reserved1", ctypes.c_ubyte * 3),
                ("volume_serial_number", ctypes.c_uint64),
                ("checksum", ctypes.c_uint32),
                ("bootstrap", ctypes.c_ubyte * 426),
                ("end_of_sector_marker", ctypes.c_uint16)]


class FileRecordSegmentHeader(ctypes.LittleEndianStructure):
    '''Entry in $Mft.'''
    _pack_ = 1
    _fields_ = [("multi_sector_header", MultiSectorHeader),
                ("lsn", ctypes.c_int64),
                ("sequence_number", ctypes.c_uint16),
                ("reference_count", ctypes.c_uint16),
                ("attr_offset", ctypes.c_uint16),
                ("flags", ctypes.c_uint16),
                ("first_free_byte", ctypes.c_uint32),
                ("bytes_available", ctypes.c_uint32),
                ("base_record", ctypes.c_uint64),
                ("next_attr_instance", ctypes.c_uint16),
                ("reserved", ctypes.c_uint16),
                ("mft_record_number", ctypes.c_uint32)]


class NonResidentAttributeRecord(ctypes.LittleEndianStructure):
    '''NonResidentAttributeRecord.'''
    _pack_ = 1
    _fields_ = [("type", ctypes.c_uint32),
                ("length", ctypes.c_uint32),
                ("form_code", ctypes.c_uint8),
                ("name_length", ctypes.c_uint8),
                ("name_offset", ctypes.c_uint16),
                ("flags", ctypes.c_uint16),
                ("instance", ctypes.c_uint16),
                ("lowest_vcn", ctypes.c_int64),
                ("highest_vcn", ctypes.c_int64),
                ("mapping_pairs_offset", ctypes.c_uint16),
                ("compression_unit", ctypes.c_uint8),
                ("reserved", ctypes.c_uint8 * 5),
                ("allocated_size", ctypes.c_int64),
                ("data_size", ctypes.c_int64),
                ("initialized_size", ctypes.c_int64),
                ("compressed_size", ctypes.c_int64)]


class RestartPageHeader(ctypes.LittleEndianStructure):
    '''Each page with restart area starts with this
    structure.
    '''
    _pack_ = 1
    _fields_ = [("multi_sector_header", MultiSectorHeader),
                ("chkdsk_lsn", ctypes.c_uint64),
                ("system_page_size", ctypes.c_uint32),
                ("log_page_size", ctypes.c_uint32),
                ("restart_offset", ctypes.c_uint16),
                ("minor_ver", ctypes.c_int16),
                ("major_ver", ctypes.c_int16)]


class LsnRestartArea(ctypes.LittleEndianStructure):
    '''Two copies of restart are exists on the first
    two log pages.
    '''
    _pack_ = 1
    _fields_ = [("current_lsn", ctypes.c_uint64),
                ("log_clients", ctypes.c_uint16),
                ("client_free_list", ctypes.c_uint16),
                ("client_in_use_list", ctypes.c_uint16),
                ("flags", ctypes.c_uint16),
                ("seq_number_bits", ctypes.c_uint32),
                ("restart_area_length", ctypes.c_uint16),
                ("client_array_offset", ctypes.c_uint16),
                ("file_size", ctypes.c_int64),
                ("last_lsn_data_length", ctypes.c_uint32),
                ("log_record_header_length", ctypes.c_uint16),
                ("log_page_data_offset", ctypes.c_uint16),
                ("restart_log_open_count", ctypes.c_uint32),
                ("reserved", ctypes.c_uint32)]


class ClientRecord(ctypes.LittleEndianStructure):
    '''ClientRecord.'''
    _pack_ = 1
    _fields_ = [("oldest_lsn", ctypes.c_int64),
                ("client_restart_lsn", ctypes.c_int64),
                ("prev_client", ctypes.c_uint16),
                ("next_client", ctypes.c_uint16),
                ("seq_number", ctypes.c_uint16),
                ("alignment", ctypes.c_uint8 * 6),
                ("name_length", ctypes.c_uint32),
                ("name", ctypes.c_ubyte * 64)]

    def get_name(self):
        '''Returns clients name.'''
        return bytes(self.name[:self.name_length]).decode(encoding="utf-16LE")


def client_record_array_factory(clients, data):
    '''Returns new array with client records.'''
    class ClientRecordArray(ctypes.LittleEndianStructure):
        '''ClientRecord container.'''
        _pack_ = 1
        _fields_ = [("clients", clients * ClientRecord)]

    return ClientRecordArray.from_buffer_copy(data)


class LogClientId(ctypes.LittleEndianStructure):
    '''LogClientId.'''
    _pack_ = 1
    _fields_ = [("seq_number", ctypes.c_uint16),
                ("client_index", ctypes.c_uint16)]


class RecordData(ctypes.Union):
    '''RecordData.'''
    _pack_ = 1
    _fields_ = [("last_lsn", ctypes.c_int64),
                ("file_offset", ctypes.c_int64)]


LOG_PAGE_LOG_RECORD_END = 0x1


class RecordPageHeader(ctypes.LittleEndianStructure):
    '''Header located on begining of every log page.
    '''
    _pack_ = 1
    _fields_ = [("sector_header", MultiSectorHeader),   # RCRD
                ("copy", RecordData),
                ("flags", ctypes.c_uint32),     # 16
                ("page_count", ctypes.c_uint16),     # 20
                ("page_position", ctypes.c_uint16),     # 22
                ("next_record_offset", ctypes.c_uint16),     # 24
                ("reserved", ctypes.c_uint8 * 6),  # 26
                ("last_end_lsn", ctypes.c_int64)]      # 32


NOOP = 0x00
COMPENSATION_LOG_RECORD = 0x01
INIT_FILE_RECORD = 0x02
DEALLOC_FILE_RECORD = 0x03
WRITE_FILE_RECORD_END = 0x04
CREATE_ATTRIBUTE = 0x05
DELETE_ATTRIBUTE = 0x06
UPDATE_RESIDENT_VALUE = 0x07
UPDATE_NON_RESIDENT_VALUE = 0x08
UPDATE_MAPPING_PAIRS = 0x09
DELETE_DIRTY_CLUSTERS = 0x0a
SET_ATTRIBUTE_SIZES = 0x0b
ADD_ROOT_ENTRY = 0x0c
DELETE_ROOT_ENTRY = 0x0d
ADD_ALLOCATION_ENTRY = 0x0e
DELETE_ALLOCATION_ENTRY = 0x0f
WRITE_BUFFER_END = 0x10
SET_ROOT_ENTRY_VCN = 0x11
SET_ALLOCATION_ENTRY_VCN = 0x12
UPDATE_ROOT_FILE_NAME = 0x13
UPDATE_ALLOCATION_FILE_NAME = 0x14
SET_BITS = 0x15
CLEAR_BITS = 0x16
HOT_FIX = 0x17
END_TOP_LEVEL_ACTION = 0x18
PREPARE_TRANSACTION = 0x19
COMMIT_TRANSACTION = 0x1a
FORGET_TRANSACTION = 0x1b
OPEN_NONRESIDENT_ATTRIBUTE = 0x1c
OPEN_ATTRIBUTE_TABLE_DUMP = 0x1d
ATTRIBUTE_NAMES_DUMP = 0x1e
DIRTY_PAGE_TABLE_DUMP = 0x1f
TRANSACTION_TABLE_DUMP = 0x20
UPDATE_ROOT_ENTRY = 0x21
UPDATE_ALLOCATION_ENTRY = 0x22


'''Mapping operation code -> real operation name (M$)'''
LOG_OPERATION = {NOOP : "Noop",
                 COMPENSATION_LOG_RECORD : "CompensationLogRecord",
                 INIT_FILE_RECORD : "InitializeFileRecordSegment",
                 DEALLOC_FILE_RECORD : "DeallocateFileRecordSegment",
                 WRITE_FILE_RECORD_END : "WriteEndofFileRecordSegment",
                 CREATE_ATTRIBUTE : "CreateAttribute",
                 DELETE_ATTRIBUTE : "DeleteAttribute",
                 UPDATE_RESIDENT_VALUE : "UpdateResidentValue",
                 UPDATE_NON_RESIDENT_VALUE : "UpdateNonResidentValue",
                 UPDATE_MAPPING_PAIRS : "UpdateMappingPairs",
                 DELETE_DIRTY_CLUSTERS : "DeleteDirtyClusters",
                 SET_ATTRIBUTE_SIZES : "SetNewAttributeSizes",
                 ADD_ROOT_ENTRY : "AddindexEntryRoot",
                 DELETE_ROOT_ENTRY : "DeleteIndexEntryRoot",
                 ADD_ALLOCATION_ENTRY : "AddIndexEntryAllocation",
                 DELETE_ALLOCATION_ENTRY : "DeleteIndexEntryAllocation",
                 WRITE_BUFFER_END : "WriteEndOfIndexBuffer",
                 SET_ROOT_ENTRY_VCN : "SetIndexEntryVcnRoot",
                 SET_ALLOCATION_ENTRY_VCN : "SetIndexEntryVcnAllocation",
                 UPDATE_ROOT_FILE_NAME : "UpdateFileNameRoot",
                 UPDATE_ALLOCATION_FILE_NAME : "UpdateFileNameAllocation",
                 SET_BITS : "SetBitsInNonresidentBitMap",
                 CLEAR_BITS : "ClearBitsInNonresidentBitMap",
                 HOT_FIX : "HotFix",
                 END_TOP_LEVEL_ACTION : "EndTopLevelAction",
                 PREPARE_TRANSACTION : "PrepareTransaction",
                 COMMIT_TRANSACTION : "CommitTransaction",
                 FORGET_TRANSACTION : "ForgetTransaction",
                 OPEN_NONRESIDENT_ATTRIBUTE : "OpenNonresidentAttribute",
                 OPEN_ATTRIBUTE_TABLE_DUMP : "OpenAttributeTableDump",
                 ATTRIBUTE_NAMES_DUMP : "AttributeNamesDump",
                 DIRTY_PAGE_TABLE_DUMP : "DirtyPageTableDump",
                 TRANSACTION_TABLE_DUMP : "TransactionTableDump",
                 UPDATE_ROOT_ENTRY : "UpdateRecordDataRoot",
                 UPDATE_ALLOCATION_ENTRY : "UpdateRecordDataAllocation"}


class ClientLogHeader(ctypes.LittleEndianStructure):
    '''ClientLogHeader.'''
    _pack_ = 1
    _fields_ = [("redo_op", ctypes.c_uint16),
                ("undo_op", ctypes.c_uint16),
                ("redo_offset", ctypes.c_uint16),
                ("redo_length", ctypes.c_uint16),
                ("undo_offset", ctypes.c_uint16),
                ("undo_length", ctypes.c_uint16),
                ("target_attr", ctypes.c_uint16),
                ("lcns_to_follow", ctypes.c_uint16),
                ("record_offset", ctypes.c_uint16),
                ("attribute_offset", ctypes.c_uint16),
                ("cluster_block_offset", ctypes.c_uint16),
                ("reserved", ctypes.c_uint16),
                ("target_vcn", ctypes.c_uint64),
                ("lcns_for_page", ctypes.c_uint64 * 1)]


RECORD_TYPE_CLIENT = 0x1
RECORD_TYPE_RESTART = 0x2


class LogRecord(ctypes.LittleEndianStructure):
    '''Header preceeding every log record.
    '''
    _pack_ = 1
    _fields_ = [("this_lsn", ctypes.c_uint64),     # 0
                ("client_previous_lsn", ctypes.c_uint64),     # 8
                ("client_undo_next_lsn", ctypes.c_uint64),     # 16
                ("client_data_length", ctypes.c_uint32),     # 24
                ("client", LogClientId),         # 28
                ("record_type", ctypes.c_uint32),     # 32
                ("transaction_id", ctypes.c_uint32),     # 36
                ("flags", ctypes.c_uint16),     # 40
                ("alignment", ctypes.c_uint16)]     # 42


class RestartArea(ctypes.LittleEndianStructure):
    '''RestartArea.'''
    _pack_ = 1
    _fields_ = [("major_version", ctypes.c_uint32), # 0x00
                ("minor_version", ctypes.c_uint32), # 0x04
                ("start_of_checkpoint", ctypes.c_uint64), # 0x08
                ("open_attr_table_lsn", ctypes.c_uint64), # 0x10
                ("attr_names_lsn", ctypes.c_uint64), # 0x18
                ("dirty_pages_table_lsn", ctypes.c_uint64), # 0x20
                ("transaction_table_lsn", ctypes.c_uint64), # 0x28
                ("open_attr_table_len", ctypes.c_uint32), # 0x30
                ("attr_names_len", ctypes.c_uint32), # 0x34
                ("dirty_pages_table_len", ctypes.c_uint32), # 0x38
                ("transaction_table_len", ctypes.c_uint32)] # 0x3C


class RestartTable(ctypes.LittleEndianStructure):
    '''RestartTable.'''
    _pack_ = 1
    _fields_ = [("entry_size", ctypes.c_uint16),
                ("number_of_entries", ctypes.c_uint16),
                ("number_allocated", ctypes.c_uint16),
                ("reserved", ctypes.c_uint16 * 3),
                ("free_goal", ctypes.c_uint32),
                ("first_free", ctypes.c_uint32),
                ("last_free", ctypes.c_uint32)]


RESTART_ENTRY_ALLOCATED = 0xFFFFFFFF


def attribute_name_entry_factory(name_length, data):
    '''Returns new attribute name entry.'''
    class AttributeNameEntry(ctypes.LittleEndianStructure):
        '''AttributeNameEntry.'''
        _pack_ = 1
        _fields_ = [("index", ctypes.c_uint16),
                    ("name_length", ctypes.c_uint16),
                    ("name", ctypes.c_uint8 * name_length)]

        def get_name(self):
            '''Returns entry name.'''
            return bytes(
                self.name[:self.name_length]).decode(encoding="utf-16LE")

    return AttributeNameEntry(data)


def dirty_page_entry_factory(lcns, data=None):
    '''Returns new dirty page entry.'''
    if data is None:
        data = bytearray()

    class DirtyPageEntry(ctypes.LittleEndianStructure):
        '''DirtyPageEntry.'''
        _pack_ = 1
        _fields_ = [("allocated_or_next_free", ctypes.c_uint32),        # 0
                    ("target_attribute", ctypes.c_uint32),        # 4
                    ("length_of_transfer", ctypes.c_uint32),        # 8
                    ("lcns_to_follow", ctypes.c_uint32),        # 12
                    ("reserved", ctypes.c_uint32),        # 16
                    ("vcn", ctypes.c_uint64),        # 20
                    ("oldest_lsn", ctypes.c_uint64),        # 28
                    ("lcns_for_page", ctypes.c_uint64 * lcns)]    # 36

    return DirtyPageEntry(data)


def open_attribute_entry_factory(data):
    '''Returns new open attribute entry.'''
    if len(data) < 0x2C:
        return open_attribute_entry64_factory(data)

    return open_attribute_entry32_factory(data)


def open_attribute_entry32_factory(data):
    '''Returns new open attribute entry.

    Found on Windows 7 32b.
    '''
    class OpenAttributeEntry(ctypes.LittleEndianStructure):
        '''OpenAttributeEntry.'''
        _pack_ = 1
        _fields_ = [("allocated_or_next_free", ctypes.c_uint32),
                    ("overlay", ctypes.c_uint32),
                    ("file_reference", ctypes.c_uint64),
                    ("lsn_of_open_record", ctypes.c_uint64),
                    ("dirty_pages_seen", ctypes.c_uint8),
                    ("attribute_name_present", ctypes.c_uint8),
                    ("reserved", ctypes.c_uint16),
                    ("attribute_type_code", ctypes.c_uint32),
                    ("attribute_name", ctypes.c_uint64),
                    ("byte_per_index_buffer", ctypes.c_uint32)]

    return OpenAttributeEntry(data)


def open_attribute_entry64_factory(data):
    '''Returns new open attribute entry.

    Found on Windows 7 64b.
    '''
    class OpenAttributeEntry(ctypes.LittleEndianStructure):
        '''OpenAttributeEntry.'''
        _pack_ = 1
        _fields_ = [("allocated_or_next_free", ctypes.c_uint32),
                    ("unknown1", ctypes.c_uint32),
                    ("attribute_type_code", ctypes.c_uint32),
                    ("unknown2", ctypes.c_uint32),
                    ("file_reference", ctypes.c_uint64),
                    ("lsn_of_open_record", ctypes.c_uint64),
                    ("unknown3", ctypes.c_uint64)]

    return OpenAttributeEntry(data)


TRANSACTION_UNINITIALIZED = 0x0
TRANSACTION_ACTIVE = 0x1
TRANSACTION_PREPARED = 0x2
TRANSACTION_COMMITED = 0x3


class TransactionEntry(ctypes.LittleEndianStructure):
    '''NewAttributeSizes.'''
    _pack_ = 1
    _fields_ = [("allocated_or_next_free", ctypes.c_uint32),
                ("transaction_state", ctypes.c_uint8),
                ("reserved", ctypes.c_uint8 * 3),
                ("first_lsn", ctypes.c_int64),
                ("previous_lsn", ctypes.c_int64),
                ("undo_next_lsn", ctypes.c_int64),
                ("undo_records", ctypes.c_uint32),
                ("undo_bytes", ctypes.c_uint32)]
