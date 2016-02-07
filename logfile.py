'''Module contains objects for accessing NTFS journal
change records.

Author:
    P. Polakovic
'''
import functools
import logging

from collections import namedtuple
from ctypes import sizeof, c_uint16
from struct import unpack

import layout

from utils import qalign, ffs


SECTOR_SIZE = 0x200


LogfileRestartBlock = namedtuple("LogfileRestartBlock",
                                 ["header", "area", "clients"])


# Holds structures required for volume restart.
ClientRestartContext = namedtuple("ClientRestartContext",
                                  ["checkpoint",
                                   "attribute_names",
                                   "dirty_pages",
                                   "open_attributes",
                                   "transactions"])


def get_restart_context(buffer):
    '''Returns restart metadata.

    Layout:
    +-------------------+ 0x0
    | RestartPageHeader |---+
    |                   |   |
    +-------------------+   | restart_offset
    ~                   ~   |
    +-------------------+<--+
    |  LsnRestartArea   |
    |                   |---+
    +-------------------+   | restart_offset +
    ~                   ~   | client_array_offset
    +-------------------+<--+
    | ClientRecordArray |
    |                   |
    ~                   ~
    |                   |
    +-------------------+ system page size
    '''

    # RestartPageHeader
    header = layout.RestartPageHeader.from_buffer_copy(buffer)

    # LsnRestartArea
    page_offset = header.restart_offset

    area = layout.LsnRestartArea.from_buffer_copy(
        buffer[page_offset:page_offset + sizeof(layout.LsnRestartArea)])

    # Clients
    page_offset += area.client_array_offset

    client_array = layout.client_record_array_factory(
        area.log_clients,
        buffer[page_offset:])

    return LogfileRestartBlock(header, area, client_array)


def get_lsn_restart_blocks(logfile):
    '''Returns restart blocks.

    Block with higher current LSN is considered to be
    valid (more up-to date). Remaining block is used as
    a backup.

    Returns:
        tuple - (valid block, backup block)
    '''
    logfile.seek(0)

    # Inspect logfile a bit...
    sector = logfile.read(SECTOR_SIZE)

    restart_header = layout.RestartPageHeader.from_buffer_copy(sector)

    logfile.seek(0)

    if restart_header.system_page_size > (1024 * 64):
        # Max cluster size is 64kB
        # https://support.microsoft.com/en-us/kb/140365
        raise ValueError("invalid system page size?")

    # Read first two pages from the logfile
    pages = [bytearray(restart_header.system_page_size),
             bytearray(restart_header.system_page_size)]

    for page in pages:
        logfile.readinto(page)
        layout.dofixup(page, sector_size=SECTOR_SIZE)

    block_first = None
    block_second = None

    if pages[0]:
        block_first = get_restart_context(pages[0])

    if pages[1]:
        block_second = get_restart_context(pages[1])

    if block_first and block_second:
        if block_first.area.current_lsn < block_second.area.current_lsn:
            # First block has lower LSN, so swap block, since we
            # are returning first block as the valid one.
            block_first, block_second = block_second, block_first
    if not block_first:
        # Swap blocks when first block is invalid.
        block_first, block_second = block_second, block_first

    return block_first, block_second


def parse_dirty_pages(table, dirty_pages):
    '''Returns dirty pages from client data.'''
    default_entry_size = sizeof(layout.dirty_page_entry_factory(1))
    lcns_per_page = int((table.entry_size - default_entry_size) / sizeof(layout.LSN))
    lcns_per_page += 1

    entries = []

    for i in range(table.number_of_entries):
        data = dirty_pages[i * table.entry_size:(i + 1) * table.entry_size]
        entries.append(layout.dirty_page_entry_factory(lcns_per_page, data))

    return entries


def parse_transactions(table, transactions):
    '''Returns dirty pages from client data.'''
    entries = {}

    for i in range(table.number_of_entries):
        data = transactions[i * table.entry_size:(i + 1) * table.entry_size]

        entries[sizeof(layout.RestartTable) + (i * table.entry_size)] = \
            layout.TransactionEntry(data)

    return entries


def parse_open_attributes(table, attributes):
    '''Returns dirty pages from client data.'''
    entries = {}

    for i in range(table.number_of_entries):
        data = attributes[i * table.entry_size:(i + 1) * table.entry_size]

        entries[sizeof(layout.RestartTable) + (i * table.entry_size)] = \
            layout.open_attribute_entry_factory(data)

    return entries


def parse_attribute_names(attribute_names):
    '''Returns attribute names from client data.'''
    header = layout.ClientLogHeader(attribute_names)
    attribute_names = attribute_names[header.redo_offset:]

    names = []
    offset = 0

    while True:
        index = unpack("<H", attribute_names[offset:offset + 2])[0]
        length = unpack("<H", attribute_names[offset + 2:offset + 4])[0]

        if index == 0 and length == 0:
            break

        data = attribute_names[offset:offset + (3 * sizeof(c_uint16)) + length]

        names.append(layout.attribute_name_entry_factory(length, data))
        offset += len(data)

    return names


LogfileControlBlock = namedtuple("LogfileControlBlock",
                                 ["system_page_size",
                                  "log_page_size",
                                  "file_size",
                                  "sequence_number_bits",
                                  "log_page_data_offset",
                                  "system_page_mask",
                                  "system_page_inverse_mask",
                                  "log_page_mask",
                                  "log_page_inverse_mask",
                                  "file_size_bits"])


class LogFile(object):
    '''Represents $LogFile::$DATA attribute, which holds
    log entries.

    Log file organization is as follows:

    +-----------------+
    | Restart Context |< Holds page sizes and info about logging
    |                 |  clients.
    +-----------------+
    | Restart Context |< Copy of above. Used when above page is
    |      copy       |  invalid. When volume is
    +-----------------+  properly shuted down, this contains
    |   Buffer zone   |  same data as first page.
    |                 |< Each new record is firstly written here.
    +-----------------+  When this page is full, it's copied to
    |   Buffer zone   |  logging area.
    |                 |< Same purpose as above page. This is not
    +-----------------+  it's copy, but can contain same data.
    |  Logging area   |< Area is splitted into pages. Each page
    ~~~~~~~~~~~~~~~~~~~  contains log records.

    Each page has multi sector protection. First two pages might
    not have the same size as pages used for records.

    Buffer zones are so called tail pages, because always
    contain last written record.
    '''
    def __init__(self, logfile_stream, restart_block):
        '''Initializes log file object.
        '''

        self.logfile_stream = logfile_stream
        self.log_clients = restart_block.clients

        self.lcb = LogfileControlBlock(
            restart_block.header.system_page_size,
            restart_block.header.log_page_size,
            restart_block.area.file_size,
            restart_block.area.seq_number_bits,
            restart_block.area.log_page_data_offset,
            restart_block.header.system_page_size - 1,
            ~(restart_block.header.system_page_size - 1),
            restart_block.header.log_page_size - 1,
            ~(restart_block.header.log_page_size - 1),
            restart_block.area.file_size.bit_length() - 3)

        # flush records from buffer area to log area
        self.first_log_page = self.flush_buffer_area()

    def lsn2seqno(self, lsn):
        '''LSN to sequence number.'''
        return (lsn >> self.lcb.file_size_bits) & 0xFFFFFFFFFFFFFFFF

    def lsn2foffset(self, lsn):
        '''LSN to file offset.'''
        return ((lsn << self.lcb.sequence_number_bits) & 0xFFFFFFFFFFFFFFFF) \
               >> (self.lcb.sequence_number_bits - 3)

    def lsn2poffset(self, lsn):
        '''LSN to page offset.'''
        return ((lsn & 0xFFFFFFFF) << 3) & (self.lcb.log_page_size - 1)

    def lsn2page(self, lsn):
        '''LSN to page.'''
        return self.foffset2page(self.lsn2foffset(lsn))

    def foffset2page(self, offset):
        '''Offset to page.'''
        return offset & self.lcb.system_page_inverse_mask

    def foffset2lsn(self, offset, sequence_number):
        '''File offset to LSN.'''
        return (offset >> 3) + ((sequence_number << self.lcb.file_size_bits) \
                                & 0xFFFFFFFFFFFFFFFF)

    @functools.lru_cache(maxsize=4)
    def get_log_page(self, page):
        '''Returns page from log.
        Does USN fixups.
        '''
        self.logfile_stream.seek(page)
        page = bytearray(self.lcb.log_page_size)
        self.logfile_stream.readinto(page)
        return layout.dofixup(page)

    def next_log_page(self, current_page):
        '''Returns offset of the next log page.'''

        next_page = current_page + self.lcb.log_page_size

        if next_page >= self.lcb.file_size:
            # wrapping up log
            next_page = self.first_log_page

        return next_page

    def get_buffer_pages(self):
        '''Returns pages from buffer zone.

        On Win7 and older (journal version 1.1), the value of
        RecordPageHeader.copy.file_offset holds file offset,
        but starting from version 2.0, file_offset is not used
        anymore and thus must be computed from last_lsn value.
        Also in version 1.1 there were 2 pages for buffer area
        but from 2.0 there are 32 pages.
        '''

        buffer_pages = []
        self.logfile_stream.seek(self.lcb.system_page_size << 1)

        while True:

            page_offset = self.logfile_stream.tell()
            page = bytearray(self.lcb.system_page_size)
            self.logfile_stream.readinto(page)

            header = layout.RecordPageHeader.from_buffer_copy(page)

            if self.lcb.log_page_mask & header.copy.last_lsn:
                # Value of the last LSN is a LSN, not
                # file offset, so if the last LSN resides
                # on a correct page (file offset), we are
                # done with the buffer zone.
                if self.lsn2page(header.copy.last_lsn) == page_offset:
                    break

            buffer_pages.append(page)        

        return buffer_pages

    def flush_buffer_area(self):
        '''Remembers pages from buffer area which should be
        copied the log area.

        Copy is made only when page from buffer area is more
        recent then page from log area.

        Also value of last_lsn of buffer pages is fixed.

        Method returns offset of the first `real` log page.
        '''

        buffer_pages = self.get_buffer_pages()

        # Now per each page check LSN on original position.
        for buffer_page in buffer_pages:

            buffer_page_header = layout.RecordPageHeader.from_buffer_copy(
                buffer_page)

            log_page_offset = buffer_page_header.copy.file_offset

            if self.lcb.log_page_mask & log_page_offset:
                log_page_offset = self.lsn2page(log_page_offset)

            self.logfile_stream.seek(log_page_offset)

            log_page = bytearray(self.lcb.system_page_size)
            self.logfile_stream.readinto(log_page)

            log_page_header = layout.RecordPageHeader.from_buffer_copy(
                log_page)

            buffer_page_last_lsn = buffer_page_header.last_end_lsn

            if self.lcb.log_page_mask & buffer_page_header.copy.file_offset:
                buffer_page_last_lsn = buffer_page_header.copy.last_lsn

            if buffer_page_last_lsn > log_page_header.copy.last_lsn:
                # Buffer page has higher LSN. So we need to overwrite
                # this log page.

                # Also overwrite last LSN value of the buffer
                # page, with the last end value but only if it's file
                # offset (< 2.0)
                if not self.lcb.log_page_mask & buffer_page_header.copy.file_offset: 
                    last_lsn_offset = layout.RecordPageHeader.copy.offset
                    last_end_lsn_offset = layout.RecordPageHeader.last_end_lsn.offset

                    buffer_page[last_lsn_offset:last_lsn_offset + 8] = \
                    buffer_page[last_end_lsn_offset:last_end_lsn_offset + 8]

                self.logfile_stream.seek(log_page_offset)
                self.logfile_stream.write(buffer_page)

        # Compute offset of the first log page.
        first_log_page = self.lcb.system_page_size << 1
        first_log_page += len(buffer_pages) * self.lcb.log_page_size

        return first_log_page

    def records(self, lsn):
        '''Enumerates log records starting from `lsn`.

        Enumeration ends, when error occurs.
        '''

        # remember starting sequence number
        seqno = self.lsn2seqno(lsn)

        while self.lsn2seqno(lsn) == seqno:

            page_offset = self.lsn2page(lsn)

            page_data = self.get_log_page(page_offset)
            page_header = layout.RecordPageHeader.from_buffer_copy(page_data)

            if self.lsn2seqno(page_header.copy.last_lsn) < seqno:
                # Page with older records was hit, stop enumeration
                raise StopIteration("sequence numbers don't match")

            offset = self.lsn2poffset(lsn)

            record = layout.LogRecord.from_buffer_copy(
                page_data[offset:offset + sizeof(layout.LogRecord)])

            assert record.this_lsn == lsn, \
                   "invalid LSN 0x%X, expected 0x%X" % (record.this_lsn, lsn)

            # Get client data for current LSN.
            # Also remember the file offset of the last byte of client data,
            # which tells us, where next LSN lays and if the sequence number
            # has to be incremented.
            client_data = b''

            client_data_offset = qalign(
                self.lsn2poffset(lsn) + sizeof(layout.LogRecord))
            client_data_last_byte = page_offset + client_data_offset

            if record.client_data_length > 0:

                # Each cycle processes one page, till all users
                # data are not read
                while True:
                    page_data = self.get_log_page(page_offset)

                    page_header = layout.RecordPageHeader.from_buffer_copy(
                        page_data)

                    # Check if page has correct sequence number.
                    # In case of corrupted logfile, sequence can be lower.
                    if self.lsn2seqno(page_header.copy.last_lsn) != seqno:
                        raise StopIteration("incomplete record found")

                    page_remaining_bytes = self.lcb.log_page_size - client_data_offset

                    if page_remaining_bytes > 0:
                        bytes_to_copy = min(page_remaining_bytes,
                                            record.client_data_length - len(client_data))

                        client_data += page_data[client_data_offset:\
                                                 client_data_offset + bytes_to_copy]

                        client_data_last_byte = page_offset + \
                            (bytes_to_copy - 1) + client_data_offset

                    if len(client_data) == record.client_data_length:
                        break

                    page_offset = self.next_log_page(page_offset)
                    # More data are needed, check if sequence number
                    # has to be adjusted.
                    if page_offset < client_data_last_byte:
                        seqno += 1

                    client_data_offset = self.lcb.log_page_data_offset

            yield record, client_data

            # Now get the next LSN. This is pretty straightforward sice we
            # have the position of the client data last byte. New LSN record
            # starts from client_data_last_byte + 1. But in case the current
            # LSN spans more pages, we have to check if log does not wrap.
            if page_header.copy.last_lsn == lsn:
                # There are no more LSN on the page, where client data for 
                # current LSN ends. So the next LSN is on the next page.
                next_page_offset = self.next_log_page(
                    self.foffset2page(client_data_last_byte))

                # If offset of the next page is lower than the offset of the
                # client data last byte for current LSN, log wrapped.
                if next_page_offset < client_data_last_byte:
                    seqno += 1

                # New LSN can be computed from file offset.
                lsn = self.foffset2lsn(
                    next_page_offset + self.lcb.log_page_data_offset,
                    seqno)
            else:
                # Next LSN resides on the same page as the end of current LSN.
                lsn = self.foffset2lsn(qalign(client_data_last_byte + 1), seqno)

    def get_restart_table(self, lsn):
        '''Returns restart table from `lsn`.'''

        _, data = next(self.records(lsn))

        client_header = layout.ClientLogHeader.from_buffer_copy(data)

        table = layout.RestartTable.from_buffer_copy(
            data[client_header.redo_offset:\
                 client_header.redo_offset + sizeof(layout.RestartTable)])

        return table, data[client_header.redo_offset + sizeof(layout.RestartTable):]

    def get_client_restart_area(self, client_name="NTFS"):
        '''Returns restart area for client.'''

        for client in self.log_clients.clients:
            if client_name == client.get_name():
                break
        else:
            raise ValueError("no such client")

        if client.client_restart_lsn == 0:
            return None, None

        _, data = next(self.records(client.client_restart_lsn))
        return layout.RestartArea.from_buffer_copy(data)

    def get_client_restart_context(self, client_restart_area):
        '''Returns clients restart context.

        Restart context describes all changes not flushed
        on disk. Context is stored as log record and consists
        from:
            - checkpoint LSN
            - transactions table
            - open attributes table
            - dirty pages table
            - attribute names

            ~                 ~
         +->|                 |
         |  |                 |
      +--|->|                 |
      |  |  |                 |
      |  |  +-------PAGE------+
      |  |  |                 |<----+
      |  |  |                 |     |
   +--|--|->|                 |     |
   |  |  |  |                 |     |
   |  |  |  |                 |     |
   |  |  |  |                 |<----|----+
   |  |  |  +-------PAGE------+     |    |
   |  |  |  |                 |     |    |
   |  |  |  +-----------------+<--|current LSN
   |  |  +--|checkpoint LSN   |    (from restart area)
   |  +-----|transactions LSN |     |    |
   +--------|dirty pages LSN  |     |    |
            |open attrs. LSN  |-----+    |
            |attr. names LSN  |----------+
            +-----------------+
            ~                 ~

        Checkpoint LSN - highest LSN in the log, which changes
        are recorded in the context tables.

        Transaction table - holds uncommited transactions in
        the moment, when context was about to be written.
        Typically is empty.

        Dirty pages table - holds pages describing ranges of
        clusters (with NTFS metadata) which are changed but
        those changes are not flushed.

        Open attributes table - holds attributes, whose clusters
        are changed (recorded in dirty pages table).

        Attributes names - holds names of attributes in open
        attributes table (if any).
        '''

        attribute_names = []
        dirty_pages = []
        transactions = {}
        open_attributes = {}

        if client_restart_area.attr_names_len > 0:
            _, data = next(self.records(client_restart_area.attr_names_lsn))
            attribute_names = parse_attribute_names(data)

        if client_restart_area.dirty_pages_table_len > 0:
            dirty_pages = parse_dirty_pages(
                *self.get_restart_table(client_restart_area.dirty_pages_table_lsn))

        if client_restart_area.open_attr_table_len > 0:
            open_attributes = parse_open_attributes(
                *self.get_restart_table(client_restart_area.open_attr_table_lsn))

        if client_restart_area.transaction_table_len > 0:
            transactions = parse_transactions(
                *self.get_restart_table(client_restart_area.transaction_table_lsn))

        return ClientRestartContext(
            client_restart_area.start_of_checkpoint,
            attribute_names,
            dirty_pages,
            open_attributes,
            transactions)
