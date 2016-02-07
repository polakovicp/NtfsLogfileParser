'''Reads NTFS journal and writes it's content
into current directory.

Author:
    P. Polakovic
'''

import argparse
import os
import layout
import ctypes

from struct import unpack
from utils import ffs


def parse_runlist(data, vcn, runlist_offset=0):
    '''Returns parsed data runs.

    For decompressing alchemy thank goes to dkovar (https://github.com/dkovar)
    who apparently "stole" the idea from Willi Ballenthin :)
    '''
    def to_positive_le_int(buf):
        '''Parses positive integer from `buf`.'''
        ret = 0
        for i, byte in enumerate(buf):
            ret += byte * (1 << (i * 8))
        return ret

    def to_negative_le_int(buf):
        '''Parses negative integer from `buf`.'''
        ret = 0
        for i, byte in enumerate(buf):
            ret += (byte ^ 0xFF) * (1 << (i * 8))
        ret += 1
        ret *= -1
        return ret

    def to_le_int(buf):
        '''Parses integer from `buf`.'''
        if not buf[-1] & 0b10000000:
            return to_positive_le_int(buf)
        else:
            return to_negative_le_int(buf)

    runlist = []
    lcn = 0

    while data[runlist_offset]:
        size = data[runlist_offset]

        runlist_offset += 1

        len_nibble = size & 0x0F
        runlist_length = 0

        for pos in range(len_nibble):
            runlist_length |= data[runlist_offset] << (pos * 8)
            runlist_offset += 1

        lcn_nibble = (size & 0xF0) >> 4

        if lcn_nibble > 0:
            if lcn is None:
                lcn = 0
            # not a sparse
            lcn += to_le_int(data[runlist_offset:runlist_offset + lcn_nibble])
        else:
            lcn = None

        runlist_offset += lcn_nibble

        runlist.append((vcn, lcn, runlist_length))
        vcn += runlist_length
    return runlist


def find_data_stream(segment):
    while len(segment) > 4 and segment[:4] != b'\xff\xff\xff\xff':
        attribute_length = unpack("<L", segment[4:8])[0]

        if segment[:4] == b'\x80\x00\x00\x00':
        
            attribute = layout.NonResidentAttributeRecord.from_buffer_copy(
                segment[:attribute_length])

            setattr(attribute, "runlist",
                    parse_runlist(
                        segment,
                        attribute.lowest_vcn,
                        attribute.mapping_pairs_offset))

            return attribute

        segment = segment[attribute_length:]


def dump_logfile(volume_path, path):
    with open(volume_path, "rb") as volume:
        bootsector = layout.NtfsBootSector.from_buffer_copy(
            volume.read(ctypes.sizeof(layout.NtfsBootSector)))

        cluster_size = bootsector.bpb.bytes_per_sector * bootsector.bpb.sectors_per_cluster

        if bootsector.clusters_per_mft_record < 0:
            file_record_size = 1 << -bootsector.clusters_per_mft_record
        else:
            file_record_size = (bootsector.clusters_per_mft_record) << \
                (layout.ffs(cluster_size) - 1)
            
        logfile_offset = bootsector.mft_lcn * cluster_size
        logfile_offset += 2 * file_record_size

        volume.seek(logfile_offset)
        
        buffer = bytearray(file_record_size)

        volume.readinto(buffer)
        layout.dofixup(buffer, sector_size=bootsector.bpb.bytes_per_sector)

        file_record = layout.FileRecordSegmentHeader.from_buffer_copy(buffer)
        
        data_stream = find_data_stream(buffer[file_record.attr_offset:])

        with open(path, "wb") as logfile_stream:
            for vcn, lcn, length in data_stream.runlist:
                volume.seek(lcn * cluster_size)

                while length > 0:
                    num = max(length, 64)
                    logfile_stream.write(volume.read(num * cluster_size))
                    length -= num
                    

def main():
    '''Things the mains do...'''

    parser = argparse.ArgumentParser(description="Dumps NTFS journal",
                                     epilog="Author: P. Polakovic")

    parser.add_argument("-p", "--path", nargs=1, required=True,
                        help=r"Path to volume (for example \\.\C:)", type=str)
    args = parser.parse_args()

    dump_logfile(args.path[0], "$LogFile")

if __name__ == "__main__":
    main()

