'''$LogFile parser.

Author:
    P. Polakovic
'''
import argparse
import logfile
import layout
import os

def main():
    '''Things the mains do...'''

    parser = argparse.ArgumentParser(description="Does $LogFile things",
                                     epilog="Author: P. Polakovic")

    parser.add_argument("-f", "--file", nargs=1, required=True,
                               help="Path to NTFS logfile", type=str)

    args = parser.parse_args()

    with open(args.file[0], "rb+") as logfile_stream:
        lrb, lrbb = logfile.get_lsn_restart_blocks(logfile_stream)

        print("Journal version: {}.{}".format(
            lrb.header.major_ver, lrb.header.minor_ver))
        print("System page size:", hex(lrb.header.system_page_size))
        print("Log page size:", hex(lrb.header.log_page_size))
        print("-- SNAPSHOT INFO --")
        print("Current LSN:", hex(lrb.area.current_lsn))
        print("Clients:", lrb.area.log_clients)

        for client in lrb.clients.clients:
            print("\tClient name:", client.get_name())
            print("\tClient's restart LSN:", hex(client.client_restart_lsn))
            print("\tClient's sequence number:", client.seq_number)

        print("Sequence:", lrb.area.seq_number_bits)
        print("File size:", hex(lrb.area.file_size))

        print("-- SNAPSHOT INFO BACKUP --")
        print("Current LSN:", hex(lrbb.area.current_lsn))

        for client in lrbb.clients.clients:
            print("Client:", client.get_name())
            print("\tClient's restart LSN:", hex(client.client_restart_lsn))
            print("\tClient's sequence number:", client.seq_number)

        print("Sequence:", lrbb.area.seq_number_bits)
        print("File size:", hex(lrbb.area.file_size))

        journal = logfile.LogFile(logfile_stream, lrbb)

        logfile_stream.seek(journal.lcb.system_page_size << 1)

        pages_file = os.path.join(os.path.dirname(args.file[0]),
                                  "pages.txt")

        with open(pages_file, "w") as pages_stream:
            pages_stream.write("Page offset;Last LSN;Last end LSN;Flags\n")

            npages = (journal.lcb.file_size - logfile_stream.tell()) / \
                journal.lcb.log_page_size

            for _ in range(int(npages)):
                page = logfile_stream.read(journal.lcb.log_page_size)

                page_header = layout.RecordPageHeader.from_buffer_copy(page)

                pages_stream.write(
                    "{:>10};{:>18};{:>18};{}\n".format(
                        hex(logfile_stream.tell() - journal.lcb.log_page_size),
                        hex(page_header.copy.last_lsn),
                        hex(page_header.last_end_lsn),
                        hex(page_header.flags)))

        restart_area = journal.get_client_restart_area("NTFS")

        print("-- CLIENT RESTART INFO --")
        print("Version:", "{}.{}".format(restart_area.major_version,
                                         restart_area.minor_version))
        print("Checkpoint LSN:", hex(restart_area.start_of_checkpoint))
        print("Open attributes LSN:", hex(restart_area.open_attr_table_lsn))
        print("Attribute names LSN:", hex(restart_area.attr_names_lsn))
        print("Dirty pages LSN:", hex(restart_area.dirty_pages_table_lsn))
        print("Transaction table LSN:", hex(restart_area.transaction_table_lsn))

        pages_file = os.path.join(os.path.dirname(args.file[0]),
                                  "working_set_records.txt")

        with open(pages_file, "w") as records_stream:
            records_stream.write("""LSN;Previous LSN;Undo next LSN;"""
                                 """Redo operation;Undo operation;Transaction\n""")

            for record, data in journal.records(lrbb.area.current_lsn):
                client_header = layout.ClientLogHeader.from_buffer_copy(data)

                records_stream.write(
                    "{:>18};{:>18};{:>18};{};{};{}\n".format(
                        hex(record.this_lsn),
                        hex(record.client_previous_lsn),
                        hex(record.client_undo_next_lsn),
                        layout.LOG_OPERATION.get(client_header.redo_op),
                        layout.LOG_OPERATION.get(client_header.undo_op),
                        record.transaction_id))

if __name__ == "__main__":
    main()