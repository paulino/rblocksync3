#!/usr/bin/env python3
import sys
import os
import hashlib
import subprocess
import json
import signal
import logging as log

from math import ceil
from time import time,sleep
from threading import Thread
from queue import Queue
from optparse import OptionParser, SUPPRESS_HELP

from collections.abc import Callable

VERSION = "1.0.0"

# Some constants

TMP_DIR = "/tmp"
BASE_BLOCK_SIZE = 2**12 # 4096 bytes base block size and multiplier
CHUNK_SIZE = 2**20  # 1 MB chunk size for block transfers
MAX_QUEUE_SIZE = 6000  # Max blocks in hash-blocks queue
SENTINEL = object()  # Sentinel with a unique id

# Global variables at module level

log.basicConfig(
    level=log.INFO,
    format='[%(levelname)s] %(message)s',
    stream=sys.stdout
)
zero_hash = hashlib.sha256(b'\0').hexdigest() # To count zero blocks (set later)
options = None # Arg parser options object
total_blocks = 0
blocks_queue_src = Queue()
blocks_queue_dst = Queue()
stop_signal = False
stats = {
    'scanned_blocks': {},
    'clean_blocks': 0,
    'updated_blocks': 0,
    'start_time': 0,
    'zero_blocks': 0
}

class RBlockSyncException(Exception):
    """ Custom exception for RBlockSync errors """
    pass

class BSFile:
    """ Interface / Abstract class for block sync file operations"""
    
    # Master SSH processes, can be two (src/dst)
    master_procs = {} 
    
    def __init__(self, uri:str):
        self.uri = uri
        # Via command line options
        self.block_size = options.block_size * BASE_BLOCK_SIZE

    def connect(self) -> None: ...
    def disconnect(self) -> None: ...   
    def open(self) -> None: ...
    def close(self) -> None: ...
    @property 
    def size(self) -> int: ...
    def exists(self) -> bool: ...
    def truncate(self, size: int) -> None: ...
    def hash_block(self, block_no) -> list: ...
    def scan(self, block_no:int, blocks:int, callback: Callable = ... ) -> None: ...
    def get_block(self, block_no : int , size : int) -> bytes: ...
    def put_block(self, block_no : int, data: bytes) -> None: ...
    def start_server(self) -> None: ...
    
    def clone(self) -> 'BSFile':
        return BSFile.create(self.uri)
    
    @staticmethod
    def create(uri) -> 'BSFile':
        """ Object factory"""
        if ':' in uri:
            return BSRemoteFile(uri)
        else:
            return BSLocalFile(uri)
    
    @staticmethod
    def cleanup_master_procs() -> None:
        for user_host, proc in BSFile.master_procs.items():
            if proc.poll() is None:                
                subprocess.run(["ssh","-q","-S",f"{TMP_DIR}/{user_host}-blocksync3.sock","-O","exit",user_host], check=True)
        BSFile.master_procs = {}
    

class BSLocalFile(BSFile):
    
    def __init__(self, path:str):
        super().__init__(uri=path)
        self.path = path
        self.is_remote = False
        self.file_handle = None

    def open(self):
        if not self.exists():
            raise RBlockSyncException(f"File {self.path} does not exist")
        if self.file_handle is None:
            self.file_handle = open(self.path, 'r+b')
    
    def close(self):
        if self.file_handle:
            self.file_handle.close()
            self.file_handle = None
    
    
    @property
    def size(self):
        file_handle = open(self.path, 'rb')
        file_handle.seek(0, os.SEEK_END)
        file_size = file_handle.tell()
        file_handle.close()
        return file_size
    
    def exists(self):
        return os.path.exists(self.path)
    
    def truncate(self, size: int) -> None:
        with open(self.path, 'a+b') as f:
            f.truncate(size)

    def hash_block(self, block_no) -> list:
        self.file_handle.seek(block_no * self.block_size)
        block = self.file_handle.read(self.block_size)
        return [block_no,len(block),
            hashlib.sha256(block).hexdigest()]
     
    def scan(self,block_no:int, blocks:int, callback = None) -> None:
        """ Scan must run in other instance """
        self.open()
        self.file_handle.seek(0, os.SEEK_END)
        file_size = self.file_handle.tell()
        max_blocks = (file_size + self.block_size - 1) // self.block_size - block_no
        blocks = min(blocks, max_blocks)
        for index in range(block_no, block_no + blocks):
            res = self.hash_block(index)
            if callback:
                callback(res)
            else:
                print(json.dumps(res),flush=True)
        if callback is None:
            print("eof",flush=True)
        self.close()

    def get_block(self, block_no : int, size : int) -> bytes:
        self.file_handle.seek(block_no * self.block_size)
        return self.file_handle.read(size)
    
    def put_block(self, block_no: int, data: bytes) -> None:
        self.file_handle.seek(block_no * self.block_size)
        self.file_handle.write(data)
        self.file_handle.flush()
    
    def start_server(self) -> None:
        while True:
            line = sys.stdin.readline()
            if line == '':
                break
            line = line.strip()
            if line == 'quit':
                break
            elif line == "exists":
                res = self.exists() and "true" or "false"
                sys.stdout.write(f"{res}\n")
                sys.stdout.flush()
            elif line == 'open':
                self.open()
                sys.stdout.write("true\n")
                sys.stdout.flush()
            elif line[0:9] == 'truncate ':
                size = int(line[9:].strip())
                self.truncate(size)
                sys.stdout.write("true\n")
                sys.stdout.flush()
            elif line == 'size':
                file_size = self.size
                sys.stdout.write(f"{file_size}\n")
                sys.stdout.flush()
            elif line[0:5] == 'hash ':
                block_no = int(line[5:].strip())
                res = self.hash_block(block_no)
                sys.stdout.write(json.dumps(res)+"\n")
                sys.stdout.flush()
            elif line[0:5] == 'scan ':
                parts = line[5:].strip().split(' ')
                if len(parts) != 2:
                    raise Exception("Invalid scan command format")
                block_no = int(parts[0])
                blocks = int(parts[1])
                self.scan(block_no, blocks)
            elif line[0:4] == 'get ':
                params = line[4:].strip().split(' ',1)
                if len(params) != 2:
                    raise Exception("Invalid get command format")
                block_no = int(params[0])
                size = int(params[1])
                block = self.get_block(block_no, size)
                sys.stdout.buffer.write(block)
                sys.stdout.buffer.flush()
            elif line[0:4] == 'put ':
                block_no , size_str = line[4:].strip().split(' ',1)
                block_no = int(block_no)
                size = int(size_str)
                sys.stdout.write("ready\n")
                sys.stdout.flush()            
                data = sys.stdin.buffer.read(size)
                if len(data) != size:
                    raise Exception(f"Expected {size} bytes but received {len(data)} bytes")                
                self.put_block(block_no, data)
                sys.stdout.write("true\n")
                sys.stdout.flush()
            elif line == 'version': # To check if remote version matches
                sys.stdout.write(f"{VERSION}\n")
                sys.stdout.flush()      


class BSRemoteFile(BSFile):
    
    def __init__(self, uri:str):
        super().__init__(uri=uri)        
        self.is_remote = True
        self.user_host, self.path = uri.split(':',1)
        self.process_server = None        
        self.master_socket = f"{TMP_DIR}/{self.user_host}-blocksync3.sock"
        local_file = os.path.abspath(__file__)
        self.remote_script = "/tmp/" + os.path.basename(local_file)
                
    def connect(self) -> None:
        """ Master SSH connection over socket """
        master_proc = BSRemoteFile.master_procs.get(self.user_host, None)
        if master_proc is None:
            if os.path.exists(self.master_socket):
                os.remove(self.master_socket)
            master_proc = subprocess.Popen(
                ["ssh", "-M", "-S", self.master_socket, self.user_host, "-N"],
                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, 
                stderr=subprocess.PIPE,
                close_fds=True
            )
            BSFile.master_procs[self.user_host] = master_proc
            while not os.path.exists(self.master_socket):
                master_proc.poll()
                sleep(0.1)
                if master_proc.returncode is not None:
                    err = master_proc.stderr.read().decode().strip()
                    raise RBlockSyncException(f"SSH connection fails: {err}")
            # Copy script to remote at /tmp
            local_file = os.path.abspath(__file__)
            try:
                subprocess.run(["scp","-q", "-o",
                                f"ControlPath={self.master_socket}", local_file, 
                                self.user_host+":/tmp"], check=True)
            except subprocess.CalledProcessError as e:
                raise Exception(f"Error when copying the file: {e} via SSH")
        # Now master
        self.start_server()


    def disconnect(self) -> None:
        self.stop_server()
                    
    def start_server(self) -> None:
        """ Start a remote instance this script in server mode via ssh """
        if self.process_server:
            return  # Already started
        try:
            self.process_server = subprocess.Popen(
                ["ssh", "-S", self.master_socket, self.user_host,
                    f"python3 {self.remote_script} --server --block-size {options.block_size} {self.path} "],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        except subprocess.CalledProcessError as e:
            raise Exception(f"Error executing script remotely: {e.stderr}")

    def stop_server(self) -> None:
        if self.process_server is None:
            return  # Not started
                    
        if self.process_server.poll() is not None:
            return  # Already stopped
        try:
            self.process_server.stdin.write('quit\n'.encode())
            self.process_server.stdin.flush()
            self.process_server.stdin.close()
            self.process_server.wait()
        except BrokenPipeError:
            pass  # Process already terminated

            
    def scan(self,block_no: int, blocks: int, callback: Callable = None ) -> None:
        # Remote scan over other ssh connection
        self.process_server.stdin.write(f"scan {block_no} {blocks}\n".encode())
        self.process_server.stdin.flush()
        for line in self.process_server.stdout:
            if line.strip() == b"eof":
                break
            block_info = json.loads(line)                        
            callback(block_info)
            if self.process_server.returncode != None and self.process_server.returncode != 0:
                err = self.process_server.stderr.read()
                raise Exception(f"Remote scan error: {err}")

    def open(self) -> None:
        self.process_server.stdin.write(b"open\n")
        self.process_server.stdin.flush()
        line = self.process_server.stdout.readline()  # Read acknowledgment
        if line != b"true\n":
            raise Exception("Error opening remote file: " + line.decode().strip())
        
    def exists(self) -> bool:
        self.process_server.stdin.write(b"exists\n")
        self.process_server.stdin.flush()
        line = self.process_server.stdout.readline()
        if line == b"true\n":
            return True
        else:
            return False
        
    def truncate(self, size: int) -> None:
        self.process_server.stdin.write(f"truncate {size}\n".encode())
        self.process_server.stdin.flush()
        line = self.process_server.stdout.readline()  # Read acknowledgment
        if line != b"true\n":
            raise Exception("Error truncating remote file: " + line.decode().strip())
        
    def hash_block(self, block_no: int) -> list:
        """ return list: [block_no, block_size, hash_block] """
        self.process_server.stdin.write(f"hash {block_no}\n".encode())
        self.process_server.stdin.flush()
        line = self.process_server.stdout.readline()
        block_info = json.loads(line)
        return block_info
        

    def get_block(self, block_no: int, size: int) -> bytes:
        self.process_server.stdin.write(f"get {block_no} {size}\n".encode())
        self.process_server.stdin.flush()
        block_data = self.process_server.stdout.read(size)
        return block_data
    
    def put_block(self, block_no: int, data: bytes) -> None:
        self.process_server.stdin.write(
            f"put {block_no} {len(data)}\n".encode())
        self.process_server.stdin.flush()
        line = self.process_server.stdout.readline()  # Read ready
        if line != b"ready\n":
            raise Exception("Error preparing to write block remotely: " + line.decode().strip())
        self.process_server.stdin.write(data)
        self.process_server.stdin.flush()
        line = self.process_server.stdout.readline()  # Read acknowledgment
        if line != b"true\n":
            raise Exception("Error writing block remotely: " + line.decode().strip())

    @property
    def size(self) -> int:
        self.process_server.stdin.write(b"size\n")
        self.process_server.stdin.flush()
        # Wait until a non-empty line is received
        while True:
            size_line = self.process_server.stdout.readline()
            if size_line:
                break
        if b"Error" in size_line:
            raise Exception(f"Error from remote server: {size_line.strip()}")
        return int(size_line.strip())

def pretty_size(size_bytes:int ) -> str:
    """ Human readable size """
    if size_bytes == 0 or size_bytes < 1024:
        return "0 B"
    if size_bytes < 1024*1024:
        return f"{size_bytes / 1024:.2f} KB"
    if size_bytes < 1024*1024*1024:
        return f"{size_bytes / (1024*1024):.2f} MB"    
    return f"{size_bytes / (1024*1024*1024):.2f} GB"
    
        
def scan_worker(src_uri : str, blocks_queue: Queue) -> None:
    """ Scan file blocks and put them in the queue
        if one file is remote, scan may run the remote ssh to get performance
    """
    # clone bsfile to avoid thread conflicts
    scan_instance = BSFile.create(src_uri)
    current_block = 0
    
    def callback(block_info):
        blocks_queue.put(block_info)
        stats['scanned_blocks'][src_uri] += 1
    
    scan_instance.connect()
    while current_block < total_blocks and not stop_signal:
        scan_instance.scan(current_block, MAX_QUEUE_SIZE, callback)
        current_block += MAX_QUEUE_SIZE
        while blocks_queue.qsize() >= MAX_QUEUE_SIZE and not stop_signal:
            sleep(0.1)
    scan_instance.disconnect()
    blocks_queue.put(SENTINEL)
                     
def updater_worker(src_uri: str, dst_uri: str):
    dst_bsfile = BSFile.create(dst_uri)
    src_bsfile = BSFile.create(src_uri)
    dst_bsfile.connect()
    src_bsfile.connect()
    dst_bsfile.open()
    src_bsfile.open()
    block_no = 0
    pending_size = 0
    
    def flush_pending():
        nonlocal pending_size, block_no
        if pending_size > 0:
            if not options.dry_run:
                block_data = src_bsfile.get_block(block_no,pending_size)
                dst_bsfile.put_block(block_no,block_data)
            pending_size = 0
    
    while not stop_signal:
        block_info_src = blocks_queue_src.get()
        block_info_dst = blocks_queue_dst.get()
        if block_info_src is SENTINEL or block_info_dst is SENTINEL:
            flush_pending()
            break
        # Unpack block info
        block_no_src, block_size_src, hash_block_src = block_info_src
        block_no_dst, block_size_dst, hash_block_dst = block_info_dst
        if block_no_src != block_no_dst:
            raise RBlockSyncException("Internal error: block numbers do not "\
                "match between source and destination")
        if hash_block_src == hash_block_dst:
            stats['clean_blocks'] += 1            
            if hash_block_src == zero_hash:
                stats['zero_blocks'] += 1            
            flush_pending()            
        else:
            stats['updated_blocks'] += 1
            if pending_size == 0:
                block_no = block_no_src
            pending_size += block_size_src
        if pending_size >= CHUNK_SIZE:
            flush_pending()
        blocks_queue_src.task_done()
        blocks_queue_dst.task_done()
    dst_bsfile.close()
    src_bsfile.close()
    dst_bsfile.disconnect()
    src_bsfile.disconnect()


def print_stats_worker(src_uri: str, dst_uri: str) -> None:
    
    def print_stats( end='') -> None:
        
        block_size = options.block_size * BASE_BLOCK_SIZE
        percent_done = ceil((stats['updated_blocks'] + 
                             stats['clean_blocks']) * 100 / total_blocks)
        scanned_src_blocks = stats['scanned_blocks'][src_uri]
        scanned_dst_blocks = stats['scanned_blocks'][dst_uri]
        scanned_blocks = max(scanned_src_blocks, scanned_dst_blocks)
        scanned_gb = (scanned_blocks * block_size) / (1024*1024*1024)
        updated_gb = (stats['updated_blocks'] * block_size) / (1024*1024*1024)
        clean_gb = (stats['clean_blocks'] * block_size) / (1024*1024*1024)
        zero_gb = (stats['zero_blocks'] * block_size) / (1024*1024*1024)
        display = f"\r[{percent_done}%] B-Scan: {scanned_blocks} "\
            f"({scanned_gb:.2f} GB), "\
            f"Z-Blks: {stats['zero_blocks']} ({zero_gb:.2f} GB), "\
            f"Clean: {stats['clean_blocks']} ({clean_gb:.2f} GB), "\
            f"Updated: {stats['updated_blocks']} ({updated_gb:.2f} GB)  "
        print(display, end=end)    
    
    while not stop_signal:
        print_stats(end='')
        sleep(0.25)
    print_stats(end='\n')
        
    
def regular_sync(src_uri:str,dst_uri:str) -> None:
    """ Regular sync between source and destination files
    """
    global stop_signal,zero_hash,total_blocks
    stats['start_time'] = time()
    zero_hash = hashlib.sha256(b'\0'*(options.block_size * BASE_BLOCK_SIZE)).hexdigest()
    src_file = BSFile.create(src_uri)
    dst_file = BSFile.create(dst_uri)
        
    src_file.connect()
    dst_file.connect()

    if not dst_file.exists():
        if not options.truncate:
            raise RBlockSyncException("Destination file does not exist. Please create it first or user option --truncate")
        dst_file.truncate(src_file.size)
        log.info(f"Created destination file {dst_uri} with size {pretty_size(src_file.size)}")
    
    if options.dry_run:
        log.warning("Dry run mode enabled. No changes will be made to destination file.")        
    log.info(f"Source  size: {pretty_size(src_file.size)}")
    log.info(f"Dest.   size: {pretty_size(dst_file.size)}")
    log.info(f"Block   size: {pretty_size(options.block_size * BASE_BLOCK_SIZE)}")
        
    if src_file.size != dst_file.size:
        if src_file.size > dst_file.size:
            log.warning(f"Source is larger than destination. Diff {src_file.size - dst_file.size} bytes.")
        else:
            log.warning(f"Destination is larger than source. Diff {dst_file.size - src_file.size} bytes.")
    if not options.ignore_size and src_file.size != dst_file.size:
        if not options.truncate:
            raise RBlockSyncException("Source/destination sizes differ, you can use --truncate or --ignore-size to force sync")
        dst_file.truncate(src_file.size)
        if dst_file.size != src_file.size:
            raise RBlockSyncException("Failed to truncate destination file to source file size. Destination may be a block device?")
    
    
    total_blocks = int(src_file.size / (options.block_size * BASE_BLOCK_SIZE)) + 1
    log.info(f"Total blocks: {total_blocks}")
    stats['scanned_blocks'][src_uri] = 0
    stats['scanned_blocks'][dst_uri] = 0

    src_file.disconnect()
    dst_file.disconnect()
    
    ## Workers    
    
    th_scan_src = Thread(target=scan_worker, args=(src_uri,blocks_queue_src))
    th_scan_dst = Thread(target=scan_worker, args=(dst_uri,blocks_queue_dst))    
    th_updater = Thread(target=updater_worker, args=(src_uri,dst_uri))
    th_stats = Thread(target=print_stats_worker, args=(src_uri,dst_uri))
    
    th_scan_src.start()
    th_scan_dst.start()        
    th_updater.start()
    
    if not options.quiet:
        th_stats.start()
    
    th_scan_src.join() # Wait until scanning is done
    th_scan_dst.join()        
    th_updater.join()     
    stop_signal = True
    if not options.quiet:
        th_stats.join()  
    BSFile.cleanup_master_procs()
    elapsed = int(time() - stats['start_time'])
    hours, remainder = divmod(elapsed, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours > 0:
        elapsed_str = f"{hours}h {minutes}m {seconds}s"
    elif minutes > 0:
        elapsed_str = f"{minutes}m {seconds}s"
    else:
        elapsed_str = f"{seconds}s"
    log.info(f"Finished. Elapsed time: {elapsed_str}")
    


def handle_sigint(signum: int, frame) -> None:
    global stop_signal
    if stop_signal:
        print("\nForced termination. Exiting immediately ...\n\n")
        sys.exit(1)    
    stop_signal = True
    print("\nInterrupted by user. Terminating processes please wait some seconds ...\n")
    

if __name__ == "__main__":
    signal.signal(signal.SIGINT, handle_sigint)
    parser = OptionParser(usage="usage: %prog [options] source_file destination_file",
                          version="%prog 1.0")
    # server mode is only used to serve via ssh
    parser.add_option("--server", dest="server", action="store_true" ,default=False,
                    help=SUPPRESS_HELP)
    parser.add_option("--truncate", dest="truncate", action="store_true", default=False,
                    help="Truncate/Create destination file to source file size before syncing")
    parser.add_option("--dry-run", dest="dry_run", action="store_true", default=False,
                      help="Perform a dry run without making any changes")
    parser.add_option("--block-size", dest="block_size", type="int", default=1,
                      help="Set the block size in 4K multiples (default: 1)")
    parser.add_option("--ignore-size", dest="ignore_size", action="store_true", default=False,
                      help="Ignore size differences between source and destination files")
    parser.add_option("-q", "--quiet", dest="quiet", action="store_true", default=False,
                      help="Suppress all logging output")

    (options, args) = parser.parse_args()
    
    if options.block_size <= 0:
        parser.print_help()
        sys.exit(1)
        
    if options.quiet:
        log.getLogger().setLevel(log.ERROR)
    
    if options.server: # This is an instance server to serve a file via ssh
        if len(args) != 1:
            parser.print_help()
            sys.exit(1)
        bf = BSFile.create(args[0])
        try:
            bf.start_server()
        except Exception as e:
            log.error(f"Error occurred while starting server: {e}")
    elif len(args) != 2:
            parser.print_help()
            sys.exit(1)
    else:
        try:
            # Regular use case: sync src to dst
            regular_sync(args[0], args[1])
        except RBlockSyncException as e:
            log.error(e)