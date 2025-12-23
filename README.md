# rblocksync3

Remote block-level file/device synchronization tool. 

*rblocksync3* is a Python tool for synchronization using block-level hashing. It
transfers only the changed blocks between files/devices. It supports large files
and devices over SSH connections. It is optimized for zero blocks, such as those
found in lvm-thin or sparse files.

Features:
- Block-level synchronization
- Keeps zero-blocks for lvm-thin devices or sparse files
- Remote and local file support
- Multi-threaded scanning and transfers
- SSH-based remote operations

It is inspired by [theraser/blocksync](https://github.com/theraser/blocksync)
but rewritten in Python3.

## Use cases

It has been created mainly for copying virtual machine images between
heterogeneous environments: Proxmox, libvirt, etc. It can be used for any file
or block device. Some examples of use cases are shown below.

Synchronizes a remote device to a local device that are the same size.

```bash
rblocksync3 user@remote_host:/dev/sda /dev/sdb
```

Synchronizes a remote device to a local sparse file. The local file is created
if it does not exist or is truncated when the destination size does not match.

```bash
rblocksync3 --truncate user@remote:/dev/lvm/vol myfile.img
``` 

Also, it is possible to sync remote-remote, although it is not optimal for
performance.

```bash
rblocksync3 user1@remote1:/dev/sda user2@remote2:/dev/sdb
```

Local-to-local sync is also supported.

```bash
rblocksync3 /dev/sdc /lib/libvirt/images/image.img
```

Other options are in the [manpage](manpage.rst)

## Installation

rblocksync3 requires Python 3 and SSH in both extremes. The script is copied and
executed via SSH in the remote host, acting as an *rblocksync* server.

The script ```rblocksync3.py``` can be downloaded and executed directly with
Python 3 interpreter, and also, there is a Debian package in the releases
section

## Internal details

rblocksync3 scans the file or the device using a default block size of 4 KB. The
blocks are hashed using the SHA-256 algorithm at both extremes. A block size of
4 KB is optimal for most file systems containing unallocated blocks
(zero-blocks).

Increasing the block size improves the transfer performance. However, larger
block sizes lose the advantage of zero blocks detection. An option is available
for this purpose.

## License

The project is licensed under the Apache 2.0 license.

## Author

Developed and maintained by Paulino Ruiz de Clavijo Vázquez <pruiz@us.es>
