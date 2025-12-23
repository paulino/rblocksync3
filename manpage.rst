============
rblocksync3
============

--------------------------------------------
Remote block-level file synchronization tool
--------------------------------------------

:Manual section: 1
:Manual group: User Commands

SYNOPSIS
========

**rblocksync3** [*options*] *source_file* *destination_file*

Description
===========

**rblocksync3** is a Python tool for synchronization using block-level hashing.
It transfers only the changed blocks between files/devices. It supports large
files and devices over SSH connections. It is optimized for zero blocks, such as
those found in lvm-thin or sparse files.

Features:
   - Block-level synchronization
   - Keeps zero blocks for lvm-thin devices or sparse files
   - Remote and local file support
   - Multi-threaded scanning and transfers
   - SSH-based remote operations

Options
=======

-h, --help

   Show help message and exit.

-q, --quiet

   Suppress all logging output except errors.

--dry-run

   Run the synchronization process without making any changes. It is useful for
   previewing differences and counting zero blocks.

--block-size SIZE
   
   Set the multiplier for the 4KB base block size. Default is 1 (4KB).

--truncate

   Create or truncate the destination file to match the source size. This
   is only possible when the destination is a file.

--ignore-size

   Force synchronization when the source and destination files differ in size.
   The synchronization size will be that of the smaller file or device.


Examples
========

Sync local to local::

    $ rblocksync3 /dev/sdc /path/to/destination.img

Sync a remote device to a local file over SSH::

    $ rblocksync3 user@host:/dev/lvm/vol1 /lib/libvirt/images/vm1.img

Dry run to see what would be synchronized::

    $ rblocksync3 --dry-run source.img user@host:/dev/lvm/vol2

Create the destination file if it doesn't exist::

    $ rblocksync3 --truncate /dev/sdc new_destination.img

Author
======

Paulino Ruiz de Clavijo Vázquez pruiz@us.es

Reporting bugs
==============

Report bugs at https://github.com/paulino/rblocksync3/issues

See also
========

rsync(1), ssh(1), scp(1)
