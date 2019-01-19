# Grofs - Git Read-Only FS

Simple FUSE implementation that exposes git repository as a file system. It's still in the early development so don't use it in production.

## Install

Before build, make sure you have `libgit2`, `fuse2`, `pkgconf` and `make` (tested with GNU make) installed.

Clone this repository and  run `make && sudo make install`.

## Usage

Run `grofs <git-repo-root-path> <mount-point>` to mount git repository.

Virtualized file system has following structure

```
/
    commits/
        commit1-sha1/
            tree/
                ... - tree for that commit as if you did git checkout commit1-sha1
            parent - file that contains SHA1 of the parent (no file if no parrent)
        commit2-sha1/
            ...
        ...
        commitn-sha1/
            ...
    blobs/
        blob1-sha1 - contains content for blob blob1-sha1
        blob2-sha1
        ...
        blobn-sha1
```

Some more rules

- file `parent` is always of size 40
- files representing blob have their size set to the size of raw blob content
- folders representing commits (like `commit1-sha1` in the example above) have commit time as their create and modified time
- every other file system item has `grofs` start time as create and modified time

## Further development

- Possibility to specify remote which is exposed
- Incremental directory reads
- More virtualized files/folders like commit author, message, etc.
