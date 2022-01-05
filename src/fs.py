from collections import defaultdict
from pathlib import Path
from types import resolve_bases
from typing import Dict, List, NamedTuple, Optional
from enum import Enum

FS_FILENAME = "fs"

# All sizes are in bytes.

BLOCK_SIZE = 10_000
FS_SIZE = 1_000_000

BLOCK_COUNT = FS_SIZE // BLOCK_SIZE

PAGE_TYPE_SIZE = 1
NEXT_PAGE_ID_SIZE = 4

NEXT_PAGE_ID_OFFSET = BLOCK_SIZE - NEXT_PAGE_ID_SIZE

# Map page requirements.
assert BLOCK_SIZE >= BLOCK_COUNT + PAGE_TYPE_SIZE

# Address page requirements.
FILENAME_SIZE = 8
PAGE_ID_SIZE = 4
FILESIZE_FIELD_SIZE = 4
MAX_BLOCK_OFFSET = 2 ** (PAGE_ID_SIZE * 8)
assert BLOCK_COUNT < MAX_BLOCK_OFFSET

FILENAME_OFFSET = 0
PAGE_ID_OFFSET = FILENAME_SIZE
FILESIZE_OFFSET = PAGE_ID_OFFSET + PAGE_ID_SIZE

ADDRESS_RECORD_SIZE = FILENAME_SIZE + PAGE_ID_SIZE + FILESIZE_FIELD_SIZE
ADDRESS_RECORD_COUNT_PER_PAGE = (
    BLOCK_SIZE - PAGE_TYPE_SIZE - NEXT_PAGE_ID_SIZE
) // ADDRESS_RECORD_SIZE

# Data page requirements.
DATA_PER_DATA_PAGE = BLOCK_SIZE - PAGE_TYPE_SIZE - NEXT_PAGE_ID_SIZE


class DiskWriter:
    def __init__(self, fs_file: Path):
        self._fs_filepath = fs_file
        self._fs_file = None

    def exists(self) -> bool:
        try:
            return self._fs_filepath.stat().st_size > 0
        except OSError:
            return False

    def open(self):
        assert self._fs_file is None

        self._fs_filepath.touch()
        self._fs_file = open(self._fs_filepath, "r+b")

    def close(self):
        assert self._fs_file is not None

        self._fs_file.close()

    def read(self, offset: int):
        assert self._fs_file is not None

        self._seek(offset)
        return self._fs_file.read(BLOCK_SIZE)

    def initalize_to_size(self, size: int):
        assert self._fs_file is not None

        self._fs_file.seek(size - 1)
        self._fs_file.write(b"\0")

    def _seek(self, offset: int):
        assert self._fs_file is not None
        assert offset <= self._fs_filepath.stat().st_size

        self._fs_file.seek(offset)

    def write(self, offset: int, data: bytes):
        assert self._fs_file is not None

        self._seek(offset)
        self._fs_file.write(data)


class PageType(Enum):
    UNINITIALIZED = 0b00
    BLOCK_MAP_PAGE = 0b01
    ADDRESS_PAGE = 0b10
    DATA_PAGE = 0b11


MAP_PAGE_ID = 0
MAP_PAGE_OFFSET = MAP_PAGE_ID * BLOCK_SIZE


def _make_block_map_page() -> bytes:
    result = bytearray(BLOCK_SIZE)
    # Page flag.
    result[0] = PageType.BLOCK_MAP_PAGE.value
    # First value in the page map.
    result[1] = PageType.BLOCK_MAP_PAGE.value
    return bytes(result)


def _parse_blocks(data: bytes) -> Dict[PageType, List[int]]:
    result = defaultdict(list)
    start = PAGE_TYPE_SIZE
    # The whole page except the page type flag describes the page flags for the rest of the file system,
    # byte i corresponds to ith page flag.
    for i, b in enumerate(data[start : start + BLOCK_COUNT]):
        result[PageType(b)].append(i)
    assert sum(len(val) for val in result.values()) == BLOCK_COUNT
    return result


class BasePageWorker:
    def __init__(self, page_id: int, data: bytes):
        self._page_id = page_id
        self._data = bytearray(data)
        self._is_dirty = False

    @property
    def page_id(self):
        return self._page_id

    @property
    def is_dirty(self):
        return self._is_dirty

    def mark_dirty(self):
        self._is_dirty = True

    def set_next_page_id(self, next_page_id: int):
        assert next_page_id > 0

        self._data[NEXT_PAGE_ID_OFFSET : NEXT_PAGE_ID_OFFSET + NEXT_PAGE_ID_SIZE] = i2b(
            next_page_id
        )
        self.mark_dirty()

    @property
    def next_page_id(self) -> Optional[int]:
        result = b2i(
            self._data[NEXT_PAGE_ID_OFFSET : NEXT_PAGE_ID_OFFSET + NEXT_PAGE_ID_SIZE]
        )
        if result == MAP_PAGE_ID:
            return None
        return result

    def cleanup(self):
        self._data = bytearray(BLOCK_SIZE)
        self.mark_dirty()

    @property
    def bytes(self) -> bytes:
        return bytes(self._data)


class MapPageWorker(BasePageWorker):
    def __init__(self, data: bytes):
        super().__init__(MAP_PAGE_ID, data)
        if self._data[0] == PageType.UNINITIALIZED.value:
            self._data[0] = PageType.BLOCK_MAP_PAGE.value
        assert self._data[0] == PageType.BLOCK_MAP_PAGE.value
        self._parsed_blocks = _parse_blocks(data)

    def get_address_pages(self) -> List[int]:
        return self._parsed_blocks[PageType.ADDRESS_PAGE]

    @property
    def free_page_count(self) -> int:
        return len(self._parsed_blocks[PageType.UNINITIALIZED])

    def allocate_address_page(self) -> int:
        """Allocates a new address page, returns its id"""
        return self._allocate(PageType.ADDRESS_PAGE)

    def allocate_data_page(self) -> int:
        """Allocates a new data page, returns its id"""
        return self._allocate(PageType.DATA_PAGE)

    def deallocate_data_page(self, page_id: int):
        self._data[PAGE_TYPE_SIZE + page_id] = PageType.UNINITIALIZED.value
        self._parsed_blocks[PageType.DATA_PAGE].remove(page_id)
        self.mark_dirty()

    def _allocate(self, page_type: PageType) -> int:
        # TODO: Handle being out of memory.
        assert self.free_page_count > 0
        new_page_id = self._parsed_blocks[PageType.UNINITIALIZED].pop()
        self._data[PAGE_TYPE_SIZE + new_page_id] = page_type.value
        self._parsed_blocks[page_type].append(new_page_id)
        self.mark_dirty()
        return new_page_id

    @property
    def bytes(self):
        return bytes(self._data)


class AddressRecord(NamedTuple):
    filename: str
    page_id: int
    filesize: int


def b2i(b: bytes) -> int:
    return int.from_bytes(b, byteorder="big")


def i2b(i: int, length=PAGE_ID_SIZE) -> bytes:
    return i.to_bytes(length=length, byteorder="big")


def _parse_address_records(data: bytes) -> List[Optional[AddressRecord]]:
    result = []
    for i in range(ADDRESS_RECORD_COUNT_PER_PAGE):
        records_offset = PAGE_TYPE_SIZE + i * ADDRESS_RECORD_SIZE
        record = data[records_offset : records_offset + ADDRESS_RECORD_SIZE]
        filesize = b2i(record[FILESIZE_OFFSET : FILESIZE_OFFSET + FILESIZE_FIELD_SIZE])
        if filesize == 0:
            # Unused record.
            result.append(None)
            continue

        page_id = b2i(record[PAGE_ID_OFFSET : PAGE_ID_OFFSET + PAGE_ID_SIZE])
        filename = str(
            record[FILENAME_OFFSET : FILENAME_OFFSET + FILENAME_SIZE], encoding="utf8"
        )
        result.append(
            AddressRecord(filename=filename, page_id=page_id, filesize=filesize)
        )

    return result


class AddressPageWorker(BasePageWorker):
    def __init__(self, page_id: int, data: bytes):
        super().__init__(page_id, data)
        if self._data[0] == PageType.UNINITIALIZED.value:
            self._data[0] = PageType.ADDRESS_PAGE.value
        assert self._data[0] == PageType.ADDRESS_PAGE.value
        self._records = _parse_address_records(data)

    def find_first_empty_record(self) -> Optional[int]:
        for i, record in enumerate(self._records):
            if record is None:
                return i
        return None

    def list_files(self) -> List[AddressRecord]:
        return [rec for rec in self._records if rec is not None]

    def delete_record(self, record: AddressRecord):
        for i, rec in enumerate(self._records):
            if rec == record:
                self._records[i] = None
                record_offset = PAGE_TYPE_SIZE + i * ADDRESS_RECORD_SIZE
                # Unset filesize to mark as deleted.
                self._data[
                    record_offset
                    + FILESIZE_OFFSET : record_offset
                    + FILESIZE_OFFSET
                    + FILESIZE_FIELD_SIZE
                ] = i2b(0)
                self.mark_dirty()
                break

    def set_record(self, record_id: int, record: AddressRecord):
        self._records[record_id] = record
        record_offset = PAGE_TYPE_SIZE + record_id * ADDRESS_RECORD_SIZE
        self._data[
            record_offset
            + FILENAME_OFFSET : record_offset
            + FILENAME_OFFSET
            + FILENAME_SIZE
        ] = bytes(record.filename, encoding="utf8")
        self._data[
            record_offset
            + PAGE_ID_OFFSET : record_offset
            + PAGE_ID_OFFSET
            + PAGE_ID_SIZE
        ] = i2b(record.page_id)
        self._data[
            record_offset
            + FILESIZE_OFFSET : record_offset
            + FILESIZE_OFFSET
            + FILESIZE_FIELD_SIZE
        ] = i2b(record.filesize)
        self.mark_dirty()


class DataPageWorker(BasePageWorker):
    def __init__(self, page_id: int, data: bytes):
        super().__init__(page_id, data)
        if self._data[0] == PageType.UNINITIALIZED.value:
            self._data[0] = PageType.DATA_PAGE.value
        assert self._data[0] == PageType.DATA_PAGE.value

    def set_data(self, data: bytes):
        assert len(data) <= DATA_PER_DATA_PAGE

        self._data[PAGE_TYPE_SIZE:] = data
        self.mark_dirty()

    def get_data(self, length: int) -> bytes:
        assert length <= DATA_PER_DATA_PAGE

        return bytes(self._data[PAGE_TYPE_SIZE : PAGE_TYPE_SIZE + length])


class LookupResult(Enum):
    SUCCESS = 0
    NOT_FOUND = 1
    FILENAME_TOO_BIG = 2
    EMPTY_FILENAME = 3
    RESULT_FILE_ALREADY_EXISTS = 4


class SaveResult(Enum):
    SUCCESS = 0
    OUT_OF_MEMORY = 1
    NOT_A_FILE = 2
    FILENAME_TOO_BIG = 3
    EMPTY_FILENAME = 4
    DUPLICATE_FILENAME = 5


class WriterContext:
    def __init__(self, writer):
        self._writer: DiskWriter = writer
        self._data: List[BasePageWorker] = []

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        if self._data:
            for page in self._data:
                if page.is_dirty:
                    self._writer.write(page.page_id * BLOCK_SIZE, page.bytes)

    def track(self, page_worker: BasePageWorker):
        self._data.append(page_worker)


class Filesystem:
    def __init__(self, directory: Path):
        assert directory.is_dir()
        self._writer = DiskWriter(directory / FS_FILENAME)

    def __enter__(self):
        self._writer.open()

        if not self._is_initialized():
            self._initialize()

        return self

    def _is_initialized(self) -> bool:
        return self._writer.exists()

    def _initialize(self):
        # Initialize the file to the maximum fs size.
        self._writer.initalize_to_size(FS_SIZE)
        # Set the first page as map page.
        self._writer.write(0 * BLOCK_SIZE, _make_block_map_page())

    def __exit__(self, exception_type, exception_value, traceback):
        self._writer.close()

    def list_files(self) -> List[str]:
        result = []
        with WriterContext(self._writer) as ctx:
            map_page_worker = self._read_map_page(ctx)
            for address_page_id in map_page_worker.get_address_pages():
                address_page_worker = self._read_address_page(ctx, address_page_id)
                result.extend(address_page_worker.list_files())
            return [rec.filename for rec in result]

    def _read_page(self, page_id: int) -> bytes:
        offset = page_id * BLOCK_SIZE
        assert offset < FS_SIZE
        return self._writer.read(page_id * BLOCK_SIZE)

    def _read_map_page(self, ctx: WriterContext):
        result = MapPageWorker(self._read_page(MAP_PAGE_ID))
        ctx.track(result)
        return result

    def _read_address_page(self, ctx: WriterContext, page_id: int):
        result = AddressPageWorker(page_id, self._read_page(page_id))
        ctx.track(result)
        return result

    def _read_data_page(self, ctx: WriterContext, page_id: int):
        result = DataPageWorker(page_id, self._read_page(page_id))
        ctx.track(result)
        return result

    def save(self, src: Path, result_filename: str) -> SaveResult:
        assert self._is_initialized()

        if not src.is_file():
            return SaveResult.NOT_A_FILE

        if len(result_filename) == 0:
            return SaveResult.EMPTY_FILENAME

        if len(result_filename) > FILENAME_SIZE:
            return SaveResult.FILENAME_TOO_BIG

        with WriterContext(self._writer) as ctx:
            map_page_worker = self._read_map_page(ctx)
            first_page_id = None
            prev_page_worker = None
            with open(src, "rb") as source:
                while True:
                    chunk = source.read(DATA_PER_DATA_PAGE)
                    if not chunk:
                        break
                    data_page_id = map_page_worker.allocate_data_page()
                    data_page_worker = self._read_data_page(ctx, data_page_id)
                    data_page_worker.set_data(chunk)

                    if first_page_id is None:
                        first_page_id = data_page_id
                    if prev_page_worker is not None:
                        # Update prev page next page id with the current page.
                        prev_page_worker.set_next_page_id(data_page_id)
                    prev_page_worker = data_page_worker

            assert first_page_id is not None

            empty_record_id = None
            address_page_worker = None
            for address_page_id in map_page_worker.get_address_pages():
                address_page_worker = self._read_address_page(ctx, address_page_id)
                empty_record_id = address_page_worker.find_first_empty_record()
                if empty_record_id is not None:
                    break

            if empty_record_id is None:
                # All records in all pages are in use, create a new address page.
                new_address_page_id = map_page_worker.allocate_address_page()
                address_page_worker = self._read_address_page(ctx, new_address_page_id)
                empty_record_id = address_page_worker.find_first_empty_record()

            assert address_page_worker is not None
            assert empty_record_id is not None

            address_page_worker.set_record(
                empty_record_id,
                AddressRecord(
                    filename=result_filename,
                    page_id=first_page_id,
                    filesize=src.stat().st_size,
                ),
            )

        return SaveResult.SUCCESS

    def load(self, src: str, dest: Path) -> LookupResult:
        if len(src) == 0:
            return LookupResult.EMPTY_FILENAME

        if len(src) > FILENAME_SIZE:
            return LookupResult.FILENAME_TOO_BIG

        if dest.exists():
            return LookupResult.RESULT_FILE_ALREADY_EXISTS

        with WriterContext(self._writer) as ctx:
            map_page_worker = self._read_map_page(ctx)
            for address_page_id in map_page_worker.get_address_pages():
                address_page_worker = self._read_address_page(ctx, address_page_id)

                for record in address_page_worker.list_files():
                    if record.filename == src:
                        data_left_to_read = record.filesize
                        next_block_id = record.page_id
                        assert next_block_id is not None
                        with open(dest, "wb") as result:
                            while data_left_to_read > 0 and next_block_id is not None:
                                data_page_worker = self._read_data_page(
                                    ctx, next_block_id
                                )
                                data = data_page_worker.get_data(
                                    min(DATA_PER_DATA_PAGE, data_left_to_read)
                                )
                                data_left_to_read -= len(data)
                                result.write(data)
                                next_block_id = data_page_worker.next_page_id
                        return LookupResult.SUCCESS

        return LookupResult.NOT_FOUND

    def delete(self, src: str) -> LookupResult:
        if len(src) == 0:
            return LookupResult.EMPTY_FILENAME

        if len(src) > FILENAME_SIZE:
            return LookupResult.FILENAME_TOO_BIG

        with WriterContext(self._writer) as ctx:
            map_page_worker = self._read_map_page(ctx)
            for address_page_id in map_page_worker.get_address_pages():
                address_page_worker = self._read_address_page(ctx, address_page_id)

                for record in address_page_worker.list_files():
                    if record.filename == src:
                        next_block_id = record.page_id
                        assert next_block_id is not None
                        while next_block_id is not None:
                            data_page_worker = self._read_data_page(ctx, next_block_id)
                            map_page_worker.deallocate_data_page(next_block_id)
                            next_block_id = data_page_worker.next_page_id
                            data_page_worker.cleanup()

                        address_page_worker.delete_record(record)
                        return LookupResult.SUCCESS

        return LookupResult.NOT_FOUND
