from pathlib import Path
import filecmp

import pytest

from fs import (
    FILENAME_SIZE,
    AddressPageWorker,
    Filesystem,
    BLOCK_SIZE,
    BLOCK_COUNT,
    MapPageWorker,
    PageType,
    SaveResult,
    _make_block_map_page,
    ADDRESS_RECORD_COUNT_PER_PAGE,
    AddressRecord,
    LookupResult,
)

SHORT_TEST_FILE = "t"
TEST_FILE = "testfile"
TEST_RESULT_FILE = "test_result_file"


def test_map_page_worker_correctly_checks_type():
    data = bytearray(BLOCK_SIZE)
    data[0] = PageType.ADDRESS_PAGE.value
    with pytest.raises(AssertionError):
        _ = MapPageWorker(bytes(data))


def test_map_page_worker_parses_blocks():
    data = bytearray(BLOCK_SIZE)
    data[0] = PageType.BLOCK_MAP_PAGE.value
    data[1] = PageType.BLOCK_MAP_PAGE.value
    data[2] = PageType.ADDRESS_PAGE.value
    data[3] = PageType.DATA_PAGE.value
    worker = MapPageWorker(bytes(data))
    assert worker._parsed_blocks[PageType.BLOCK_MAP_PAGE] == [0]
    assert worker.get_address_pages() == [1]
    assert worker._parsed_blocks[PageType.DATA_PAGE] == [2]
    assert worker._parsed_blocks[PageType.UNINITIALIZED] == list(range(3, BLOCK_COUNT))


def test_map_page_allocates_pages():
    worker = MapPageWorker(_make_block_map_page())
    # 1 for map page.
    assert worker.free_page_count == BLOCK_COUNT - 1

    page_id = worker.allocate_address_page()
    assert worker.get_address_pages() == [page_id]
    # 1 for map page, 1 for address page.
    assert worker.free_page_count == BLOCK_COUNT - 2

    # 1 for map page, 1 for address page, 1 for data page.
    _ = worker.allocate_data_page()
    assert worker.free_page_count == BLOCK_COUNT - 3

    worker = MapPageWorker(worker.bytes)
    assert worker.get_address_pages() == [page_id]
    assert worker.free_page_count == BLOCK_COUNT - 3


def test_address_page_saves_records():
    data = bytearray(BLOCK_SIZE)
    data[0] = PageType.ADDRESS_PAGE.value
    worker = AddressPageWorker(1, bytes(data))
    assert worker._records == [None] * ADDRESS_RECORD_COUNT_PER_PAGE
    empty_record = worker.find_first_empty_record()
    assert empty_record is not None
    TEST_RECORD = AddressRecord(filename=TEST_FILE, page_id=7, filesize=8000)
    worker.set_record(empty_record, TEST_RECORD)

    worker = AddressPageWorker(1, worker.bytes)
    assert worker.list_files() == [TEST_RECORD]


def test_empty_filename_is_not_saved(tmpdir):
    fs_folder = Path(tmpdir.mkdir("fs_dir"))
    test_filepath = Path(tmpdir) / TEST_FILE
    with open(test_filepath, "w") as test_file:
        test_file.write("LOL")

    with Filesystem(fs_folder) as fs:
        res = fs.save(test_filepath, "")
        assert res == SaveResult.EMPTY_FILENAME


def test_too_big_filename_is_not_saved(tmpdir):
    fs_folder = Path(tmpdir.mkdir("fs_dir"))
    test_filepath = Path(tmpdir) / TEST_FILE
    with open(test_filepath, "w") as test_file:
        test_file.write("LOL")

    with Filesystem(fs_folder) as fs:
        res = fs.save(test_filepath, "x" * (FILENAME_SIZE + 1))
        assert res == SaveResult.FILENAME_TOO_BIG


# def test_saved_short_filename_is_listed(tmpdir):
#     fs_folder = Path(tmpdir.mkdir("fs_dir"))
#     test_filepath = Path(tmpdir) / TEST_FILE
#     with open(test_filepath, "w") as test_file:
#         test_file.write("LOL")

#     with Filesystem(fs_folder) as fs:
#         res = fs.save(test_filepath, SHORT_TEST_FILE)
#         assert res == SaveResult.SUCCESS
#         assert fs.list_files() == [SHORT_TEST_FILE]

#     with Filesystem(fs_folder) as fs:
#         assert fs.list_files() == [SHORT_TEST_FILE]


def test_saved_filename_is_listed(tmpdir):
    fs_folder = Path(tmpdir.mkdir("fs_dir"))
    test_filepath = Path(tmpdir) / TEST_FILE
    with open(test_filepath, "w") as test_file:
        test_file.write("LOL")

    with Filesystem(fs_folder) as fs:
        res = fs.save(test_filepath, TEST_FILE)
        assert res == SaveResult.SUCCESS
        assert fs.list_files() == [TEST_FILE]

    with Filesystem(fs_folder) as fs:
        assert fs.list_files() == [TEST_FILE]


def test_missing_filename_is_not_found(tmpdir):
    fs_folder = Path(tmpdir.mkdir("fs_dir"))
    test_result_filepath = Path(tmpdir) / TEST_RESULT_FILE
    with Filesystem(fs_folder) as fs:
        res = fs.load("missing", test_result_filepath)
        assert res == LookupResult.NOT_FOUND


def test_saved_file_is_readable(tmpdir):
    fs_folder = Path(tmpdir.mkdir("fs_dir"))
    test_filepath = Path(tmpdir) / TEST_FILE
    with open(test_filepath, "w") as test_file:
        test_file.write("LOL")

    with Filesystem(fs_folder) as fs:
        res = fs.save(test_filepath, TEST_FILE)
        assert res == SaveResult.SUCCESS

    test_result_filepath = Path(tmpdir) / TEST_RESULT_FILE
    with Filesystem(fs_folder) as fs:
        res = fs.load(TEST_FILE, test_result_filepath)
        assert res == LookupResult.SUCCESS
        assert filecmp.cmp(test_filepath, test_result_filepath)


def test_saved_large_file_is_readable(tmpdir):
    fs_folder = Path(tmpdir.mkdir("fs_dir"))
    test_filepath = Path(tmpdir) / TEST_FILE
    with open(test_filepath, "wb") as test_file:
        for _ in range(BLOCK_SIZE * 2):
            test_file.write(b"\01")

    with Filesystem(fs_folder) as fs:
        res = fs.save(test_filepath, TEST_FILE)
        assert res == SaveResult.SUCCESS

    test_result_filepath = Path(tmpdir) / TEST_RESULT_FILE
    with Filesystem(fs_folder) as fs:
        res = fs.load(TEST_FILE, test_result_filepath)
        assert res == LookupResult.SUCCESS
        assert filecmp.cmp(test_filepath, test_result_filepath)


def test_delete_removes_file_deallocates_memory(tmpdir):
    fs_folder = Path(tmpdir.mkdir("fs_dir"))
    test_filepath = Path(tmpdir) / TEST_FILE
    with open(test_filepath, "wb") as test_file:
        for _ in range(BLOCK_SIZE * 2):
            test_file.write(b"\01")

    with Filesystem(fs_folder) as fs:
        res = fs.save(test_filepath, TEST_FILE)
        assert res == SaveResult.SUCCESS

    with Filesystem(fs_folder) as fs:
        assert fs.list_files() == [TEST_FILE]
        res = fs.delete(TEST_FILE)
        assert res == LookupResult.SUCCESS
        assert fs.list_files() == []

    with open(test_filepath, "w") as test_file:
        test_file.write("LOL")
    with Filesystem(fs_folder) as fs:
        res = fs.save(test_filepath, TEST_FILE)
        assert res == SaveResult.SUCCESS

    test_result_filepath = Path(tmpdir) / TEST_RESULT_FILE
    with Filesystem(fs_folder) as fs:
        res = fs.load(TEST_FILE, test_result_filepath)
        assert res == LookupResult.SUCCESS
        assert filecmp.cmp(test_filepath, test_result_filepath)
