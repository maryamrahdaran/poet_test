import csv
import tempfile
from io import StringIO
import os

from mock import Mock, patch
import pytest

import test100
from test100 import main


@patch("test100.main.csv.DictWriter.writerow")
@patch("test100.main.csv.DictWriter.writeheader")
def test_write_csv(mock_header, mock_row):
    """
    mock write_to_file
    :param mock_header:
    :param mock_row:
    :return:
    """
    with patch("test100.main.open") as mocked_open:
        r = StringIO()
        mocked_open.return_value = r
        main.write_to_file.run(
            [
                {
                    "movieId": "19",
                    "title": "Hommage (2002)",
                    "genres": "Documentary",
                    "genre_count": 1,
                },
                {
                    "movieId": "19",
                    "title": "Hommage (2002)",
                    "genres": "Documentary",
                    "genre_count": 1,
                },
            ],
            "t.csv",
        )
        assert mock_header.call_count == 1
        assert mock_row.call_count == 2
        # print(r.read())


def test_read_from_file():
    with patch("test100.main.open") as mocked_open:
        mocked_open.return_value = StringIO("col1\n" "1\n")
        assert main.read_from_file.run("t") == [{"col1": "1"}]


"""
def test_read_from_file2(mocker):
    mocked_open = mocker.patch.object(test100.main, "open")
    mocked_open.return_value = StringIO("col1\n" "1\n")
    assert main.read_from_file.run("t") == [{"col1": "1"}]
"""


class TestReadFromCsv:
    def test_read_from_csv(self):
        """
        mock read from file
        :return:
        """
        with tempfile.TemporaryDirectory() as temp:
            output_file = os.path.join(temp, "r.csv")
            records = "movieId,title,genres\n19,Hommage (2002),Documentary\n19,Hommage (2002),Documentary\n"
            expected = [
                {"movieId": "19", "title": "Hommage (2002)", "genres": "Documentary"},
                {"movieId": "19", "title": "Hommage (2002)", "genres": "Documentary"},
            ]

            with open(output_file, "w") as of:
                of.write(records)
            data = main.read_from_file.run(output_file)
            assert data == expected


class TestWriteToCsv:
    def test_write_to_csv(self):
        """
        mock write_to_file
        :return:
        """
        with tempfile.TemporaryDirectory() as temp:
            output_file = os.path.join(temp, "temp.csv")
            records = [
                {
                    "movieId": "19",
                    "title": "Hommage (2002)",
                    "genres": "Documentary",
                    "genre_count": 1,
                },
                {
                    "movieId": "19",
                    "title": "Hommage (2002)",
                    "genres": "Documentary",
                    "genre_count": 1,
                },
            ]
            expected = "movieId,title,genres,genre_count\n19,Hommage (2002),Documentary,1\n19,Hommage (2002),Documentary,1\n"
            main.write_to_file.run(records, output_file)

            with open(output_file) as of:
                data = of.read()
        assert data == expected


@pytest.mark.parametrize(
    "genre, expected",
    [("comedy | drama | action", 3), ("comedy", 1), ("(no genres listed)", 0)],
)
def test_genre_count(genre, expected):
    """
    mock genre_count
    :param genre: string | separated
    :param expected: expected result - int
    :return:
    """
    result = main.genre_count(genre)
    assert result == expected


@pytest.mark.parametrize(
    "movie_list, expected",
    [
        (
            [{"genres": "comedy|drama|action"}],
            (
                {"comedy": 1, "drama": 1, "action": 1},
                [{"genres": "comedy|drama|action", "genre_count": 3}],
                1,
                3,
            ),
        ),
        (
            [
                {"genres": "comedy|horror|action"},
                {"genres": "comedy|drama|action"},
                {"genres": "(no genres listed)"},
            ],
            (
                {
                    "comedy": 2,
                    "drama": 1,
                    "action": 2,
                    "horror": 1,
                    "(no genres listed)": 1,
                },
                [
                    {"genres": "comedy|horror|action", "genre_count": 3},
                    {"genres": "comedy|drama|action", "genre_count": 3},
                    {"genres": "(no genres listed)", "genre_count": 0},
                ],
                3,
                6,
            ),
        ),
    ],
)
def test_make_genre_dict(movie_list, expected):
    """
    mock make_genre_dict
    :param movie_list: list of dictionary containing movies
    :param expected: expected result- (dic, list, int, int)
    :return:
    """

    assert main.make_genre_dict(movie_list) == expected


@pytest.mark.parametrize(
    "dict_genre, expected",
    [
        ({"action": 10, "drama": 2}, ("action", 10)),
        ({"action": 10, "drama": 2, "comedy": 10}, ("action", 10)),
    ],
)
def test_find_max(dict_genre, expected):
    """
    test find_max
    :param dict_genre: a dictionary of genre with its ount
    :param expected: tuple genre, count
    :return:
    """
    result = main.find_max(dict_genre)
    assert result == expected


@pytest.mark.parametrize(
    "movie_list, expected",
    [
        (
            [
                {"foo": "bar", "genres": "comedy|drama|action"},
                {"foo": "foo", "genres": "comedy|drama"},
            ],
            [
                {"foo": "bar", "genres": "comedy|drama|action", "genre_count": 3},
                {"foo": "foo", "genres": "comedy|drama", "genre_count": 2},
            ],
        )
    ],
)
def test_add_genre_count(movie_list, expected):
    """
    test add genre_count
    :param movie_list: a dictionary
    :param expected: expected results which the genre_count is added
    :return:
    """
    assert main.add_genre_count.run(movie_list) == expected
