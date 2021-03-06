from typing import List, Dict

from prefect import task, Flow, Parameter
from collections import defaultdict
import prefect
import csv


def genre_count(genres: str) -> int:
    """
    this function counts the number of genres in the genre string
    :param genres: genres. | separated
    :return: int
    """
    if not isinstance(genres, str) or not genres:
        raise Exception("empty list or | is not included")
    else:
        genre_list = genres.split("|")  # TODO rename
        if genre_list[0] == "(no genres listed)":
            return 0
        else:
            return len(genre_list)


def make_genre_dict(reader: List[Dict]) -> tuple:
    """
    this function make a list of key=genres and values=count, add the new column "genre_count", and average
    with new added column
    :param reader: a list of dictionary
    :return: tuple( a dict, new list of dict with added new column, average)
    """
    sum_genre_count = 0
    num_lines = 0
    movie_list = []

    dict_genre = defaultdict(int)  # TODO default dict
    if not isinstance(reader, list):
        raise Exception("List IS empty")
    for line in reader:
        if not isinstance(line, dict) or "genres" not in line.keys():
            raise Exception("genres key is not present")
        movie_list.append(line)
        genre_list = line["genres"].split("|")
        for genre in genre_list:
            dict_genre[genre] = dict_genre[genre] + 1

        # add the new count field to dictionary
        count = genre_count(line["genres"])
        movie_list[-1]["genre_count"] = count

        # for calculating average
        sum_genre_count += count
        num_lines += 1
    average = sum_genre_count / num_lines
    return dict_genre, movie_list, average


def find_max(dict_genre: dict) -> tuple:
    """
    this function find the most genre in a dictionary
    :param dict_genre: a dictionary
    :return: a tuple (most genre, count of the most genre)
    """
    max_genre_count = 0
    max_genre = ""
    if not isinstance(dict_genre, dict):
        raise Exception("not a dictionary")
    for key, value in dict_genre.items():
        if max_genre_count < value:
            max_genre_count = value
            max_genre = key
    return (max_genre, max_genre_count)


@task
def read_from_file(input_file: str) -> list:
    """
    this function read the file in dictionary format and return a list of content
    :param input_file: file to be read
    :return: list
    """
    with open(input_file, mode="r") as readFile:
        reader = csv.DictReader(readFile)
        return list(reader)


@task
def add_genre_count(reader: List[Dict]) -> List[Dict]:
    """
    this function add the new column genre_count. also find the most genre and average
    :param reader: list of dictionary
    :return: list of dictionary
    """
    dict_genre, movie_list, average = make_genre_dict(reader)  # TODO

    # find the most genre
    max_genre = find_max(dict_genre)
    logger = prefect.context.get("logger")
    logger.info(f"Average is: {average}")  # TODO change to prefect logging
    logger.info(f"most common genre is {max_genre[0]} with {max_genre[1]} count.")
    return movie_list


@task
def write_to_file(
    movie_list: List[Dict], output_file: str
) -> None:  # TODO change arg and comments
    """
    this function write the list of dictionary(movie fields) to a csv file
    :param movie_list: list of dictionary
    :param output_file: file to be written
    :return: nothing
    """
    with open(output_file, mode="w") as writeFile:
        writer = csv.DictWriter(
            writeFile, ["movieId", "title", "genres", "genre_count"]
        )

        writer.writeheader()

        for line in movie_list:
            writer.writerow(line)


with Flow("ETL") as flow:
    input_file = Parameter("input_file", default="test100/movies.csv")
    output_file = Parameter("output_file", default="movie_enhanced.csv")
    reader = read_from_file(input_file)
    movie_list = add_genre_count(reader)
    write_to_file(movie_list, output_file)


if __name__ == "__main__":
    flow.run(input_file="movies.csv")
