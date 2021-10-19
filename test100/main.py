from prefect import task, Flow, Parameter
import csv


def genre_count(genres):
    """
    this function counts the number of genres in the genre string
    :param genres: genres. | separated
    :return: int
    """
    glist = genres.split("|")
    if glist[0] == "(no genres listed)":
        return 0
    else:
        return len(glist)


def make_genre_dict(line, dict_genre):
    """
    this function make a list of genre dictionary for each line
    :param line: a line of file(line)
    :param dict_genre:
    :return: nothing
    """
    glist = line["genres"].split("|")
    for g in glist:
        if g in dict_genre:
            dict_genre[g] = dict_genre[g] + 1
        else:
            dict_genre[g] = 1


def find_max(dict_genre):
    """
    this function find the most genre in a dictionary
    :param dict_genre: a dictionary
    :return: a tuple (most genre, count of the most genre)
    """
    max_genre_count = 0
    max_genre = ""
    for key, value in dict_genre.items():
        if max_genre_count < value:
            max_genre_count = value
            max_genre = key
    return (max_genre, max_genre_count)


@task
def read_from_file(input_file):
    """
    this function read the file in dictionary format and return a list of content
    :param input_file: file to be read
    :return: list
    """
    with open(input_file, mode="r") as readFile:
        print(type(readFile))
        reader = csv.DictReader(readFile)
        return list(reader)


@task
def add_genre_count(reader):
    """
    this function add the new column genre_count. also find the most genre and average
    :param reader: list of dictionary
    :return: list of dictionary
    """
    num_lines = 0
    sum_genre_count = 0
    dict_genre = {}

    for line in reader:
        # make the dictionary of genres
        make_genre_dict(line, dict_genre)

        # add the new count field to dictionary
        count = genre_count(line["genres"])
        line["genre_count"] = count

        # for calculating average
        sum_genre_count += count
        num_lines += 1

    # find the most genre
    max_genre = find_max(dict_genre)
    print("Average is: {}".format(sum_genre_count / num_lines))
    print("most common genre is {} with {} count.".format(max_genre[0], max_genre[1]))
    return reader


@task
def write_to_file(transform, output_file):
    """
    this function write the list of dictionary to a csv file
    :param transform: list of dictionary
    :param output_file: file to be written
    :return: nothing
    """
    with open(output_file, mode="w") as writeFile:
        writer = csv.DictWriter(
            writeFile, ["movieId", "title", "genres", "genre_count"]
        )

        writer.writeheader()

        for line in transform:
            writer.writerow(line)


with Flow("ETL") as flow:
    input_file = Parameter("input_file", default="test10/movies.csv")
    output_file = Parameter("output_file", default="movie_enhanced.csv")
    reader = read_from_file(input_file)
    transform = add_genre_count(reader)
    write_to_file(transform, output_file)


if __name__ == "__main__":
    flow.run(input_file="movies.csv")
