from pyspark import SparkConf, SparkContext
import collections

sc = SparkContext(conf=SparkConf().setAppName("TwitterPath").setMaster("local"))
twitter_sample_path = "twitter_sample_small.txt"


def parse_edge(row):
    user, follower = row.split("\t", 1)
    return int(user), int(follower)


def next_path(item):
    path_so_far, next_vert = item[1][0], item[1][1]
    return next_vert, (path_so_far, next_vert)


def tuple2list(a):
    return list((tuple2list(x) if isinstance(x, tuple) else x for x in a))


def flatten(x):
    if isinstance(x, collections.Iterable):
        return [str(a) for i in x for a in flatten(i)]
    else:
        return [x]


start_user = 12
end_user = 34
partition_count = 1
edge_tuples = sc.textFile(twitter_sample_path).map(parse_edge)
edge_tuples.cache()
fwd_edge_tuples = edge_tuples.map(lambda edge: (edge[1], edge[0])).partitionBy(partition_count).persist()
paths = sc.parallelize([(start_user, start_user)]).partitionBy(partition_count)

while True:
    new_paths = paths.join(fwd_edge_tuples, partition_count).map(next_path).persist()
    complete_count = new_paths.filter(lambda path: path[0] == end_user).count()

    if complete_count > 0:
        final_path = new_paths.filter(lambda path: path[0] == end_user).collect()
        print(','.join(flatten(tuple2list(final_path[0][1]))))
        break
    else:
        paths = new_paths
