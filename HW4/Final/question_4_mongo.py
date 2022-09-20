import mongodb_check
import pandas as pd


def question_4_example():

    # You may have to do this just to be safe.

    connect_url = "mongodb://localhost:27017/"
    mongodb_check.set_connect_url(connect_url)

    # Get the client
    client = mongodb_check.get_mongo_client()

    # This aggregation returns all of the characters that appeared in season 1, episode 1 and the scenes.
    # Requires the PyMongo package.
    # https://api.mongodb.com/python/current

    result = client['F21_Final']['got_episodes'].aggregate([
        {
            '$unwind': {
                'path': '$scenes',
                'includeArrayIndex': 'sceneNum'
            }
        }, {
            '$unwind': {
                'path': '$scenes.characters'
            }
        }, {
            '$project': {
                'seasonNum': 1,
                'episodeNum': 1,
                'sceneNum': {
                    '$sum': [
                        '$sceneNum', 1
                    ]
                },
                'characterName': '$scenes.characters.name'
            }
        }, {
            '$match': {
                'seasonNum': 1,
                'episodeNum': 1
            }
        }, {
            '$project': {
                '_id': 0
            }
        }
    ])

    result = list(result)
    result = pd.DataFrame(result)
    return result


def question_4_ratings():
    connect_url = "mongodb://localhost:27017/"
    mongodb_check.set_connect_url(connect_url)

    # Get the client
    client = mongodb_check.get_mongo_client()

    # This aggregation returns all of the characters that appeared in season 1, episode 1 and the scenes.
    # Requires the PyMongo package.
    # https://api.mongodb.com/python/current

    result = client['F21_Final']['got_episodes'].aggregate([


            {
                '$project': {
                    'seasonNum': 1,
                    'episodeNum': 1,
                    'episodeTitle': 1,
                    'episodeDescription': 1,
                    'episodeDate': '$episodeAirDate',
                    'episodeLinkep': {
                        '$substr': [
                            '$episodeLink', 7, 9
                        ]
                    }
                }
            }, {
            '$lookup': {
                'from': 'title_ratings',
                'localField': 'episodeLinkep',
                'foreignField': 'tconst',
                'as': 'Links'
            }
        }, {
            '$project': {
                'seasonNum': 1,
                'episodeNum': 1,
                'episodeTitle': 1,
                'episodeDescription': 1,
                'episodeDate': 1,
                'tconst': '$Links.tconst',
                'averageRating': '$Links.averageRating',
                'numVotes': '$Links.numVotes'
            }
        }, {
            '$project': {
                '_id': 0
            }
        }, {
            '$unwind': {
                'path': '$tconst'
            }
        }, {
            '$unwind': {
                'path': '$averageRating'
            }
        }, {
            '$unwind': {
                'path': '$numVotes'
            }
        }
        ])

    result = list(result)
    result = pd.DataFrame(result)
    return result




