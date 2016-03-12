__author__ = 'renienj'

import shutil


if __name__ == "__main__":

    newProduct = "|R||L|\n"
    newAttribute = "|L|\n"
    newValue = "||"

    count = 0

    attribute = ["userid", "age", "dob_day", "dob_year", "dob_month", "gender",	"tenure", "friend_count",
                 "friendships_initiated", "likes",	"likes_received", "mobile_likes", "mobile_likes_received",
                 "www_likes", "www_likes_received", "user_name"]

    with open('../data/sample_fb_data_name.tsv') as old, open('../data/sample_fb_data_small.feed', 'w') as new:

        for line in old.read().split('\r'):
            newLine = newProduct
            words = line.split('\t')
            count += 1
            if count == 50:
                break
            for index, word in enumerate(words):
                newLine = newLine + attribute[index] + newValue + word + newAttribute
            new.write(newLine)