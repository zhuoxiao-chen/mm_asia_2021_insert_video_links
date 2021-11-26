from pymongo import MongoClient, errors
import argparse
import pandas as pd
from tqdm import tqdm

def main(args):
    
    # # debug only
    # args.insert_youtube = True
    # args.insert_bilibili = True

    print("Connect to database ... ")
    client = MongoClient('localhost', 27017)
    papers = client.mmasia2021.papers

    print("Load the target excel file ... ")
    df = pd.read_excel(args.excel_path, header=None)

    fail_youtube, fail_bilibili = 0, 0
    fail_no_youtube_link, fail_no_bilibili_link = "", ""
    fail_insertion_youtube, fail_insertion_bilibili = "", ""

    print("Start to insert ... ")
    for i in tqdm(range(df.shape[0])):
        paper_title = df.iloc[i][0]
        youtube_link = df.iloc[i][1]
        bilibili_link_embed = df.iloc[i][2]

        # insert to youtube
        if args.insert_youtube and not pd.isnull(youtube_link):
            if not insert_youtube(papers, paper_title, youtube_link):
                fail_youtube += 1 
                fail_insertion_youtube += paper_title + "\n"
            pass
        else:
            fail_youtube += 1
            fail_no_youtube_link += paper_title + "\n"

        # insert to bilibili
        if args.insert_bilibili and not pd.isnull(bilibili_link_embed):
            if not insert_bilibili(papers, paper_title, bilibili_link_embed):
                fail_bilibili += 1 
                fail_insertion_bilibili += paper_title + "\n"
            pass
        else:
            fail_bilibili += 1
            fail_no_bilibili_link += paper_title + "\n"

    print("Complete insertion!\n")

    if args.insert_youtube:
        print("Youtube: number of failed insertion of youtube links:", fail_youtube, '\n')
        print()
        print("Youtube: failed due to NO links provided: \n" + fail_no_bilibili_link)
        print("\n Youtube: failed due to insertion ERROR: \n" + fail_insertion_bilibili)

    if args.insert_bilibili:
        print("Bilibili: number of failed insertion of links:", fail_bilibili, '\n')
        print()
        print("Bilibili: failed due to no links provided: \n" + fail_no_youtube_link)
        print("\n Bilibili: failed due to insertion error: \n" + fail_insertion_youtube)

def insert_youtube(papers, paper_title, youtube_link):
    """
    Return True is inserted successfuly. False otherwise. 
    """
    youtube_link_embed = 'https://www.youtube.com/embed/' + youtube_link.split("/")[-1]

    filter = {'title': paper_title}
    new_values = {"$set": {'youtube_link': youtube_link, 'youtube_link_embed': youtube_link_embed}}
    result = papers.update_one(filter, new_values)
    return result.acknowledged

def insert_bilibili(papers, paper_title, bilibili_link_embed):
    """
    Return True is inserted successfuly. False otherwise. 
    """

    bvid = bilibili_link_embed.split()[1][4:].split("&")[1][5:]
    p = bilibili_link_embed.split()[1][4:].split("&")[3][5:]

    bilibili_link = 'https://www.bilibili.com/video/' + bvid + '?p=' + p
    bilibili_link_embed = 'https:' + bilibili_link_embed.split()[1][5:-1]

    filter = {'title': paper_title}
    new_values = {"$set": {'bilibili_link': bilibili_link, 'bilibili_link_embed': bilibili_link_embed}}
    result = papers.update_one(filter, new_values)
    return result.acknowledged

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description='Insert video links to database of MMAsia2021. This Python script accepts an excel file, with 3 columns. 1: the full name of paper, 2: youtube link of the paper, 3: bilibili embed link of the paper. This script processes these two links for each paper, creates two other links (bilibili link, youtube embed link), then inserts all four links to each paper record in MMASIA2021 database.')

    parser.add_argument(
        '--excel_path', default='links_to_insert.xlsx', help="Enter the excel path.")

    parser.add_argument('--insert_youtube', action='store_true', help="insert youtube links to database.")

    parser.add_argument('--insert_bilibili', action='store_true', help="insert bilibili links to database.")
    
    args = parser.parse_args()

    main(args)