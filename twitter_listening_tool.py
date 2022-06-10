import tweepy
from googleapiclient.discovery import build
from gcloud import storage as storage_writer
from google.cloud import storage as storage_reader
from google.cloud import secretmanager

client = secretmanager.SecretManagerServiceClient()
project_id = "tweets-listening-tool"
bucket_name = "tweets_listener"
csv_filename = 'robot_mascot_tweets.csv'
request = {
    "name": f"projects/766760202834/secrets/twitter_config_secret/versions/1"}
response = client.access_secret_version(request)
twitter_secret_key = response.payload.data.decode("UTF-8")
request = {"name": f"projects/766760202834/secrets/robot_mascot_gs_id/versions/1"}
response = client.access_secret_version(request)
googlesheet_id = response.payload.data.decode("UTF-8")


def fetch_tweets(name_val):
    auth_key = eval(twitter_secret_key)
    auth = tweepy.OAuthHandler(auth_key["api_key"], auth_key["api_key_secret"])
    auth.set_access_token(auth_key["access_token"],
                          auth_key["access_token_secret"])
    api = tweepy.API(auth)
    try:
        return tweepy.Cursor(api.user_timeline, screen_name=name_val, count=100, tweet_mode="extended").items(200)
    except tweepy.errors.BadRequest:
        return 0


def parse_handle(handle):
    if "/" in handle:
        handle = handle.split('/')
        pass
    elif "@" in handle:
        handle = handle.split('@')
        pass
    else:
        handle = handle.split('/')
    return handle


def reformat_date(x):
    day = x.strftime("%d")
    month = x.strftime("%b")
    year = x.strftime("%Y")
    date = f"{day}th {month} {year}"
    return date


def search_keyword_in_tweet(tweets, keywords):
    data = []
    for tweet in tweets:
        try:
            for keyword in keywords:
                full_text_list = tweet.full_text.split(" ")
                word_list = keyword.split(" ")
                flag = False
                for test in range(len(full_text_list)):
                    if len(word_list) > 1:
                        if word_list[0].lower() in full_text_list[test].lower():
                            try:
                                if word_list[1].lower() == full_text_list[test+1].lower():
                                    if "@" not in full_text_list[test]:
                                        flag = True
                                elif ("".join(word_list)).lower() in full_text_list[test].lower():
                                    if "@" not in full_text_list[test]:
                                        flag = True
                            except:
                                if ("".join(word_list)).lower() in full_text_list[test].lower():
                                    if "@" not in full_text_list[test]:
                                        flag = True
                    else:
                        if word_list[0].lower() in full_text_list[test].lower():
                            if "@" not in full_text_list[test]:
                                flag = True
                if(flag == True):
                    print((tweet.user.screen_name))
                    date = reformat_date(tweet.created_at)
                    data.append([tweet.user.screen_name, date, keyword, tweet.full_text,
                                'https://twitter.com/twitter/statuses/'+str(tweet.id)])
        except:
            print("No or unexpected value returned")
            pass
    return data


def save_in_gs(handles, keywords):
    service = build('sheets', 'v4')
    sheet = service.spreadsheets()
    data = []
    for handle in handles:
        handle = parse_handle(handle)
        tweets = fetch_tweets(handle[-1])
        if tweets:
            data = search_keyword_in_tweet(tweets, keywords)
            request = sheet.values().append(spreadsheetId=googlesheet_id,
                                            range="Output!A2:E2", valueInputOption="USER_ENTERED",
                                            insertDataOption="INSERT_ROWS", body={"values": data}).execute()
        del(tweets)
    print("Done")


def is_file_not_empty(gs_file):
    try:
        f = gs_file.split("\n")
        f.pop()
    except Exception as error:
        error_string = str(error)
        print(error_string)
    if f != "":
        return f
    else:
        return ""


def read_data_from_gcs():
    storage_client = storage_reader.Client(project=project_id)
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(csv_filename)
    gs_file = blob.download_as_text()
    return gs_file


def write_data_to_gcs(new_str):
    client = storage_writer.Client(project=project_id)
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(csv_filename)
    blob.upload_from_string(new_str)


def read_data_from_gs():
    service = build('sheets', 'v4')
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=googlesheet_id,
                                range="Input!A2:A").execute()
    handles = result.get('values', [])
    result = sheet.values().get(spreadsheetId=googlesheet_id,
                                range="Input!B2:B").execute()
    words = result.get('values', [])
    name_str = ""
    keywords = []
    for handle in handles:
        name_str = f"{name_str}{''.join(handle)}\n"
    for word in words:
        keywords.append(''.join(word))
    return name_str, keywords


def main(request):
    name_str, keywords = read_data_from_gs()
    gs_file = read_data_from_gcs()
    if is_file_not_empty(gs_file) != "":
        f = gs_file.split("\n")
        f.pop()
    name_list = name_str.split(("\n"))
    name_list.pop()
    new_list = list(set(name_list) - set(f))
    new_str = gs_file
    client = storage_writer.Client(project=project_id)
    if len(new_list):
        for name in new_list:
            new_str = f"{new_str}{name}\n"
        write_data_to_gcs(new_str)
        save_in_gs(new_list, keywords)
    return("Done")
