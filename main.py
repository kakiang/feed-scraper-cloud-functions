import sys
from datetime import datetime
from time import mktime
from bleach import clean
from feedparser import parse as feedparse
from google.cloud import firestore
from google.cloud import language
from google.cloud.language import enums, types

feed_urls = [
    'http://www.africanews.com/feed/rss',
    'http://fr.africanews.com/feed/rss',
    'https://www.biztechafrica.com/feed/rss/',
    'https://www.afro.who.int/rss/emergencies.xml',
    'https://www.afro.who.int/rss/featured-news.xml',
    'https://www.afro.who.int/rss/speeches-messages.xml',
    'https://www.afdb.org/en/news-and-events/rss',
    'https://www.afdb.org/en/about-us/careers/current-vacancies/consultants/rss',
    'https://www.afdb.org/en/about-us/careers/current-vacancies/rss',
    'https://allafrica.com/tools/headlines/rdf/latest/headlines.rdf',
    'https://allafrica.com/tools/headlines/rdf/africa/headlines.rdf',
    'https://allafrica.com/tools/headlines/rdf/ict/headlines.rdf',
    'http://feeds.reuters.com/reuters/AFRICATopNews',
    'http://feeds.reuters.com/reuters/AFRICAbusinessNews',
    'http://feeds.feedburner.com/AfricaEnergyIntelligence',
    'http://feeds.news24.com/articles/News24/TopStories/rss'
    'https://news.google.com/rss',
    'http://feeds.bbci.co.uk/news/world/africa/rss.xml',
    'http://www.jeuneafrique.com/feed/',
    'https://www.aljazeera.com/xml/rss/all.xml']


def parse_feed(rss_url):
    return feedparse(rss_url)


def struct_time_to_datetime(struct_time):
    if not struct_time:
        return None
    return datetime.fromtimestamp(mktime(struct_time))


def analyze_sentiment(text_content):
    language_client = language.LanguageServiceClient()

    document = types.Document(
        content=text_content,
        type=enums.Document.Type.PLAIN_TEXT,
    )
    # encoding_type = enums.EncodingType.UTF8
    annnotations = language_client.analyze_sentiment(document)
    score = round(annnotations.document_sentiment.score, 2)
    magnitude = round(annnotations.document_sentiment.magnitude, 2)
    return score, magnitude


def parse_feed_entries(feed_url):

    result = parse_feed(feed_url)
    feed = result['feed']

    channel = {}
    channel['title'] = feed.get('title')
    channel['link'] = feed.get('link')
    channel['language'] = feed.get('language')
    channel['updated_date'] = struct_time_to_datetime(
        feed.get('updated_parsed', ''))

    feed_items = []
    for item in result['entries']:

        if item.get('updated_parsed', ''):
            struct_date = item.get('updated_parsed')
        elif item.get('published_parsed', ''):
            struct_date = item.get('published_parsed')
        else:
            continue

        pubdate = struct_time_to_datetime(struct_date)

        elapsed_time = datetime.now() - pubdate

        # exclude items that are older than 1h
        if elapsed_time.days >= 1 or elapsed_time.seconds > 3600:
            continue

        item_map = {}

        if item.get('title'):

            score, magnitude = analyze_sentiment(item.get('title'))
            item_map['sentiment_score'] = score
            item_map['sentiment_magnitude'] = magnitude
            print(f"Sentiment: score of {score} with magnitude of {magnitude}")

        item_map['id'] = str(mktime(struct_date))
        item_map['title'] = item.get('title')
        item_map['summary'] = clean(f'''{item.get('summary')}''', strip=True)
        item_map['link'] = item.get('link')
        item_map['pubdate'] = pubdate
        item_map['feed'] = channel

        for link in item.get('links'):
            if 'image' in link['type']:
                item_map['image'] = link['href']
            if 'video' in link['type']:
                item_map['video'] = link['href']

        feed_items.append(item_map)

    return feed_items


def insert_firestore():
    db = firestore.Client()
    col_ref = db.collection('articles')
    count = 0

    for url in feed_urls:

        entries = parse_feed_entries(url)
        # sorted_entries = sorted(
        #     entries, key=lambda entry: entry["pubdate"])
        # sorted_entries.reverse()

        for entry in entries:

            try:
                doc_ref = col_ref.document(entry['id'])
                # if doc_ref.get().exists:
                #     continue
                del entry['id']
                doc_ref.set(entry, merge=True)
                count = count + 1
                print('-> successfully written to db', entry['title'])
            except:
                print('Oops!', sys.exc_info(), 'occurred.', entry['title'], )
            # finally:
            #     doc_ref = col_ref.document()
            #     doc_ref.set(entry)

    print(count, 'articles added')


def scraper(request):
    insert_firestore()
