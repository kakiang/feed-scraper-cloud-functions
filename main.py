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


def datetime_of(struct_time):
    if struct_time:
        return datetime.fromtimestamp(mktime(struct_time))
    return None


def check_feed_entry_date(item):
    if item.get('updated_parsed', ''):
        struct_date = item.get('updated_parsed')
    elif item.get('published_parsed', ''):
        struct_date = item.get('published_parsed')
    else:
        return None

    pubdate = datetime_of(struct_date)
    elapsed_time = datetime.now() - pubdate
    # exclude items that are older than 1h
    if elapsed_time.days >= 1 or elapsed_time.seconds > 3600:
        return None

    return pubdate


def get_parsed_feed_entry(entry):

    pubdate = check_feed_entry_date(entry)

    if not pubdate:
        return None

    entry_map = {}

    if entry.get('title'):

        score, magnitude = analyze_sentiment(entry.get('title'))
        entry_map['sentiment_score'] = score
        entry_map['sentiment_magnitude'] = magnitude
        print(entry['title'])
        print(f"Sentiment: score of {score} with magnitude of {magnitude}")

    # entry_map['id'] = str(mktime(struct_date))
    entry_map['id'] = str(mktime(datetime.timetuple(pubdate)))
    entry_map['title'] = entry.get('title')
    entry_map['summary'] = clean(f'''{entry.get('summary')}''', strip=True)
    entry_map['link'] = entry.get('link')
    entry_map['pubdate'] = pubdate

    for link in entry['enclosures']:
        if 'image' in link['type']:
            entry_map['image'] = link['href']
        if 'video' in link['type']:
            entry_map['video'] = link['href']
    else:
        for link in entry.get('links'):
            if 'image' in link['type']:
                entry_map['image'] = link['href']
            if 'video' in link['type']:
                entry_map['video'] = link['href']

    return entry_map


def parse_feed_entries(parser_response):

    feed = parser_response['feed']

    channel = {}
    channel['title'] = feed.get('title')
    channel['link'] = feed.get('link')
    channel['language'] = feed.get('language')
    channel['updated_date'] = datetime_of(
        feed.get('updated_parsed', ''))

    feed_entries = []

    for entry in parser_response['entries']:

        entry_map = get_parsed_feed_entry(entry)
        if not entry_map:
            continue

        entry_map['feed'] = channel

        feed_entries.append(entry_map)

    return feed_entries


def parse_feed():

    all_feed_entries = []

    for feed_url in feed_urls:
        parser_response = feedparse(feed_url)
        feed_entries = parse_feed_entries(parser_response)
        if feed_entries:
            all_feed_entries.extend(feed_entries)

    return all_feed_entries


def save_feed_entries_firestore(all_entries):

    db = firestore.Client()
    col_ref = db.collection('articles')
    count = 0

    for entry in all_entries:

        try:
            doc_ref = col_ref.document(entry['id'])
            del entry['id']
            doc_ref.set(entry, merge=True)
            count += 1
        except Exception as e:
            print('Error', e, entry['title'], )
            raise

    print(count, 'articles added')


def parse_save_feed_entries_firestore(resquest):
    all_entries = parse_feed()
    save_feed_entries_firestore(all_entries)
