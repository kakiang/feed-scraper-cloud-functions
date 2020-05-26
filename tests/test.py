import unittest
import time
from src.main import datetime_of, check_feed_entry_date


class DateTest(unittest.TestCase):

    struct_time = time.gmtime()

    def test_that_datetime_of_return_None_if_struct_time_is_empty(self):
        self.assertIsNone(datetime_of(''))

    def test_that_datetime_of_raises_exception_if_struct_time_is_not_tuple(self):
        with self.assertRaises(TypeError):
            datetime_of('01-22-2020')

    def test_datetime_of_on_correct_date_format(self):
        self.assertIsNotNone(datetime_of(self.struct_time))

    def test_that_check_feed_entry_date_includes_items_less_than_1h(self):
        item = {'published_parsed': self.struct_time}
        self.assertIsNotNone(check_feed_entry_date(item))

    def test_that_check_feed_entry_date_exclude_items_older_than_1h(self):
        struct_time2 = time.gmtime((time.time() - 3601))
        entry = {'published_parsed': struct_time2}
        self.assertIsNone(check_feed_entry_date(entry))
