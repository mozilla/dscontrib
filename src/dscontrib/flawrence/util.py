# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import datetime


def add_days(date_string, n_days):
    """Add `n_days` days to a date string like '20190101'."""
    # TODO: would love to do some pre-computation here!
    original_date = datetime.datetime.strptime(date_string, '%Y%m%d')
    new_date = original_date + datetime.timedelta(days=n_days)
    return datetime.datetime.strftime(new_date, '%Y%m%d')
