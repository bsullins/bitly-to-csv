bitly-to-csv
======================
Pull click counts by country from bitly for a users "popular links" or a specified list of links and store them into a csv file. I then use this as the source for an incrementally updated Tableau extract.

Setup
======================
  1. download and install python (http://python.org/download/)
  2. install ez_setup.py (https://pypi.python.org/pypi/setuptools)
  3. rename config-local.py as config.py and edit values with your own
  4. edit pop_links array inside bitly_countries_csv.py
  5. run bitly_countries_csv.py
  6. (optional) setup nightly job to run bitly_countries_csv.py
