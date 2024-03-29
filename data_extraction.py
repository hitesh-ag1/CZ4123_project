# -*- coding: utf-8 -*-
"""BD Data Extraction.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/16nNdBiKHqAM8RKdnDrZsG9KHskSmoAev
"""

from __future__ import print_function
import json
import time
import datetime
import numpy as np

# Python 2 and 3: alternative 4
try:
    from urllib.request import urlopen
except ImportError:
    from urllib2 import urlopen

# Number of attempts to download data
MAX_ATTEMPTS = 6
# HTTPS here can be problematic for installs that don't have Lets Encrypt CA
SERVICE = "http://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?"

def download_data(uri):
    """Fetch the data from the IEM
    The IEM download service has some protections in place to keep the number
    of inbound requests in check.  This function implements an exponential
    backoff to keep individual downloads from erroring.
    Args:
      uri (string): URL to fetch
    Returns:
      string data
    """
    attempt = 0
    while attempt < MAX_ATTEMPTS:
        try:
            data = urlopen(uri, timeout=300).read().decode("utf-8")
            if data is not None and not data.startswith("ERROR"):
                return data
        except Exception as exp:
            print("download_data(%s) failed with %s" % (uri, exp))
            time.sleep(5)
        attempt += 1

    print("Exhausted attempts to download, returning empty data")
    return ""

networks = ['MY__ASOS', 'BR__ASOS', 'CA_BC_ASOS' ,'AK_ASOS', 'LY__ASOS', 'EG__ASOS', 'DE__ASOS', 'PL__ASOS', 'AU__ASOS', 'GL__ASOS', 'RU__ASOS', 'SG__ASOS', 'SA__ASOS', 'NF__ASOS', 'NP__ASOS', 'BT__ASOS', 'ES__ASOS', 'IT__ASOS', 'GR__ASOS', 'FI__ASOS', 'AQ__ASOS', 'NV_ASOS']

# uri = (
#         "https://mesonet.agron.iastate.edu/geojson/network/%s.geojson"
#     ) % ('AK_ASOS',)
#     # print(uri)
# data = urlopen(uri)
# jdict = json.load(data)
# for site in (jdict["features"]):
#   print(site['id'], site['properties']['sname'])

my_stations = []
for network in networks:
    # Get metadata
    uri = (
        "https://mesonet.agron.iastate.edu/geojson/network/%s.geojson"
    ) % (network,)
    # print(uri)
    data = urlopen(uri)
    jdict = json.load(data)
    if(network == 'AU__ASOS'):
      sites = ['YPJT', 'YMHB', 'YBBN']
    elif(network == 'ES__ASOS'):
      sites = ['LEBL', 'LEGT']
    elif(network == 'IT__ASOS'):
      sites = ['LIRF']
    elif(network=='BR__ASOS'):
      sites = ['SBJR']
    elif(network=='AK_ASOS'):
      sites = ['PANC']
    elif(network=='SG__ASOS'):
      sites = ['WSSS']
    elif(network=='GL__ASOS'):
      sites=['BGGH']
    elif(network=='DE__ASOS'):
      sites=['EDAC']
    elif(network=='NV_ASOS'):
      sites=['BVU']
    elif(network=='MY__ASOS'):
      sites=['WMSA']
    elif(network=='AQ__ASOS'):
      sites=['NZWD']
    else:
      sites = None
    if(sites == None):
      print(jdict["features"][0]['id'], jdict["features"][0]['properties']['sname'], network)
      my_stations.append(jdict["features"][0]['id'])
    else:
      for site in jdict["features"]:
        if(site['id'] in sites):
          print(site['id'] ,site['properties']['sname'], network)
          my_stations.append(site['id'])
          sites.remove(site['id'])

len(my_stations)

import re
import os
def stripComments(code):
    code = str(code)
    return re.sub(r'(?m)^ *#.*\n?', '', code)

def main(start_date, end_date, station, multiple_stations = False):
    data1 = 'tmpc'
    data2 = 'relh'
    service = SERVICE + "data=%s&data=%s&tz=Etc/UTC&format=comma&latlon=no&" % (data1, data2)

    service += start_date.strftime("year1=%Y&month1=%m&day1=%d&")
    service += end_date.strftime("year2=%Y&month2=%m&day2=%d&")
    if(multiple_stations == False):
      uri = "%s&station=%s" % (service, station)

      print("Downloading: %s" % (station,))
      data = download_data(uri)
      outfn = "%s_%s_%s.txt" % (
          station,
          start_date.strftime("%Y%m%d%H%M"),
          end_date.strftime("%Y%m%d%H%M"),
      )
      data = stripComments(data)
      out = open(outfn, "w")
      out.write(data)
      out.close()
    else:
      for st in station:
        uri = "%s&station=%s" % (service, st)
        print("Downloading: %s" % (st))
        data = download_data(uri)
        outfn = "%s_%s.txt" % (
            start_date.strftime("%Y%m%d%H%M"),
            end_date.strftime("%Y%m%d%H%M"),
        )
        data = stripComments(data)
        if(outfn in os.listdir()):
          data = '\n'.join((data.split('\n')[1:]))
          out = open(outfn, "a")
          out.write(data)
          out.close()
        else:
          out = open(outfn, "w")
          out.write(data)
          out.close()

date1 = datetime.datetime(2017, 1, 1)
date2 = datetime.datetime(2022, 2, 1)
main(date1, date2, my_stations, multiple_stations=True)