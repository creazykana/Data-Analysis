# -*- coding: utf-8 -*-
"""
Created on Tue Apr 23 10:31:12 2019

@author: hongzk
"""

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
import re
import pandas as pd
import numpy as np
import os
import sys
import time



def pivotable(df):
    