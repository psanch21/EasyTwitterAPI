from selenium import webdriver  # Launch
from selenium.webdriver.common.by import By
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

import numpy as np
import pandas as pd


class Scrapping(object):
    def __init__(self, url_base):
        self.url_base = url_base
        self.option = webdriver.ChromeOptions()
        self.option.add_argument('incognito')
        # option.add_argument('headless')

    def scrappe(self):
        raise NotImplementedError

    def process(self):
        raise NotImplementedError

    #  brand_list, type_list, model_list, gender_list, hand_list, price_list, url_list, url_img_list
    def save_to_csv(self, table_list, column_list, file_name):
        table = list()

        for my_list in table_list:
            table.append(my_list)

        table = np.array(table).transpose()

        df = pd.DataFrame(table, columns=column_list)
        df.to_csv('csv/' + file_name + '.csv', sep=';')