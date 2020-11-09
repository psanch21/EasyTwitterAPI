
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
# %%
url = r'https://twitter.com/genederose/lists/memberships'
driver = webdriver.Chrome(ChromeDriverManager().install())
driver.get(url)


# %%

content = driver.find_element_by_class_name('css-1dbjc4n r-1awozwy r-13awgt0 r-18u37iz r-1wbh5a2')


# %%


for element in  driver.find_elements_by_xpath("//a[@role='link']"):
    print()
